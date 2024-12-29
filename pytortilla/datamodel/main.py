import re
import datetime
import pathlib
from typing import Optional, Union, Literal, Callable, List, Dict
from tqdm import tqdm

import pandas as pd
import numpy as np
import pydantic
import concurrent.futures

from pytortilla.datamodel import utils
from pytortilla.datamodel.utils import fetch_gee_metadata, load_admin_codes, map_admin_codes, GEEPRODUCTS

# Define a macro type for numbers
Number = Union[int, float]


class STAC(pydantic.BaseModel):
    """SpatioTemporal Asset Catalog (STAC) metadata."""

    crs: str
    raster_shape: tuple[int, int]
    geotransform: tuple[float, float, float, float, float, float]    
    time_start: Union[datetime.datetime, int, float]
    centroid: Optional[str] = None
    time_end: Optional[Union[datetime.datetime, int, float]] = None

    @pydantic.model_validator(mode="after")
    def check_times(cls, values):
        """Validates that the time_start is before time_end."""
        # If time_start is a datetime object, convert it to a timestamp
        if isinstance(values.time_start, datetime.datetime):
            values.time_start = values.time_start.timestamp()

        # If time_end is a datetime object, convert it to a timestamp
        if values.time_end is not None:
            if isinstance(values.time_end, datetime.datetime):
                values.time_end = values.time_end.timestamp()

            if values.time_start > values.time_end:
                raise ValueError(
                    f"Invalid times: {values.time_start} > {values.time_end}"
                )

        return values


class RAI(pydantic.BaseModel):
    """Metadata for Responsible AI (RAI) objectives."""

    ele: Number
    cisi: Number
    gdp: Number
    hdi: Number
    gmi: Number
    pop: Number
    admin0: str
    admin1: str
    admin2: str


class Sample(pydantic.BaseModel):
    """A sample with STAC and RAI metadata."""

    id: str
    file_format: utils.GDAL_FILES
    path: pathlib.Path
    data_split: Optional[Literal["train", "validation", "test"]] = None 
    stac_data: Optional[STAC] = None
    rai_data: Optional[RAI] = None

    class Config:
        extra = "allow"

    def export_metadata(self):
        """
        Exports metadata as a dictionary, including STAC and RAI attributes, and any extra fields.
        """
        # If crs, raster_shape and geotransform are not provided, then create the stac:centroid
        if self.stac_data is not None:
            if self.stac_data.centroid is None:
                if (
                    self.stac_data.crs is not None
                    and self.stac_data.geotransform is not None
                    and self.stac_data.raster_shape is not None
                ):
                    self.stac_data.centroid = utils.raster_centroid(
                        crs=self.stac_data.crs,
                        geotransform=self.stac_data.geotransform,
                        raster_shape=self.stac_data.raster_shape,
                    )

        # Gather core metadata
        metadata = {
            "internal:path": str(self.path.resolve()),
            "tortilla:id": self.id,
            "tortilla:file_format": self.file_format,
            "tortilla:data_split": self.data_split,
            "tortilla:offset": 0,
            "tortilla:length": self.path.stat().st_size,
        }

        # Add STAC metadata if available
        if self.stac_data:
            metadata.update({
                "stac:crs": self.stac_data.crs,
                "stac:geotransform": self.stac_data.geotransform,
                "stac:raster_shape": self.stac_data.raster_shape,
                "stac:time_start": self.stac_data.time_start,
                "stac:time_end": self.stac_data.time_end,
                "stac:centroid": self.stac_data.centroid,
            })

        # Add RAI metadata if available
        if self.rai_data:
            metadata.update({
                "rai:ele": self.rai_data.ele,
                "rai:cisi": self.rai_data.cisi,
                "rai:gdp": self.rai_data.gdp,
                "rai:hdi": self.rai_data.hdi,
                "rai:gmi": self.rai_data.gmi,
                "rai:pop": self.rai_data.pop,
                "rai:admin0": self.rai_data.admin0,
                "rai:admin1": self.rai_data.admin1,
                "rai:admin2": self.rai_data.admin2,                                
            })

        # Merge with additional metadata and remove None values
        metadata.update(
            {k: v for k, v in self.model_dump(
                exclude={"id", "path", "stac_data", "rai_data", "file_format", "data_split"},
                by_alias=True,
            ).items() if v is not None}
        )

        return metadata


class Samples(pydantic.BaseModel):
    samples: list[Sample]

    @pydantic.model_validator(mode="after")
    def check_samples(cls, values):
        """
        Validates that the samples have unique IDs and path exists.
        """        
        # Check if the ids are unique
        ids = [sample.id for sample in values.samples]
        if len(ids) != len(set(ids)):
            raise ValueError("The samples must have unique IDs.")
        
        # Check if the paths exist
        for sample in values.samples:
            if not sample.path.exists():
                raise FileNotFoundError(f"Path does not exist: {sample.path}")
                        
        return values

    @staticmethod
    def process_chunk(chunk):
        return [sample.export_metadata() for sample in chunk]
    
    def export_metadata(self, nworkers: int=4, chunk_size: int=1000) -> pd.DataFrame:
        """
        Export metadata from samples in parallel.

        Args:
            samples (list): List of sample objects with `export_metadata` method.
            max_workers (int): Number of parallel workers.

        Returns:
            pd.DataFrame: DataFrame containing metadata.
        """

        chunks = np.array_split(self.samples, max(1, len(self.samples) // chunk_size))
        with concurrent.futures.ProcessPoolExecutor(max_workers=nworkers) as executor:
            results = executor.map(Samples.process_chunk, chunks)

        # Flatten results
        return pd.DataFrame([item for sublist in results for item in sublist])
    
    def deep_validator(self, read_function: Callable, max_workers: int = 4) -> List[str]:
        """
        Return a list of files that failed when trying to read them
        
        Args:
            read_function (Callable): Function to read a file.
            max_workers (int): Number of parallel threads.
        
        Returns:
            List[str]: List of file paths that failed to read.
        """
        # Get the paths of the samples
        internal_paths = [sample.path for sample in self.samples]

        # Use ThreadPoolExecutor to parallelize the validation
        failed_files = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Use tqdm to display progress
            for result in tqdm(
                executor.map(validate_file, internal_paths, [read_function] * len(internal_paths)),
                total=len(internal_paths),
                desc="Validating files",
            ):
                if result is not None:
                    failed_files.append(result)

        return failed_files


    def include_rai_metadata(
        self,
        sample_footprint: Optional[int] = 5120,
        cache: Optional[bool] = True,
        quiet: Optional[bool] = False
    ) -> pd.DataFrame:
        """
        Include RAI metadata in the samples.

        Args:
            sample_footprint (int): The size of the sample footprint in meters.
            cache (bool): Whether to cache the metadata.
            quiet (bool): Whether to print progress messages.

        Returns:
            pd.DataFrame: DataFrame containing the RAI metadata.
        """
        self.samples = get_rai_metadata(
            samples=self,
            sample_footprint=sample_footprint,
            cache=cache,
            quiet=quiet
        )

        return self
                         

def get_rai_metadata(
    samples: Samples,
    sample_footprint: Optional[int] = 5120,
    cache: Optional[bool] = True,
    quiet: Optional[bool] = False
) -> List[Sample]:
    """ Get RAI metadata given a Samples object.

    Args:
        samples (pytortilla.datamodel.Samples): The samples to get the 
            metadata from.
        sample_footprint (int, optional): The size of the sample footprint.
            Defaults to 5120.
        quiet (bool, optional): Whether to print progress messages. Defaults
            the progress messages are printed.

    Raises:
        ImportError: This module depends on the 'ee' module. Please install 
            it first.

    Returns:
        List[Sample]: A list of samples with the RAI metadata added.
    """

    try: 
        import ee
    except ImportError:
        raise ImportError("This module depends on the 'ee' module. Please install it first.")


    if not quiet:
        print("Preparing RAI metadata process...")
    
    # 0. Set up cache path
    if cache:
        cache_path = pathlib.Path("cache")
        cache_path.mkdir(exist_ok=True, parents=True)

    # 1. Export metadata and chunk the database. Data is fetched in 
    # chunks of 100 samples.
    db = samples.export_metadata()
    db_chunks = [db.iloc[i:i+100] for i in range(0, len(db), 100)]
    point_pattern = re.compile(r"POINT\s*\(\s*(-?\d+\.\d+)\s+(-?\d+\.\d+)\s*\)")

    # 2. Load Earth Engine Image Collection dataset
    imagecollection = {
        'mean' : {
            "ele": ee.ImageCollection(GEEPRODUCTS["ele"]).mosaic().unmask(0).rename("ele"),
            "cisi": ee.Image(GEEPRODUCTS["cisi"]).unmask(0).rename("cisi"),
            "gdp": ee.Image(GEEPRODUCTS["gdp"]).unmask(0).select("b26").rename("gdp"),
            "hdi": ee.Image(GEEPRODUCTS["hdi"]).unmask(0).select("b26").rename("hdi"),
            "gmi": ee.Image(GEEPRODUCTS["gmi"]).unmask(0).rename("gmi"),
            "pop": ee.ImageCollection(GEEPRODUCTS["pop"]).mosaic().unmask(0).rename("pop"),
        },
        "mode": {
            "admin0": ee.Image(GEEPRODUCTS["admin0"]).unmask(65535).rename("admin_code0"),
            "admin1": ee.Image(GEEPRODUCTS["admin1"]).unmask(65535).rename("admin_code1"),
            "admin2": ee.Image(GEEPRODUCTS["admin2"]).unmask(65535).rename("admin_code2")
        }
    }
    image_mean = ee.Image(list(imagecollection["mean"].values()))
    image_mode = ee.Image(list(imagecollection["mode"].values()))

    # 3. Load admin codes datasets
    admin_codes: Dict[str, pd.DataFrame] = load_admin_codes()
    
    # 4. Iterate over the chunks and get the metadata
    rai_metadata_container = []
    for chunk in tqdm(db_chunks, disable=quiet, desc="Fetching RAI metadata"):
        
        ## 4.1. Get the chunk id
        chunk_id = chunk[["tortilla:id"]]
        chunk_id.reset_index(drop=True, inplace=True)

        ## 4.2. If the metadata is cached, load it
        if cache:
            if (chunk_id.iloc[0, 0] + ".csv") in cache_path.iterdir():
                rai_metadata = pd.read_csv(cache_path / f"{chunk_id.iloc[0, 0]}.csv")
                rai_metadata_container.append(rai_metadata)
                continue

        ## 4.3. Extract points
        coords = chunk["stac:centroid"].apply(lambda x: point_pattern.match(x).groups())
        points = ee.FeatureCollection([ee.Geometry.Point(float(x), float(y)) for x, y in coords])

        ## 4.4. Fetch metadata - returns a DataFrame with the metadata        
        rai1_df: pd.DataFrame = fetch_gee_metadata(image_mean, points, ee.Reducer.mean(), sample_footprint)
        rai2_df: pd.DataFrame = fetch_gee_metadata(image_mode, points, ee.Reducer.mode(), 556.5974539663679)

        ## 4.5. Map admin codes to descriptive names
        admin_metadata: pd.DataFrame = map_admin_codes(admin_codes, rai2_df)
        admin_metadata.reset_index(drop=True, inplace=True)

        ## 4.6. Get the id
        rai_metadata = pd.concat([chunk_id, rai1_df, admin_metadata], axis=1)

        ## 4.7. Cache metadata
        if cache:            
            cache_filename = cache_path / f"{chunk_id.iloc[0, 0]}.csv"
            rai_metadata.to_csv(cache_filename, index=False)

        ## 4.8. Append to the container
        rai_metadata_container.append(rai_metadata)
   
    # 5. Concatenate and clean up metadata
    rai_metadata = pd.concat(rai_metadata_container).set_index("tortilla:id")
    for col in ["admin0", "admin1", "admin2"]:
        rai_metadata[col] = rai_metadata[col].fillna("missing").astype(str)
    
    ## 6. Clean cache folder
    if cache:
        for file in cache_path.iterdir():
            if file.name not in rai_metadata.index:
                file.unlink()

    # 7. Create new samples with metadata
    enriched_samples = [
        Sample(
            **{
                **sample.model_dump(),
                **{"rai_data": rai_metadata.loc[sample.id].to_dict()}
            }
        )
        for sample in samples.samples
    ]

    return enriched_samples


def validate_file(file_path, read_function):
    """Validate if the file can be read by the read_function."""
    try:
        read_function(file_path)
        return None  # Return None if successful
    except Exception:
        return file_path  # Return file path if failed
