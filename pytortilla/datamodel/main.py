import datetime
import pathlib
from typing import Optional, Union

import pandas as pd
import pydantic

from .utils import GDAL_FILES, raster_centroid


class STAC(pydantic.BaseModel):
    """SpatioTemporal Asset Catalog (STAC) metadata."""

    crs: str
    raster_shape: tuple[int, int]
    geotransform: tuple[float, float, float, float, float, float]
    centroid: Optional[str] = None
    time_start: datetime.datetime
    time_end: Optional[datetime.datetime] = None

    @pydantic.model_validator(mode="after")
    def check_times(cls, values):
        """Validates that the time_start is before time_end."""
        if values.time_start > values.time_end:
            raise ValueError(f"Invalid times: {values.time_start} > {values.time_end}")

        return values


class RAI(pydantic.BaseModel):
    """Metadata for Responsible AI (RAI) objectives."""

    populationdensity: Optional[Union[int, float]] = None
    female: Optional[Union[int, float]] = None
    womenreproducibleage: Optional[Union[int, float]] = None
    children: Optional[Union[int, float]] = None
    youth: Optional[Union[int, float]] = None
    elderly: Optional[Union[int, float]] = None


class Sample(pydantic.BaseModel):
    """A sample with STAC and RAI metadata."""

    id: str
    path: pathlib.Path
    stac_data: Optional[STAC] = None
    rai_data: Optional[RAI] = None

    class Config:
        extra = "allow"

    @pydantic.field_validator("path")
    def check_path(cls, value):
        """
        Validates that the provided path exists.
        """
        if not value.exists():
            raise ValueError(f"{value} does not exist.")
        return value

    def export_metadata(self):
        """
        Exports metadata as a dictionary, including STAC and RAI attributes, and any extra fields.
        """
        # Gather additional metadata (extra fields not defined explicitly)
        extra_metadata = self.model_dump(
            exclude={"id", "path", "stac_data", "rai_data"}, by_alias=True
        )

        # If crs, raster_shape and geotransform are not provided, then create the stac:centroid
        if self.stac_data is not None:
            if self.stac_data.centroid is None:
                if (
                    self.stac_data.crs is not None
                    and self.stac_data.geotransform is not None
                    and self.stac_data.raster_shape is not None
                ):
                    self.stac_data.centroid = raster_centroid(
                        crs=self.stac_data.crs,
                        geotransform=self.stac_data.geotransform,
                        raster_shape=self.stac_data.raster_shape
                    )

        # Merge all metadata into a single dictionary
        metadata = {
            "internal:path": self.path.resolve().as_posix(),
            "tortilla:id": self.id,
            "tortilla:offset": 0,
            "tortilla:length": self.path.stat().st_size,
            "stac:crs": self.stac_data.crs,
            "stac:geotransform": self.stac_data.geotransform,
            "stac:raster_shape": self.stac_data.raster_shape,
            "stac:time_start": self.stac_data.time_start,
            "stac:time_end": self.stac_data.time_end,
            "stac:centroid": self.stac_data.centroid,
            "rai:populationdensity": self.rai_data.populationdensity,
            "rai:female": self.rai_data.female,
            "rai:womenreproducibleage": self.rai_data.womenreproducibleage,
            "rai:children": self.rai_data.children,
            "rai:youth": self.rai_data.youth,
            "rai:elderly": self.rai_data.elderly,
        }

        # Remove None values
        metadata = {k: v for k, v in metadata.items() if v is not None}

        return {**metadata, **extra_metadata}


class Samples(pydantic.BaseModel):
    samples: list[Sample]
    file_format: GDAL_FILES

    @pydantic.model_validator(mode="after")
    def check_samples(cls, values):
        """
        Validates that the samples have unique IDs.
        """
        ids = [sample.id for sample in values.samples]
        if len(ids) != len(set(ids)):
            raise ValueError("The samples must have unique IDs.")
        return values

    def export_metadata(self):
        """
        Exports metadata for all samples in the collection.
        """
        return pd.DataFrame([sample.export_metadata() for sample in self.samples])
