import concurrent.futures
import json
import mmap
import pathlib
from typing import List, Literal, Union

import pandas as pd
import requests
import tqdm

import pytortilla.utils

GDAL_FILES = Literal[
    "VRT",
    "DERIVED",
    "GTiff",
    "COG",
    "NITF",
    "RPFTOC",
    "ECRGTOC",
    "HFA",
    "SAR_CEOS",
    "CEOS",
    "JAXAPALSAR",
    "GFF",
    "ELAS",
    "ESRIC",
    "AIG",
    "AAIGrid",
    "GRASSASCIIGrid",
    "ISG",
    "SDTS",
    "DTED",
    "PNG",
    "JPEG",
    "MEM",
    "JDEM",
    "GIF",
    "BIGGIF",
    "ESAT",
    "FITS",
    "BSB",
    "XPM",
    "BMP",
    "DIMAP",
    "AirSAR",
    "RS2",
    "SAFE",
    "PCIDSK",
    "PCRaster",
    "ILWIS",
    "SGI",
    "SRTMHGT",
    "Leveller",
    "Terragen",
    "netCDF",
    "HDF4",
    "HDF4Image",
    "ISIS3",
    "ISIS2",
    "PDS",
    "PDS4",
    "VICAR",
    "TIL",
    "ERS",
    "JP2OpenJPEG",
    "L1B",
    "FIT",
    "GRIB",
    "RMF",
    "WCS",
    "WMS",
    "MSGN",
    "RST",
    "GSAG",
    "GSBG",
    "GS7BG",
    "COSAR",
    "TSX",
    "COASP",
    "R",
    "MAP",
    "KMLSUPEROVERLAY",
    "WEBP",
    "PDF",
    "Rasterlite",
    "MBTiles",
    "PLMOSAIC",
    "CALS",
    "WMTS",
    "SENTINEL2",
    "MRF",
    "PNM",
    "DOQ1",
    "DOQ2",
    "PAux",
    "MFF",
    "MFF2",
    "GSC",
    "FAST",
    "BT",
    "LAN",
    "CPG",
    "NDF",
    "EIR",
    "DIPEx",
    "LCP",
    "GTX",
    "LOSLAS",
    "NTv2",
    "CTable2",
    "ACE2",
    "SNODAS",
    "KRO",
    "ROI_PAC",
    "RRASTER",
    "BYN",
    "NOAA_B",
    "NSIDCbin",
    "ARG",
    "RIK",
    "USGSDEM",
    "GXF",
    "BAG",
    "S102",
    "HDF5",
    "HDF5Image",
    "NWT_GRD",
    "NWT_GRC",
    "ADRG",
    "SRP",
    "BLX",
    "PostGISRaster",
    "SAGA",
    "XYZ",
    "HF2",
    "OZI",
    "CTG",
    "ZMap",
    "NGSGEOID",
    "IRIS",
    "PRF",
    "EEDAI",
    "DAAS",
    "SIGDEM",
    "HEIF",
    "TGA",
    "OGCAPI",
    "STACTA",
    "STACIT",
    "GPKG",
    "OpenFileGDB",
    "CAD",
    "PLSCENES",
    "NGW",
    "GenBin",
    "ENVI",
    "EHdr",
    "ISCE",
    "Zarr",
    "HTTP",
]


def create_tortilla(
    files: Union[List[str], List[pathlib.Path]],
    output: Union[str, pathlib.Path],
    file_format: GDAL_FILES,
    nworkers: int = 4,
    chunk_size: int = 1024 * 1024 * 100,
    quiet: bool = False,
) -> pathlib.Path:
    """Create a tortilla 🫓

    A tortilla is a new simple format for storing same format files
    optimized for blazing fast random access.

    Args:
        files (List[str]): The list of files to be included in the
            tortilla. All files must have the same format (same extension).
        file_format (GDAL_FILES): The format of the files. This format
            must be one of the GDAL formats. For example, "GTiff", "COG",
            "PNG", etc.
        nworkers (int, optional): The number of workers to use when writing
            the tortilla. Defaults to 4.
        chunk_size (int, optional): The size of the chunks to use when writing
            the tortilla. Defaults to 1024 * 1024 * 100.

    Returns:
        pathlib.Path: The tortilla file.
    """

    if file_format not in GDAL_FILES.__args__:
        raise ValueError(
            f"Invalid file format: {file_format}. Must be one of {GDAL_FILES.__args__}"
        )

    # Define the magic number
    MB: bytes = b"#y"

    # Get the total size of the files
    dict_bytes, bytes_counter = pytortilla.utils.process_files_concurrently(
        files=files, nworkers=nworkers
    )

    # Obtain the FOOTER metadata
    FOOTER: str = json.dumps(dict_bytes).encode()
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = (50 + bytes_counter).to_bytes(8, "little")

    # Data Format of items
    DF: int = file_format.encode().ljust(32)

    # Get the total size of the files
    total_size: int = 50 + bytes_counter + len(FOOTER)

    # If the folder does not exist, create it
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Create an empty file
    with open(output, "wb") as f:
        f.truncate(total_size)

    # Define the function to write into the main file
    def write_file(file, start):
        """read the file in chunks"""
        with open(file, "rb") as g:
            while True:
                chunk = g.read(chunk_size)
                if not chunk:
                    break
                mm[start : (start + len(chunk))] = chunk
                start += len(chunk)

    # Cook the tortilla 🫓
    with open(output, "r+b") as f:
        with mmap.mmap(f.fileno(), 0) as mm:
            # Write the magic number
            mm[:2] = MB

            # Write the FOOTER offset
            mm[2:10] = FO

            # Write the FOOTER length
            mm[10:18] = FL

            # Write the decoder
            mm[18:50] = DF

            # Write the DATA
            message = pytortilla.utils.tortilla_message()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=nworkers
            ) as executor:
                futures = []
                for file in files:
                    start, _ = dict_bytes[pathlib.Path(file).stem]
                    futures.append(executor.submit(write_file, file, start))

                # Wait for all futures to complete
                if not quiet:
                    list(
                        tqdm.tqdm(
                            concurrent.futures.as_completed(futures),
                            total=len(futures),
                            desc=message,
                            unit="file",
                        )
                    )
                else:
                    concurrent.futures.wait(futures)

            # Write the FOOTER
            mm[(50 + bytes_counter) :] = FOOTER

    return output


def read_tortilla_metadata_local(file: Union[str, pathlib.Path]) -> pd.DataFrame:
    """Read the metadata of a tortilla file.

    Args:
        file (Union[str, pathlib.Path]): A tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """
    with open(file, "rb") as f:
        # Read the FIRST 2 bytes of the file
        static_bytes = f.read(50)

        # SPLIT the static bytes
        MB: bytes = static_bytes[:2]
        FO: bytes = static_bytes[2:10]
        FL: bytes = static_bytes[10:18]
        DF: str = static_bytes[18:50].strip().decode()

        if MB != b"#y":
            raise ValueError("You are not a tortilla 🫓")

        # Read the NEXT 8 bytes of the file
        footer_offset: int = int.from_bytes(FO, "little")

        # Seek to the FOOTER offset
        f.seek(footer_offset)

        # Read the bytes 100-200
        footer_data: dict = json.loads(f.read(int.from_bytes(FL, "little")))

        # Convert dataset to DataFrame
        datapoints: List[tuple] = list(footer_data.items())
        metadata = pd.DataFrame(datapoints, columns=["id", "values"])
        metadata[["tortilla:offset", "tortilla:size"]] = pd.DataFrame(
            metadata["values"].tolist(), index=metadata.index
        )
        metadata = metadata.drop(columns="values")
        metadata["tortilla:file_format"] = DF
        metadata["tortilla:mode"] = "local"

    return metadata


def read_tortilla_metadata_online(file: str) -> pd.DataFrame:
    """Read the metadata of a tortilla file given a URL. The
        server must support the HTTPS Range Request.

    Args:
        file (str): The URL of the tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """

    # Fetch the first 8 bytes of the file
    headers = {"Range": "bytes=0-50"}
    response: requests.Response = requests.get(file, headers=headers)
    static_bytes: bytes = response.content

    # SPLIT the static bytes
    MB: bytes = static_bytes[:2]
    FO: bytes = static_bytes[2:10]
    FL: bytes = static_bytes[10:18]
    DF: str = static_bytes[18:50].strip().decode()

    # Check if the file is a tortilla
    if MB != b"#y":
        raise ValueError("You are not a tortilla 🫓")

    # Interpret the bytes as a little-endian integer
    footer_offset: int = int.from_bytes(FO, "little")
    footer_length: int = int.from_bytes(FL, "little")

    # Fetch the footer
    headers = {"Range": f"bytes={footer_offset}-{footer_offset + footer_length}"}
    with requests.get(file, headers=headers) as response:

        # Interpret the response as a JSON object
        footer_data = json.loads(response.content)

        # Convert dataset to DataFrame
        datapoints: List[tuple] = list(footer_data.items())
        metadata = pd.DataFrame(datapoints, columns=["id", "values"])
        metadata[["tortilla:offset", "tortilla:size"]] = pd.DataFrame(
            metadata["values"].tolist(), index=metadata.index
        )
        metadata = metadata.drop(columns="values")
        metadata["tortilla:file_format"] = DF
        metadata["tortilla:mode"] = "online"

    return metadata


def load_metadata(file: Union[str, pathlib.Path]) -> pd.DataFrame:
    """Read the metadata of a tortilla file.

    Args:
        file (Union[str, pathlib.Path]): A tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """
    if isinstance(file, str):
        if pytortilla.utils.is_valid_url(file):
            metadata: pd.DataFrame = read_tortilla_metadata_online(file)
            partial_file: pd.Series = metadata.apply(
                lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:size']},/vsicurl/{file}",
                axis=1,
            )
        else:
            metadata: pd.DataFrame = read_tortilla_metadata_local(file)
            partial_file: pd.Series = metadata.apply(
                lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:size']},{file}",
                axis=1,
            )

    elif isinstance(file, pathlib.Path):
        metadata: pd.DataFrame = read_tortilla_metadata_local(file)
        partial_file: pd.Series = metadata.apply(
            lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:size']},{file}",
            axis=1,
        )
    else:
        raise ValueError("Invalid file type. Must be a string or pathlib.Path.")

    metadata["tortilla:file"] = partial_file

    return metadata


def load_data(dataset: Union[pd.DataFrame, pd.Series]) -> List[str]:
    """Get direct access to the data stored in the tortilla file.

    Args:
        dataset (Union[pd.DataFrame, pd.Series]): A pandas DataFrame or Series
            that contains the metadata of a specific item in the tortilla file.

    Returns:
        List[str]: A list of strings which contains the direct access to the data.
            Using rasterio, you can read the data by simply passing the string to
            the `rio.open` function. Similar behavior can be expected with other
            libraries, such as `rioxarray` or `osgeo.gdal`.
    """
    if isinstance(dataset, pd.Series):
        return [dataset["tortilla:file"]]
    return dataset["tortilla:file"].tolist()