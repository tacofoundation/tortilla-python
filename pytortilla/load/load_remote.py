import pathlib
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


def file2metadata(file: str) -> pd.DataFrame:
    """Read the metadata of a tortilla file given a URL. The
        server must support HTTP Range requests.

    Args:
        files (str): A URL pointing to the tortilla file.
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
    DF: str = static_bytes[18:42].strip().decode()

    # Check if the file is a tortilla
    if MB != b"#y":
        raise ValueError("You are not a tortilla 🫓")

    # Interpret the bytes as a little-endian integer
    footer_offset: int = int.from_bytes(FO, "little")
    footer_length: int = int.from_bytes(FL, "little")

    # Fetch the footer
    headers = {"Range": f"bytes={footer_offset}-{footer_offset + footer_length}"}
    with requests.get(file, headers=headers) as response:

        # Interpret the response as a parquet table
        metadata = pq.read_table(pa.BufferReader(response.content)).to_pandas()

    # Add the file format and mode
    metadata["tortilla:file_format"] = DF
    metadata["tortilla:mode"] = "online"
    metadata["tortilla:subfile"] = metadata.apply(
        lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:length']},/vsicurl/{file}",
        axis=1,
    )
    return metadata


def files2metadata(files: List[str]) -> pd.DataFrame:
    """Read the metadata of tortillas files given a set of URLs. The
        server must support HTTP Range requests.

    Args:
        files (List[str]): A list of URLs pointing to the
            tortilla files.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """

    container = []
    for file in files:

        # Fetch the first 8 bytes of the file
        headers = {"Range": "bytes=0-50"}
        response: requests.Response = requests.get(file, headers=headers)
        static_bytes: bytes = response.content

        # SPLIT the static bytes
        MB: bytes = static_bytes[:2]
        FO: bytes = static_bytes[2:10]
        FL: bytes = static_bytes[10:18]
        DF: str = static_bytes[18:42].strip().decode()

        # Check if the file is a tortilla
        if MB != b"#y":
            raise ValueError("You are not a tortilla 🫓")

        # Interpret the bytes as a little-endian integer
        footer_offset: int = int.from_bytes(FO, "little")
        footer_length: int = int.from_bytes(FL, "little")

        # Fetch the footer
        headers = {"Range": f"bytes={footer_offset}-{footer_offset + footer_length}"}
        with requests.get(file, headers=headers) as response:

            # Interpret the response as a parquet table
            metadata = pq.read_table(pa.BufferReader(response.content)).to_pandas()

        # Add the file format and mode
        metadata["tortilla:file_format"] = DF
        metadata["tortilla:mode"] = "online"
        metadata["tortilla:subfile"] = metadata.apply(
            lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:length']},/vsicurl/{file}",
            axis=1,
        )
        container.append(metadata)

    return pd.concat(container, ignore_index=True)
