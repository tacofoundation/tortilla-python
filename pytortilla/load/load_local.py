import pathlib
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def file2metadata(file: Union[str, pathlib.Path]):
    """Read the metadata of tortilla file given a local path.

    Args:
        files (Union[str, pathlib.Path]): A local path pointing to the
            tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """
    with open(file, "rb") as f:
        static_bytes = f.read(50)

        # SPLIT the static bytes
        MB: bytes = static_bytes[:2]
        FO: bytes = static_bytes[2:10]
        FL: bytes = static_bytes[10:18]
        DF: str = static_bytes[18:42].strip().decode()
        # DP: str = static_bytes[42:50]

        if MB != b"#y":
            raise ValueError("You are not a tortilla 🫓 or a TACO 🌮")

        # Read the NEXT 8 bytes of the file
        footer_offset: int = int.from_bytes(FO, "little")

        # Seek to the FOOTER offset
        f.seek(footer_offset)

        # Select the FOOTER length
        # Read the FOOTER
        footer_length: int = int.from_bytes(FL, "little")
        metadata = pq.read_table(pa.BufferReader(f.read(footer_length))).to_pandas()

        # Convert dataset to DataFrame
        metadata["internal:file_format"] = DF
        metadata["internal:mode"] = "local"
        metadata["internal:subfile"] = metadata.apply(
            lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:length']},{file}",
            axis=1,
        )

    return metadata


def files2metadata(files: Union[List[str], List[pathlib.Path]]):
    """Read the metadata of tortilla files given local paths.

    Args:
        files (Union[List[str], List[pathlib.Path]]): A list of local
            paths pointing to the tortilla files.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """

    # Merge the metadata of the files
    container = []
    for file in files:
        with open(file, "rb") as f:
            static_bytes = f.read(50)

            # SPLIT the static bytes
            MB: bytes = static_bytes[:2]
            FO: bytes = static_bytes[2:10]
            FL: bytes = static_bytes[10:18]
            DF: str = static_bytes[18:42].strip().decode()
            # DP: str = static_bytes[42:50]

            if MB != b"#y":
                raise ValueError("You are not a tortilla 🫓 or a TACO 🌮")

            # Read the NEXT 8 bytes of the file
            footer_offset: int = int.from_bytes(FO, "little")

            # Seek to the FOOTER offset
            f.seek(footer_offset)

            # Select the FOOTER length
            # Read the FOOTER
            footer_length: int = int.from_bytes(FL, "little")
            metadata = pq.read_table(pa.BufferReader(f.read(footer_length))).to_pandas()

            # Convert dataset to DataFrame
            metadata["internal:file_format"] = DF
            metadata["internal:mode"] = "local"
            metadata["internal:subfile"] = metadata.apply(
                lambda row: f"/vsisubfile/{row['tortilla:offset']}_{row['tortilla:length']},{file}",
                axis=1,
            )
            container.append(metadata)

    return pd.concat(container, ignore_index=True)
