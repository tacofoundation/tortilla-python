import concurrent.futures
import mmap
import os
import pathlib
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

from ..datamodel.main import Samples
from .utils import group_dataframe_by_size, human2bytes, tortilla_message


def create(
    samples: Samples,
    output: Union[str, pathlib.Path],
    nworkers: int = min(4, os.cpu_count()),
    chunk_size: str = "20GB",
    chunk_size_iter: str = "100MB",
    quiet: bool = False,
) -> Union[pathlib.Path, List[pathlib.Path]]:
    """Create a tortilla 🫓

    A tortilla is a new simple format for storing same format files
    optimized for very fast random access.

    Args:
        samples (Samples): The list of samples to be included in
            the tortilla file. All samples must have the same format
            (same extension). The Sample objects must have a unique
            `id` field.
        output (Union[str, pathlib.Path]): The path where the tortilla
            file will be saved.
        nworkers (int, optional): The number of workers to use when writing
            the tortilla. Defaults to 4.
        chunk_size (str, optional): Avoid large tortilla files by splitting
            the data into chunks. By default, if the number of samples exceeds
            20GB, the data will be split into chunks of 20GB.
        chunk_size_iter (int, optional): The writting chunk size. By default,
            it is 100MB. Faster computers can use a larger chunk size.
        quiet (bool, optional): If True, the function does not print any
            message. By default, it is False.

    Returns:
        Union[pathlib.Path, List[pathlib.Path]]: The path of the tortilla file.
            If the tortilla file is split into multiple parts, a list of paths
            is returned.
    """

    # Create the output folder if it does not exist
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # From samples to metadata
    metadata: pd.DataFrame = samples.export_metadata()

    # From human-readable to bytes
    chunk_size_iter_bytes: int = human2bytes(chunk_size_iter)
    chunk_size_bytes: int = human2bytes(chunk_size)

    # Aggregate metadata into groups
    metadata_groups: List[pd.DataFrame] = group_dataframe_by_size(
        metadata=metadata, chunk_size=chunk_size_bytes
    )

    # Save the tortilla
    if len(metadata_groups) == 1:
        return create_a_tortilla(
            metadata=metadata_groups[0],
            output=output,
            ndatapartitions=1,
            file_format=samples.file_format,
            nworkers=nworkers,
            chunk_size_iter_bytes=chunk_size_iter_bytes,
            quiet=quiet,
        )

    # Save the tortilla in parts
    paths = []
    for idx, metadata in enumerate(metadata_groups):
        paths.append(
            create_a_tortilla(
                metadata=metadata,
                output=output.with_suffix(f".{str(idx).zfill(4)}.part.tortilla"),
                ndatapartitions=len(metadata_groups),
                file_format=samples.file_format,
                nworkers=nworkers,
                chunk_size_iter_bytes=chunk_size_iter_bytes,
                quiet=quiet,
            )
        )
    return paths


def create_a_tortilla(
    metadata: pd.DataFrame,
    output: Union[str, pathlib.Path],
    ndatapartitions: int,
    file_format: str,
    nworkers: int,
    chunk_size_iter_bytes: int,
    quiet: bool = False,
) -> pathlib.Path:
    """Create a SINGLE tortilla file 🫓"""

    # Define the magic number
    MB: bytes = b"#y"

    # Estimate the new offset
    metadata.loc[:, "tortilla:offset"] = (
        metadata["tortilla:length"].shift(1, fill_value=0).cumsum() + 200
    )

    # Number of bytes in the DATA pile
    bytes_counter: int = (
        metadata.iloc[-1]["tortilla:length"] + metadata.iloc[-1]["tortilla:offset"]
    )

    # Drop the internal path
    internal_path = metadata["internal:path"].tolist()
    metadata.drop(columns=["internal:path"], inplace=True)

    # Create an in-memory Parquet file with BufferOutputStream
    with pa.BufferOutputStream() as sink:
        pq.write_table(
            pa.Table.from_pandas(metadata),
            sink,
            compression="zstd",  # Highly efficient codec
            compression_level=22,  # Maximum compression for Zstandard
            use_dictionary=False,  # Optimizes for repeated values
        )
        # return a blob of the in-memory Parquet file as bytes
        # Obtain the FOOTER metadata
        FOOTER: bytes = sink.getvalue().to_pybytes()

    # Define the FOOTER length and offset
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = int(bytes_counter).to_bytes(8, "little")

    # Data Format of items
    DF: int = file_format.encode().ljust(24)

    # Get the total size of the files
    total_size: int = len(FOOTER) + bytes_counter

    # If the folder does not exist, create it
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Create an empty file
    with open(output, "wb") as f:
        f.truncate(total_size)

    # Define the function to write into the main file
    def write_file(file, offset, length):
        """read the file in chunks"""
        with open(file, "rb") as g:
            while True:
                chunk = g.read(chunk_size_iter_bytes)
                if not chunk:
                    break
                mm[offset : (offset + length)] = chunk

    # Cook the tortilla 🫓
    with open(output, "r+b") as f:
        with mmap.mmap(f.fileno(), 0) as mm:
            # Write the magic number (MB)
            mm[:2] = MB

            # Write the FOOTER offset (FO)
            mm[2:10] = FO

            # Write the FOOTER length (FL)
            mm[10:18] = FL

            # Write the DATA format (DF)
            mm[18:42] = DF

            # Write the number of data partitions (DP)
            mm[42:50] = ndatapartitions.to_bytes(8, "little")

            # Write the free space (FP)
            mm[50:200] = b"\0" * 150

            # Write the DATA pile
            message = tortilla_message()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=nworkers
            ) as executor:

                # Submit the files to be written
                futures = []
                for path, offset, length in zip(
                    internal_path,
                    metadata["tortilla:offset"],
                    metadata["tortilla:length"],
                ):
                    futures.append(executor.submit(write_file, path, offset, length))

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
            mm[bytes_counter:(bytes_counter + len(FOOTER))] = FOOTER

    return output
