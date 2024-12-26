import concurrent.futures
import mmap
import os
import pathlib
from typing import List, Union, Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

from pytortilla.create import utils
from pytortilla.datamodel.main import Samples


def create(
    samples: Samples,
    output: Union[str, pathlib.Path],
    nworkers: int = min(4, os.cpu_count()),
    chunk_size: str = "20GB",
    chunk_size_iter: str = "100MB",
    tortilla_message: Callable[[], str] = utils.tortilla_message,
    quiet: bool = False
) -> Union[pathlib.Path, List[pathlib.Path]]:
    """Create a tortilla 🫓

    A tortilla is a new simple format for storing files
    optimized for partial & very fast random access.

    Args:
        samples (Samples): The list of samples to be included in
            the tortilla file. The Sample objects must have a unique
            `id` field.
        output (Union[str, pathlib.Path]): The path where the tortilla
            file will be saved.
        nworkers (int, optional): The number of workers to use when writing
            the tortilla. Defaults to 4. As the writing process is I/O bound,
            the number of workers should be significantly lower than the number
            of cores in the machine.
        chunk_size (str, optional): Avoid large tortilla files by splitting
            the data into chunks. By default, if the number of samples exceeds
            20GB, the data will be split into chunks of 20GB. The partitions
            can be read independently, as each partition contains the metadata
            of the samples it contains.
        chunk_size_iter (int, optional): The writting chunk size. By default,
            it is 100MB. Faster computers can use a larger chunk size.
        tortilla_message (Callable[[], str], optional): A function that 
            returns the message to be displayed when creating the tortilla.
            By default, it is utils.tortilla_message which returns a random
            message.
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
    if not quiet:
        print("Reading metadata from samples...")
    metadata: pd.DataFrame = samples.export_metadata(nworkers=nworkers, chunk_size=1000)

    # From human-readable to bytes
    chunk_size_iter_bytes: int = utils.human2bytes(chunk_size_iter)
    chunk_size_bytes: int = utils.human2bytes(chunk_size)

    # Aggregate metadata into groups
    metadata_groups: List[pd.DataFrame] = utils.group_dataframe_by_size(
        metadata=metadata, chunk_size=chunk_size_bytes
    )

    # Save the tortilla
    if len(metadata_groups) == 1:
        return create_a_tortilla(
            metadata=metadata_groups[0],
            output=output,
            ndatapartitions=1,
            nworkers=nworkers,
            chunk_size_iter_bytes=chunk_size_iter_bytes,
            tortilla_message=tortilla_message,
            quiet=quiet,
        )

    # Save the tortilla in parts
    paths = []
    for idx, metadata in enumerate(metadata_groups):
        output_suffix: str = output.suffix
        paths.append(
            create_a_tortilla(
                metadata=metadata,
                output=output.with_suffix(f".{str(idx).zfill(4)}.part{output_suffix}"),
                ndatapartitions=len(metadata_groups),
                nworkers=nworkers,
                chunk_size_iter_bytes=chunk_size_iter_bytes,
                tortilla_message=tortilla_message,
                quiet=quiet,
            )
        )
    return paths


def create_a_tortilla(
    metadata: pd.DataFrame,
    output: Union[str, pathlib.Path],
    ndatapartitions: int,
    nworkers: int,
    chunk_size_iter_bytes: int,
    tortilla_message: Callable[[], str],
    quiet: bool = False,
) -> pathlib.Path:
    """Create a SINGLE tortilla file 🫓"""

    # Set the Magic Number (MB)
    # Don't forget 3.oct.11
    MB: bytes = b"#y"

    # Estimate the new offset
    metadata.loc[:, "tortilla:offset"] = (
        metadata["tortilla:length"].shift(1, fill_value=0).cumsum() + 200
    )

    # Number of bytes in the DATA pile
    bytes_counter: int = (
        metadata.iloc[-1]["tortilla:length"] + metadata.iloc[-1]["tortilla:offset"]
    )

    # Drop the internal:path field (the paths to the original files)
    internal_path: List[str] = metadata["internal:path"].tolist()
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
        # This is the FOOTER metadata
        FOOTER: bytes = sink.getvalue().to_pybytes()

    # Define the FOOTER length and offset
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = int(bytes_counter).to_bytes(8, "little")

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
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as mm:
            # Write the magic number (MB)
            mm[:2] = MB

            # Write the FOOTER offset (FO)
            mm[2:10] = FO

            # Write the FOOTER length (FL)
            mm[10:18] = FL

            # Write the number of data partitions (DP)
            mm[18:26] = ndatapartitions.to_bytes(8, "little")

            # Write the free space (FP)
            mm[26:200] = b"\0" * 174

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
            mm[bytes_counter : (bytes_counter + len(FOOTER))] = FOOTER

    return output
