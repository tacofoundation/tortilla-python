import concurrent.futures
import mmap
import pathlib
import re
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import tqdm

from .utils import build_simplified_range_header, tortilla_message, human2bytes


def compile(
    metadata: pd.DataFrame,
    output: Union[str, pathlib.Path],
    chunk_size_iter: str = "100MB",
    nworkers: int = 4,
    overwrite: bool = True,
    quiet: bool = False,
) -> pathlib.Path:
    """Select a subset of a Tortilla file and write a new "small" Tortilla file.

    Args:
        metadata (pd.DataFrame): A subset of the Tortilla file.
        output_folder (Union[str, pathlib.Path]): The folder where the Tortilla file
            will be saved. If the folder does not exist, it will be created.
        chunk_size_iter (int, optional): The writting chunk size. By default,
            it is 100MB. Faster computers can use a larger chunk size.
        nworkers (int, optional): The number of workers to use when writing
            the tortilla. Defaults to 4.
        overwrite (bool, optional): If True, the function overwrites the file if it
            already exists. By default, it is True.
        quiet (bool, optional): If True, the function does not print any
            message. By default, it is False.
    Returns:
        pathlib.Path: The path to the new Tortilla file.
    """
    # Keep the data format
    data_format: str = metadata["internal:file_format"].iloc[0]

    # From human-readable to bytes
    chunk_size_iter: int = human2bytes(chunk_size_iter)

    # If the folder does not exist, create it
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Check if the file already exists
    if output.exists() and overwrite:
        output.unlink()

    # Remove the index from the previous dataset
    metadata = metadata.copy()
    metadata.sort_values("tortilla:offset", inplace=True)
    metadata.reset_index(drop=True, inplace=True)

    # Compile your tortilla
    mode = metadata["internal:mode"].iloc[0]
    if mode == "local":
        compile_local(metadata, output, chunk_size_iter, nworkers, data_format, quiet)
    elif mode == "online":
        compile_online(metadata, output, chunk_size_iter, data_format, quiet)
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return output


def compile_local(
    metadata: pd.DataFrame,
    output: str,
    chunk_size_iter: int,
    nworkers: int,
    data_format: str,
    quiet: bool
) -> pathlib.Path:
    """Prepare a subset of a local Tortilla file and write it to a new file.

    Args:
        metadata (pd.DataFrame): The metadata of a Tortilla file.
        output (str): The path to the Tortilla file.
        chunk_size_iter (int): The size of the chunks to use when writing
            the tortilla.
        nworkers (int): The number of workers to use when writing the tortilla.        
        data_format (str): The format of the data.
        quiet (bool): Whether to suppress the progress bar.

    Returns:
        pathlib.Path: The path to the new Tortilla file.
    """
    
    # Estimate the new offset
    metadata.loc[:, "tortilla:new_offset"] = (
        metadata["tortilla:length"].shift(1, fill_value=0).cumsum() + 200
    )

    # Create the new FOOTER
    # Remove the columns generated on-the-fly by the load function (internal fields)
    new_footer = metadata.copy()
    new_footer.drop(
        columns=[
            "geometry", "internal:mode", "internal:file_format", "internal:subfile", "tortilla:offset"
        ],
        inplace=True
    )
    new_footer.rename(columns={"tortilla:new_offset": "tortilla:offset"}, inplace=True)  

    # Create an in-memory Parquet file with BufferOutputStream
    with pa.BufferOutputStream() as sink:
        pq.write_table(
            pa.Table.from_pandas(new_footer),
            sink,
            compression="zstd",  # Highly efficient codec
            compression_level=22,  # Maximum compression for Zstandard
            use_dictionary=False,  # Optimizes for repeated values
        )
        # return a blob of the in-memory Parquet file as bytes
        # Obtain the FOOTER metadata
        FOOTER: bytes = sink.getvalue().to_pybytes()

    # Calculate the bytes of the data blob (DATA)
    bytes_counter: int = (
        metadata.iloc[-1]["tortilla:new_offset"] + metadata.iloc[-1]["tortilla:length"]
    )

    # Prepare the static bytes
    MB: bytes = b"#y"
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = int(bytes_counter).to_bytes(8, "little")
    DF: bytes = data_format.encode().ljust(24)
    DP: bytes = int(1).to_bytes(8, "little")

    # Create the tortilla file (empty)
    with open(output, "wb") as f:
        f.truncate(bytes_counter + len(FOOTER))

    # Define the function to write into the main file
    def write_file(file, old_offset, length, new_offset):
        """read the file in chunks"""
        with open(file, "rb") as g:
            g.seek(old_offset)
            while True:
                # Iterate over the file in chunks until the length is 0
                if chunk_size_iter > length:
                    chunk = g.read(length)
                    length = 0
                else:
                    chunk = g.read(chunk_size_iter)
                    length -= chunk_size_iter

                # Write the chunk into the mmap
                mm[new_offset : (new_offset + len(chunk))] = chunk
                new_offset += len(chunk)

                if length == 0:
                    break

    # Cook the tortilla 🫓
    with open(output, "r+b") as f:
        with mmap.mmap(f.fileno(), 0) as mm:
            # Write the magic number
            mm[:2] = MB

            # Write the FOOTER offset
            mm[2:10] = FO

            # Write the FOOTER length
            mm[10:18] = FL

            # Write the DATA FORMAT
            mm[18:42] = DF

            # Write the DATA PARTITIONS
            mm[42:50] = DP

            # Write the free space
            mm[50:200] = b"\0" * 150

            # Write the DATA
            message = tortilla_message()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=nworkers
            ) as executor:
                futures = []
                for _, item in metadata.iterrows():
                    old_offset = item["tortilla:offset"]
                    new_offset = item["tortilla:new_offset"]
                    length = item["tortilla:length"]
                    file = metadata["internal:subfile"].iloc[0].split(",")[-1]
                    futures.append(
                        executor.submit(
                            write_file, file, old_offset, length, new_offset
                        )
                    )

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

    return pathlib.Path(output)


def compile_online(
    metadata: pd.DataFrame,
    output: str,
    chunk_size_iter: int,
    data_format: str,
    quiet: bool
) -> pathlib.Path:
    """Prepare a subset of an online Tortilla file and write it to a new local file.

    Args:
        metadata (pd.DataFrame): The metadata of the Tortilla file.
        output (str): The path to the Tortilla file.
        chunk_size_iter (int, optional): The size of the chunks to use when writing
            the tortilla.
        data_format (str): The format of the data.
        quiet (bool): Whether to suppress the progress bar.

    Returns:
        pathlib.Path: The path to the new Tortilla file.
    """

    # Get the URL of the file
    url_pattern = r"(ftp|https?)://[^\s,]+"
    url = re.search(url_pattern, metadata["internal:subfile"].iloc[0]).group(0)

    # Calculate the new offsets
    metadata["tortilla:new_offset"] = (
        metadata["tortilla:length"].shift(1, fill_value=0).cumsum() + 200
    )

    # Create the new FOOTER
    # Remove the columns generated on-the-fly by the load function
    new_footer = metadata.copy()
    new_footer.drop(
        columns=["geometry", "internal:mode", "internal:file_format", "internal:subfile", "tortilla:offset"],
        inplace=True
    )
    new_footer.rename(columns={"tortilla:new_offset": "tortilla:offset"}, inplace=True)

    # Create an in-memory Parquet file with BufferOutputStream
    with pa.BufferOutputStream() as sink:
        pq.write_table(
            pa.Table.from_pandas(new_footer),
            sink,
            compression="zstd",  # Highly efficient codec
            compression_level=22,  # Maximum compression for Zstandard
            use_dictionary=False,  # Optimizes for repeated values
        )
        # return a blob of the in-memory Parquet file as bytes
        # Obtain the FOOTER metadata
        FOOTER: bytes = sink.getvalue().to_pybytes()

    # Calculate the total size of the data
    total_bytes: int = (
        metadata.iloc[-1]["tortilla:new_offset"] + metadata.iloc[-1]["tortilla:length"]
    )

    # Prepare static bytes
    MB: bytes = b"#y"
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = int(total_bytes).to_bytes(8, "little")
    DF: bytes = data_format.encode().ljust(24)
    DP: bytes = int(1).to_bytes(8, "little")

    # Get checksum
    checksum: int = total_bytes + len(FOOTER)

    # Merge multiple ranges into a single range header
    headers = build_simplified_range_header(metadata)

    # Check if the file exists and determine the download start point
    start = 200
    output_path = pathlib.Path(output)
    if output_path.exists():
        start = output_path.stat().st_size

    # Check if the download is complete
    if start == checksum:
        return None

    # Open the file once and write metadata and footer later
    message: str = tortilla_message()
    with open(output, "ab") as f:
        # Start writing the header (first write)
        if start == 200:
            f.write(MB)
            f.write(FO)
            f.write(FL)
            f.write(DF)
            f.write(DP)
            f.write(b"\0" * 150)  # write free space

        # Download and write the data in chunks
        try:
            with requests.get(
                url, headers=headers, stream=True, timeout=10
            ) as response:
                response.raise_for_status()

                # Resume download if needed
                if start != 200 and not quiet:
                    print(
                        f"Resuming download from byte {start} ({start / (1024 ** 3):.2f} GB)"
                    )

                # Calculate total size for progress bar
                total_download_size = int(total_bytes)

                # Use tqdm for progress bar if not in quiet mode
                with tqdm.tqdm(
                    total=total_download_size,
                    unit="B",
                    unit_scale=True,
                    disable=quiet,
                    desc=message,
                ) as pbar:
                    # Write the downloaded data in chunks and update the progress bar
                    for chunk in response.iter_content(chunk_size=chunk_size_iter):
                        if chunk:  # Filter out keep-alive new chunks
                            f.write(chunk)
                            pbar.update(len(chunk))  # Update progress bar

        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}")
            return None

        # Write the FOOTER once download completes
        f.write(FOOTER)

    return None