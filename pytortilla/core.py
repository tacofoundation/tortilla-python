import concurrent.futures
import json
import mmap
import pathlib
import re
from typing import List, Tuple, Union

import pandas as pd
import requests
import tqdm

import pytortilla.utils


def read_tortilla_metadata_local(file: Union[str, pathlib.Path]) -> pd.DataFrame:
    """Read the metadata of a tortilla file given a local path.

    Args:
        file (Union[str, pathlib.Path]): A tortilla file in the local filesystem.

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
        metadata[["tortilla:item_offset", "tortilla:item_length"]] = pd.DataFrame(
            metadata["values"].tolist(), index=metadata.index
        )
        metadata = metadata.drop(columns="values")
        metadata["tortilla:file_format"] = DF
        metadata["tortilla:mode"] = "local"

    return metadata


def read_tortilla_metadata_online(file: str) -> pd.DataFrame:
    """Read the metadata of a tortilla file given a URL. The
        server must support HTTP Range requests.

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
        metadata[["tortilla:item_offset", "tortilla:item_length"]] = pd.DataFrame(
            metadata["values"].tolist(), index=metadata.index
        )
        metadata = metadata.drop(columns="values")
        metadata["tortilla:file_format"] = DF
        metadata["tortilla:mode"] = "online"

    return metadata


def compile_local(
    dataset: pd.DataFrame, output: str, chunk_size: int, nworkers: int, quiet: bool
) -> None:
    """Prepare a subset of a local Tortilla file and write it to a new file.

    Args:
        dataset (pd.DataFrame): The metadata of the Tortilla file.
        output (str): The path to the Tortilla file.
        chunk_size (int): The size of the chunks to use when writing
            the tortilla.
    """
    # Get the name of the file
    file = dataset["tortilla:subfile"].iloc[0].split(",")[-1]

    # Estimate the new offset
    dataset.loc[:, "tortilla:item_new_offset"] = (
        dataset["tortilla:item_length"].shift(1, fill_value=0).cumsum() + 50
    )

    # From DataFrame to FOOTER
    keys: List[str] = dataset["id"].tolist()
    new_values: List[Tuple[int, int]] = dataset[
        ["tortilla:item_new_offset", "tortilla:item_length"]
    ].values.tolist()

    FOOTER = json.dumps(dict(zip(keys, new_values))).encode()

    # Calculate the bytes of the data blob (DATA)
    bytes_counter: int = sum(dataset["tortilla:item_length"])

    # Prepare the static bytes
    MB: bytes = b"#y"
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = (50 + bytes_counter).to_bytes(8, "little")
    DF: bytes = dataset["tortilla:file_format"].iloc[0].encode().ljust(32)

    # Create the tortilla file (empty)
    with open(output, "wb") as f:
        f.truncate(50 + bytes_counter + len(FOOTER))

    # Define the function to write into the main file
    def write_file(file, old_offset, length, new_offset):
        """read the file in chunks"""
        with open(file, "rb") as g:
            g.seek(old_offset)
            while True:
                # Iterate over the file in chunks until the length is 0
                if chunk_size > length:
                    chunk = g.read(length)
                    length = 0
                else:
                    chunk = g.read(chunk_size)
                    length -= chunk_size

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
            mm[18:50] = DF

            # Write the DATA
            message = pytortilla.utils.tortilla_message()
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=nworkers
            ) as executor:
                futures = []
                for _, item in dataset.iterrows():
                    old_offset = item["tortilla:item_offset"]
                    new_offset = item["tortilla:item_new_offset"]
                    length = item["tortilla:item_length"]
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
            mm[(50 + bytes_counter) :] = FOOTER

    return None


def compile_online(
    dataset: pd.DataFrame, output: str, chunk_size: int, quiet: bool
) -> None:
    """Prepare a subset of an online Tortilla file and write it to a new local file.

    Args:
        dataset (pd.DataFrame): The metadata of the Tortilla file.
        output (str): The path to the Tortilla file.
        chunk_size (int): The size of the chunks to use when writing
            the tortilla.
        quiet (bool): Whether to suppress the progress bar.

    Returns:
        None
    """

    # Get the URL of the file
    url_pattern = r"(ftp|https?)://[^\s,]+"
    url = re.search(url_pattern, dataset["tortilla:subfile"].iloc[0]).group(0)

    # Calculate the new offsets
    dataset["tortilla:item_new_offset"] = (
        dataset["tortilla:item_length"].shift(1, fill_value=0).cumsum() + 50
    )

    # Convert DataFrame to FOOTER
    ids: List[str] = dataset["id"].tolist()
    new_values: List[Tuple[int, int]] = dataset[
        ["tortilla:item_new_offset", "tortilla:item_length"]
    ].values.tolist()
    FOOTER = json.dumps(dict(zip(ids, new_values))).encode()

    # Calculate the total size of the data
    total_bytes: int = sum(dataset["tortilla:item_length"])

    # Prepare static bytes
    MB: bytes = b"#y"
    FL: bytes = len(FOOTER).to_bytes(8, "little")
    FO: bytes = (50 + total_bytes).to_bytes(8, "little")
    DF: bytes = dataset["tortilla:file_format"].iloc[0].encode().ljust(32)

    # Get checksum
    checksum: int = 50 + total_bytes + len(FOOTER)

    # Build range headers
    headers = pytortilla.utils.build_simplified_range_header(dataset)

    # Check if the file exists and determine the download start point
    start = 50
    output_path = pathlib.Path(output)
    if output_path.exists():
        start = output_path.stat().st_size

    # Check if the download is complete
    if start == checksum:
        return None

    # Open the file once and write metadata and footer later
    message: str = pytortilla.utils.tortilla_message()
    with open(output, "ab") as f:
        # Start writing the header (first write)
        if start == 50:
            f.write(MB)
            f.write(FO)
            f.write(FL)
            f.write(DF)

        # Download and write the data in chunks
        try:
            with requests.get(
                url, headers=headers, stream=True, timeout=10
            ) as response:
                response.raise_for_status()

                # Resume download if needed
                if start != 50 and not quiet:
                    print(
                        f"Resuming download from byte {start} ({start / (1024 ** 3):.2f} GB)"
                    )

                # Calculate total size for progress bar
                total_download_size = response.headers.get("content-length")
                if total_download_size is None:
                    total_download_size = total_bytes
                total_download_size = int(total_download_size)

                # Use tqdm for progress bar if not in quiet mode
                with tqdm.tqdm(
                    total=total_download_size,
                    unit="B",
                    unit_scale=True,
                    disable=quiet,
                    desc=message,
                ) as pbar:
                    # Write the downloaded data in chunks and update the progress bar
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:  # Filter out keep-alive new chunks
                            f.write(chunk)
                            pbar.update(len(chunk))  # Update progress bar

        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}")
            return None

        # Write the FOOTER once download completes
        f.write(FOOTER)

    return None
