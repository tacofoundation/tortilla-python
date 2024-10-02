import concurrent.futures
import json
import mmap
import pathlib
from typing import List, Union

import pandas as pd
import tqdm

import pytortilla.core
import pytortilla.utils
import pytortilla.utils_gdal


def create(
    files: Union[List[str], List[pathlib.Path]],
    output: Union[str, pathlib.Path],
    file_format: pytortilla.utils_gdal.GDAL_FILES,
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
            the tortilla. Defaults to 1024 * 1024 * 100 (100 MB).

    Returns:
        pathlib.Path: The tortilla file.
    """

    if file_format not in pytortilla.utils_gdal.GDAL_FILES.__args__:
        raise ValueError(
            f"Invalid file format: {file_format}. Must be one of {pytortilla.utils_gdal.GDAL_FILES.__args__}."
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


def load(file: Union[str, pathlib.Path]) -> pd.DataFrame:
    """Load the metadata of a tortilla file.

    Args:
        file (Union[str, pathlib.Path]): A tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """
    if isinstance(file, str):
        if pytortilla.utils.is_valid_url(file):
            metadata: pd.DataFrame = pytortilla.core.read_tortilla_metadata_online(file)
            partial_file: pd.Series = metadata.apply(
                lambda row: f"/vsisubfile/{row['tortilla:item_offset']}_{row['tortilla:item_length']},/vsicurl/{file}",
                axis=1,
            )
        else:
            metadata: pd.DataFrame = pytortilla.core.read_tortilla_metadata_local(file)
            partial_file: pd.Series = metadata.apply(
                lambda row: f"/vsisubfile/{row['tortilla:item_offset']}_{row['tortilla:item_length']},{file}",
                axis=1,
            )

    elif isinstance(file, pathlib.Path):
        metadata: pd.DataFrame = pytortilla.core.read_tortilla_metadata_local(file)
        partial_file: pd.Series = metadata.apply(
            lambda row: f"/vsisubfile/{row['tortilla:item_offset']}_{row['tortilla:item_length']},{file}",
            axis=1,
        )
    else:
        raise ValueError("Invalid file type. Must be a string or pathlib.Path.")

    metadata["tortilla:subfile"] = partial_file

    return metadata


def compile(
    dataset: pd.DataFrame,
    output: Union[str, pathlib.Path],
    chunk_size: int = 1024 * 1024 * 100,
    nworkers: int = 4,
    force: bool = False,
    quiet: bool = False,
) -> pathlib.Path:
    """Prepare a subset of a Tortilla file and write it to a new local file.

    Args:
        dataframe (pd.DataFrame): A subset of the metadata of a Tortilla file.
        output_folder (Union[str, pathlib.Path]): The folder where the Tortilla file
            will be saved. If the folder does not exist, it will be created.
        chunk_size (int, optional): The size of the chunks to use when writing
            the tortilla. Defaults to 1024 * 1024 * 100 (100 MB).
        nworkers (int, optional): The number of workers to use when writing
            the tortilla. Defaults to 4. Only used when the Tortilla file is
            local.
        force (bool, optional): If True, the function overwrites the file if it
            already exists. By default, it is False.
        quiet (bool, optional): If True, the function does not print any
            message. By default, it is False.
    Returns:
        pathlib.Path: The path to the Tortilla file.
    """

    # If the folder does not exist, create it
    output = pathlib.Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Check if the file already exists
    if output.exists() and force:
        output.unlink()

    # Remove the index from the previous dataset
    dataset = dataset.copy()
    dataset.sort_values("tortilla:item_offset", inplace=True)
    dataset.reset_index(drop=True, inplace=True)

    # Compile your tortilla
    if dataset["tortilla:mode"].iloc[0] == "local":
        pytortilla.core.compile_local(dataset, output, chunk_size, nworkers, quiet)

    if dataset["tortilla:mode"].iloc[0] == "online":
        pytortilla.core.compile_online(dataset, output, chunk_size, quiet)

    return pathlib.Path(output)
