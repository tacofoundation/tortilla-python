import concurrent.futures
import pathlib
import random
from urllib.parse import urlparse

import pandas as pd


def is_valid_url(url: str) -> bool:
    """Check if a URL is valid

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    try:
        result = urlparse(url)
        return all([result.scheme in ["http", "https"], result.netloc])
    except ValueError:
        return False


def tortilla_message() -> str:
    """Get a random tortilla message"""

    tortilla_messages = [
        "Making a tortilla",
        "Making a tortilla 🫓",
        "Cooking a tortilla",
        "Making a tortilla 🫓",
        "Working on a tortilla",
        "Working on a tortilla 🫓",
        "Rolling out a tortilla",
        "Rolling out a tortilla 🫓",
        "Baking a tortilla",
        "Baking a tortilla 🫓",
        "Grilling a tortilla",
        "Grilling a tortilla 🫓",
        "Toasting a tortilla",
        "Toasting a tortilla 🫓",
    ]

    # Randomly accessing a message
    random_message = random.choice(tortilla_messages)
    return random_message


def get_file_size(file):
    """Helper function to get file size and return the stem and size."""
    file_path = pathlib.Path(file)
    file_size = file_path.stat().st_size
    return file_path.stem, file_size


def process_files_concurrently(files, nworkers) -> tuple[dict[str, list[int]], int]:
    """Process files concurrently and return a dictionary with the bytes."""
    bytes_counter = 50
    dict_bytes = {}

    # Use ThreadPoolExecutor to read file sizes concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=nworkers) as executor:
        # Map each file to the get_file_size function
        results = list(executor.map(get_file_size, files))

    # Process the results after gathering them
    for stem, file_size in results:
        dict_bytes[stem] = [bytes_counter, file_size]
        bytes_counter += file_size

    return dict_bytes, bytes_counter


def build_simplified_range_header(df):
    # Create a list to hold the simplified byte ranges
    simplified_ranges = []

    # Get the initial offset and length
    current_start = df.iloc[0]["offset"]
    current_end = current_start + df.iloc[0]["length"]

    # Iterate over the rows starting from the second row
    for i in range(1, len(df)):
        row = df.iloc[i]
        next_start = row["offset"]
        next_end = next_start + row["length"]

        # If the current range and the next range are consecutive, merge them
        if next_start == current_end:
            current_end = next_end
        else:
            # Otherwise, add the current range to the list and start a new one
            simplified_ranges.append((current_start, current_end))
            current_start = next_start
            current_end = next_end

    # Append the final range
    simplified_ranges.append((current_start, current_end))

    # Create the range header
    range_header = ",".join(
        [f"bytes={start}-{end-1}" for start, end in simplified_ranges]
    )
    headers = {"Range": range_header}

    return headers


def build_simplified_range_header(dataset: pd.DataFrame) -> dict[str, str]:
    """Given the metadata of a Tortilla file, build a simplified range header.

    Args:
        dataset (pd.DataFrame): The metadata of a Tortilla file.

    Returns:
        dict[str, str]: A dictionary containing the range header.
    """

    # Create a list to hold the simplified byte ranges
    simplified_ranges = []

    # Get the initial offset and length
    current_start = dataset.iloc[0]["tortilla:item_offset"]
    current_end = current_start + dataset.iloc[0]["tortilla:item_length"]

    # Iterate over the rows starting from the second row
    for i in range(1, len(dataset)):
        row = dataset.iloc[i]
        next_start = row["tortilla:item_offset"]
        next_end = next_start + row["tortilla:item_length"]

        # If the current range and the next range are consecutive, merge them
        if next_start == current_end:
            current_end = next_end
        else:
            # Otherwise, add the current range to the list and start a new one
            simplified_ranges.append((current_start, current_end))
            current_start = next_start
            current_end = next_end

    # Append the final range
    simplified_ranges.append((current_start, current_end))

    # Create the range header
    range_header = ",".join(
        [f"bytes={start}-{end-1}" for start, end in simplified_ranges]
    )
    headers = {"Range": range_header}

    return headers