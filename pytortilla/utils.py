import concurrent.futures
import pathlib
import random
from urllib.parse import urlparse


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
        "Making a tortilla 🫓:",
        "Cooking a tortilla",
        "Making a tortilla 🫓:",
        "Working on a tortilla",
        "Working on a tortilla 🫓:",
        "Rolling out a tortilla",
        "Rolling out a tortilla 🫓:",
        "Baking a tortilla",
        "Baking a tortilla 🫓:",
        "Grilling a tortilla",
        "Grilling a tortilla 🫓:",
        "Toasting a tortilla",
        "Toasting a tortilla 🫓:",
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