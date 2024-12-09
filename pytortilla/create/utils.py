import random

import pandas as pd


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


def human2bytes(size_str: str) -> int:
    """
    Converts a human-readable size string (e.g., "100MB") into bytes.
    Supported units: KB, MB, GB, TB, PB.
    """
    units = {"KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12, "PB": 10**15}
    size_str = size_str.strip().upper()

    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            try:
                value = float(size_str[: -len(unit)].strip())
                return int(value * multiplier)
            except ValueError:
                raise ValueError(f"Invalid size value in '{size_str}'.")
    raise ValueError(
        f"Unsupported unit in '{size_str}'. Supported units are: {', '.join(units.keys())}."
    )


def bytes2human(size_bytes: int) -> str:
    """
    Converts a size in bytes into a human-readable size string.
    Automatically selects the largest appropriate unit.
    """
    units = [
        ("PB", 10**15),
        ("TB", 10**12),
        ("GB", 10**9),
        ("MB", 10**6),
        ("KB", 10**3),
    ]

    for unit, multiplier in units:
        if size_bytes >= multiplier:
            value = size_bytes / multiplier
            return f"{value:.2f}{unit}"
    return f"{size_bytes}B"  # Bytes if smaller than 1KB


def group_dataframe_by_size(
    metadata: pd.DataFrame, chunk_size: int
) -> list[pd.DataFrame]:
    """
    Aggregate rows of a DataFrame into groups, where the total size of each group
    does not exceed a given chunk size.

    Args:
        metadata (pd.DataFrame): A DataFrame containing metadata.
        chunk_size (int): Maximum total size for each group.

    Returns:
        list[pd.DataFrame]: A list of DataFrames, each containing a group of rows.
    """
    groups = []
    current_group = []
    current_sum = 0

    for index, row in metadata.iterrows():
        size = row["tortilla:length"]
        if current_sum + size <= chunk_size:
            current_group.append(index)
            current_sum += size
        else:
            groups.append(metadata.loc[current_group])
            current_group = [index]
            current_sum = size

    if current_group:  # Add the last group if it has rows
        groups.append(metadata.loc[current_group])

    return groups
