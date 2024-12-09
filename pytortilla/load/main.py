import pathlib
from typing import List, Union

import pandas as pd

from .load_local import file2metadata as local_file2metadata
from .load_local import files2metadata as local_files2metadata
from .load_remote import file2metadata as remote_file2metadata
from .load_remote import files2metadata as remote_files2metadata
from .utils import is_valid_url, snippet2files


def load(file: Union[str, pathlib.Path, List[pathlib.Path], List[str]]) -> pd.DataFrame:
    """Load the metadata of a tortilla file.

    Args:
        file (Union[str, pathlib.Path, List]): The path of
            the tortilla file. If the file is split into multiple
            parts, a list of paths is accepted. Also, multiple
            parts can be read by putting a asterisk (*) at the end
            of the file name. For example, "file*.tortilla". In this
            case, the function will create a list will all the partitions
            before the reading process.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """

    # Transform our snippet into a list of files
    # If it is not a snippet, it will return the same file
    file = snippet2files(file=file)

    if isinstance(file, list):
        if is_valid_url(file):
            metadata = remote_files2metadata(file)
        else:
            metadata = local_files2metadata(file)
    elif isinstance(file, (str, pathlib.Path)):
        if is_valid_url(file):
            metadata = remote_file2metadata(file)
        else:
            metadata = local_file2metadata(file)
    else:
        raise ValueError("Invalid file type. Must be a list, string or pathlib.Path.")

    return metadata
