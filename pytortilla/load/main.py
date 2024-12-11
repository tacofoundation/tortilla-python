import re
import pathlib
from typing import List, Union

import pandas as pd
import requests

from .load_local import file2metadata as local_file2metadata
from .load_local import files2metadata as local_files2metadata
from .load_local import lazyfile2metadata as lazy_local_file2metadata
from .load_remote import file2metadata as remote_file2metadata
from .load_remote import files2metadata as remote_files2metadata
from .load_remote import lazyfile2metadata as lazy_remote_file2metadata
from .utils import is_valid_url, snippet2files, sort_columns_add_geometry

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

    # Clean up the metadata
    metadata = sort_columns_add_geometry(metadata)

    return TortillaDataFrame(metadata)


def lazy_load(offset: int, file: Union[str, pathlib.Path]) -> pd.DataFrame:
    """ Lazy load a tortilla file.

    Useful for datasets that have tortillas as samples (tortillas inside tortillas).
    The offset is used to read a specific part of the main tortilla file.
    
    Args:
        offset (int): The byte offset where the reading process will start.        
        file (Union[str, pathlib.Path]): The path tot the main tortilla file.

    Returns:
        pd.DataFrame: The metadata of the tortilla file.
    """
    
    if is_valid_url(file):
        metadata = lazy_remote_file2metadata(offset, file)
    else:
        metadata = lazy_local_file2metadata(offset, file)

    # Clean up the metadata
    metadata = sort_columns_add_geometry(metadata)

    return TortillaDataFrame(metadata)
    

class TortillaDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return TortillaDataFrame

    @staticmethod
    def get_internal_path(row):
        pattern: re.Pattern = re.compile(r"/vsisubfile/(\d+)_(\d+),(.+)")
        offset, length, path = pattern.match(row["internal:subfile"]).groups()
        
        # If it is a curl file, remove the first 9 characters
        if path.startswith('/vsicurl/'):
            path = path[9:]
        
        return int(offset), int(length), path

    def read(self, idx):
        row = self.iloc[idx]        
        if row["internal:file_format"] == "TORTILLA":
            offset, length, path = self.get_internal_path(row)
            return lazy_load(row["tortilla:offset"], path)
        elif row["internal:file_format"] == "BYTES":
            
            # Obtain the offset, length and internal path
            offset, length, path = self.get_internal_path(row)
            
            # Get the bytes
            if is_valid_url(path):
                headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
                response: requests.Response = requests.get(path, headers=headers)
                return response.content
            else:
                with open(path, "rb") as f:
                    f.seek(int(offset))
                    return f.read(int(length))
        else:
            return row["internal:subfile"]