import json
import pathlib
from pathlib import Path
from unittest.mock import mock_open, patch

import pandas as pd
import pytest

from pytortilla.compile import compile, create, load


class TestCreateFunction:

    @pytest.fixture
    def mock_files(self):
        return [Path(f"file{i}.txt") for i in range(3)]  # Mock list of files

    @pytest.fixture
    def mock_output(self):
        return Path("output.tortilla")  # Mock output file path

    @pytest.fixture
    def mock_file_format(self):
        return "GTiff"  # Mock file format

    def test_create_valid_input(self, mock_files, mock_output, mock_file_format):
        # Test with valid input (correct files and format)
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pytortilla.utils.process_files_concurrently") as mock_process:
                mock_process.return_value = (
                    {"file0": (0, 1024), "file1": (1024, 1024), "file2": (2048, 1024)},
                    3072,
                )
                with patch("os.path.getsize", return_value=50 + 3072 + 50):
                    with patch("mmap.mmap") as mock_mmap:
                        mock_mmap.return_value.__enter__.return_value = bytearray(
                            50 + 3072 + 50
                        )
                        output = create(mock_files, mock_output, mock_file_format)
                        assert output == mock_output  # Check output file path

    def test_create_invalid_file_format(self, mock_files, mock_output):
        # Test with an invalid file format
        invalid_format = "txt"
        with pytest.raises(ValueError, match=r"Invalid file format: txt"):
            create(mock_files, mock_output, invalid_format)

    def test_create_directory_creation(self, mock_files, mock_output, mock_file_format):
        # Test if directory is created if it doesn't exist
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pytortilla.utils.process_files_concurrently") as mock_process:
                mock_process.return_value = (
                    {"file0": (0, 1024), "file1": (1024, 1024), "file2": (2048, 1024)},
                    3072,
                )
                with patch("os.path.getsize", return_value=50 + 3072 + 50):
                    with patch("mmap.mmap") as mock_mmap:
                        mock_mmap.return_value.__enter__.return_value = bytearray(
                            50 + 3072 + 50
                        )
                        with patch("pathlib.Path.mkdir") as mock_mkdir:
                            output = create(mock_files, mock_output, mock_file_format)
                            mock_mkdir.assert_called_once_with(
                                parents=True, exist_ok=True
                            )

    def test_create_file_size(self, mock_files, mock_output, mock_file_format):
        # Test that file is created with correct size
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pytortilla.utils.process_files_concurrently") as mock_process:
                mock_process.return_value = (
                    {"file0": (0, 1024), "file1": (1024, 1024), "file2": (2048, 1024)},
                    3072,
                )
                with patch("os.path.getsize", return_value=50 + 3072 + 50):
                    with patch("mmap.mmap") as mock_mmap:
                        mock_mmap.return_value.__enter__.return_value = bytearray(
                            50 + 3072 + 50
                        )
                        output = create(mock_files, mock_output, mock_file_format)
                        # Check if the file size matches
                        mock_file().truncate.assert_called_once_with(
                            50
                            + 3072
                            + len(json.dumps(mock_process.return_value[0]).encode())
                        )

    def test_create_quiet_mode(self, mock_files, mock_output, mock_file_format):
        # Test behavior when quiet mode is enabled (should not call tqdm)
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pytortilla.utils.process_files_concurrently") as mock_process:
                mock_process.return_value = (
                    {"file0": (0, 1024), "file1": (1024, 1024), "file2": (2048, 1024)},
                    3072,
                )
                with patch("os.path.getsize", return_value=50 + 3072 + 50):
                    with patch("mmap.mmap") as mock_mmap:
                        mock_mmap.return_value.__enter__.return_value = bytearray(
                            50 + 3072 + 50
                        )
                        with patch("tqdm.tqdm") as mock_tqdm:
                            output = create(
                                mock_files, mock_output, mock_file_format, quiet=True
                            )
                            mock_tqdm.assert_not_called()  # Verify tqdm is not called

    def test_write_file_handling(self, mock_files, mock_output, mock_file_format):
        # Test handling file exceptions (FileNotFoundError)
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pytortilla.utils.process_files_concurrently") as mock_process:
                mock_process.return_value = (
                    {"file0": (0, 1024), "file1": (1024, 1024), "file2": (2048, 1024)},
                    3072,
                )
                with patch("os.path.getsize", return_value=50 + 3072 + 50):
                    with patch("mmap.mmap") as mock_mmap:
                        mock_mmap.return_value.__enter__.return_value = bytearray(
                            50 + 3072 + 50
                        )

                        # Simulate a FileNotFoundError when opening a file
                        with patch(
                            "builtins.open",
                            side_effect=FileNotFoundError("File not found"),
                        ):
                            with pytest.raises(
                                FileNotFoundError, match="File not found"
                            ):
                                create(mock_files, mock_output, mock_file_format)


class TestLoadFunction:

    @patch("pytortilla.utils.is_valid_url")
    @patch("pytortilla.core.read_tortilla_metadata_online")
    @patch("pytortilla.core.read_tortilla_metadata_local")
    def test_load_valid_url(self, mock_read_local, mock_read_online, mock_is_valid_url):
        # Test loading metadata from a valid URL
        mock_is_valid_url.return_value = True
        mock_read_online.return_value = pd.DataFrame(
            {"tortilla:item_offset": [0], "tortilla:item_length": [100]}
        )

        result = load("http://example.com/tortilla_file.tortilla")

        assert (
            result["tortilla:subfile"].iloc[0]
            == "/vsisubfile/0_100,/vsicurl/http://example.com/tortilla_file.tortilla"
        )
        mock_read_online.assert_called_once()

    @patch("pytortilla.core.read_tortilla_metadata_local")
    def test_load_valid_local_file(self, mock_read_local):
        # Test loading metadata from a local file
        mock_read_local.return_value = pd.DataFrame(
            {"tortilla:item_offset": [0], "tortilla:item_length": [100]}
        )

        result = load(pathlib.Path("local_file.tortilla"))

        assert (
            result["tortilla:subfile"].iloc[0]
            == "/vsisubfile/0_100,local_file.tortilla"
        )
        mock_read_local.assert_called_once()

    def test_load_invalid_file_type(self):
        # Test loading with an invalid file type
        with pytest.raises(
            ValueError, match="Invalid file type. Must be a string or pathlib.Path."
        ):
            load(12345)  # Invalid type


class TestCompileFunction:

    @pytest.fixture
    def mock_dataset(self):
        # Mock dataset for testing compile
        return pd.DataFrame(
            {
                "tortilla:item_offset": [0, 100],
                "tortilla:item_length": [100, 200],
                "tortilla:mode": ["local", "local"],
                "tortilla:subfile": [
                    "/vsisubfile/0_100,local_file.tortilla",
                    "/vsisubfile/100_200,local_file.tortilla",
                ],
            }
        )

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.unlink")
    @patch("pytortilla.core.compile_local")
    def test_compile_force(
        self, mock_compile_local, mock_unlink, mock_exists, mock_mkdir, mock_dataset
    ):
        # Test compile with force flag to overwrite existing file
        mock_exists.return_value = True  # Simulate that file exists
        output_path = pathlib.Path("output.tortilla")
        output_path.touch()  # Create the file

        result = compile(mock_dataset, output_path, force=True)

        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_compile_local.assert_called_once()  # Ensure compile_local is called
        assert result == output_path

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pytortilla.core.compile_online")
    def test_compile_with_online_mode(
        self, mock_compile_online, mock_exists, mock_mkdir, mock_dataset
    ):
        # Test compile in online mode
        mock_exists.return_value = False  # Simulate file doesn't exist
        mock_dataset.loc[0, "tortilla:mode"] = "online"  # Change mode to online

        output_path = pathlib.Path("output_online.tortilla")
        result = compile(mock_dataset, output_path)

        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_compile_online.assert_called_once()  # Ensure compile_online is called
        assert result == output_path
