import json
import struct
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pytortilla.utils import (build_simplified_range_header, get_file_size,
                              is_valid_url, process_files_concurrently,
                              tortilla_message)


class TestIsValidURL:

    # Test that valid URLs return True
    def test_valid_urls(self):
        assert is_valid_url("http://example.com")
        assert is_valid_url("https://example.com")

    # Test that invalid URLs return False
    def test_invalid_urls(self):
        assert not is_valid_url("ftp://example.com")
        assert not is_valid_url("example")
        assert not is_valid_url("")

    # Test that ValueError is handled correctly by is_valid_url when urlparse raises an error
    def test_urlparse_value_error(self):
        with patch("pytortilla.utils.urlparse", side_effect=ValueError):
            assert not is_valid_url("http://example.com")


class TestTortillaMessage:

    # Test that tortilla_message returns one of the valid tortilla messages
    def test_tortilla_message_valid(self):
        expected_messages = [
            "Making a tortilla",
            "Making a tortilla 🫓",
            "Cooking a tortilla",
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

        message = tortilla_message()  # Get the message from tortilla_message function
        assert message in expected_messages

    # Test that tortilla_message behaves randomly (by mocking the random.choice function)
    @patch("random.choice")
    def test_tortilla_message_randomness(self, mock_random_choice):
        """Test that tortilla_message returns the mocked message."""
        mock_random_choice.return_value = "Mocked tortilla message"

        message = tortilla_message()
        assert message == "Mocked tortilla message"


class TestGetFileSize:

    @patch("pathlib.Path")
    def test_get_file_size(self, mock_path):
        """Test that get_file_size returns correct stem and size."""
        # Mock file size and stem
        mock_file = MagicMock()
        mock_file.stat.return_value.st_size = 1024  # Mock size
        mock_file.stem = "mock_file"  # Mock stem

        mock_path.return_value = mock_file  # Mock Path to return the mock file

        # Call function and check result
        stem, size = get_file_size("mock_file.txt")
        assert stem == "mock_file"  # Check stem
        assert size == 1024  # Check size

    @patch("pathlib.Path")
    def test_get_file_size_empty_file(self, mock_path):
        """Test get_file_size for an empty file."""
        # Mock empty file size and stem
        mock_file = MagicMock()
        mock_file.stat.return_value.st_size = 0  # Size is 0
        mock_file.stem = "empty_file"  # Stem is "empty_file"

        mock_path.return_value = mock_file  # Mock Path to return the mock file

        # Call function and check result
        stem, size = get_file_size("empty_file.txt")
        assert stem == "empty_file"  # Check stem
        assert size == 0  # Check size


class TestProcessFilesConcurrent:

    @patch("pytortilla.utils.get_file_size")
    def test_process_files_concurrently(self, mock_get_file_size):
        """Test process_files_concurrently handles file processing."""
        # Mock file sizes for multiple files
        mock_get_file_size.side_effect = [
            ("file1", 100),
            ("file2", 200),
            ("file3", 150),
        ]

        files = ["file1.txt", "file2.txt", "file3.txt"]
        nworkers = 3

        # Call function and check result
        result, total_bytes = process_files_concurrently(files, nworkers)

        expected_dict = {"file1": [50, 100], "file2": [150, 200], "file3": [350, 150]}

        assert result == expected_dict  # Check processed data
        assert total_bytes == 500  # Check total bytes

    @patch("pytortilla.utils.get_file_size")
    def test_process_files_concurrently_empty(self, mock_get_file_size):
        """Test process_files_concurrently with an empty file list."""
        mock_get_file_size.return_value = ("empty_file", 0)

        files = []
        nworkers = 3

        # Call function and check result
        result, total_bytes = process_files_concurrently(files, nworkers)

        assert result == {}  # Should return empty dictionary
        assert total_bytes == 50  # Only initial bytes_counter


class TestBuildSimplifiedRangeHeaderWithDataset:

    def test_tortilla_mixed_ranges(self):
        """Test build_simplified_range_header with mixed ranges."""
        df = pd.DataFrame(
            [
                {"tortilla:item_offset": 0, "tortilla:item_length": 100},
                {"tortilla:item_offset": 100, "tortilla:item_length": 100},
                {"tortilla:item_offset": 300, "tortilla:item_length": 50},
            ]
        )
        expected_header = {"Range": "bytes=0-199,bytes=300-349"}
        assert (
            build_simplified_range_header(df) == expected_header
        )  # Check if header matches
