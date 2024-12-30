import json
import os
import struct
import tempfile
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

from pytortilla.core import (compile_local, compile_online,
                             read_tortilla_metadata_local,
                             read_tortilla_metadata_online)


class TestReadTortillaMetadataLocal:
    # Test for reading a valid local tortilla metadata file
    def test_valid_file(self):
        # Prepare mock file content with magic bytes, offsets, file format, and footer
        mb = b"#y"
        fo = struct.pack("<Q", 100)
        fl = struct.pack("<Q", 20)
        df = b"tortilla-file-format".ljust(32, b"\x00")
        footer_content = json.dumps({"1": [10, 20]}).encode("utf-8")

        mock_file_content = mb + fo + fl + df + footer_content

        # Mock file opening and check the output of the function
        with patch(
            "builtins.open", mock_open(read_data=mock_file_content)
        ) as mock_file:
            result = read_tortilla_metadata_local("dummy.tortilla")

            # Strip padding from the result and verify it matches the expected output
            result["tortilla:file_format"] = result["tortilla:file_format"].str.strip(
                "\x00"
            )

            expected_df = pd.DataFrame(
                {
                    "id": ["1"],
                    "tortilla:item_offset": [10],
                    "tortilla:item_length": [20],
                    "tortilla:file_format": ["tortilla-file-format"],
                    "tortilla:mode": ["local"],
                }
            )

            pd.testing.assert_frame_equal(result, expected_df)
            mock_file.assert_called_once_with("dummy.tortilla", "rb")

    # Test for invalid magic bytes, expecting a ValueError
    def test_invalid_magic_bytes(self):
        # Simulate a file with invalid magic bytes and check for ValueError
        invalid_mb = b"XX"
        fo = struct.pack("<Q", 100)
        fl = struct.pack("<Q", 20)
        df = b"tortilla-file-format".ljust(32, b"\x00")
        footer_content = json.dumps({"1": [10, 20]}).encode("utf-8")

        mock_file_content = invalid_mb + fo + fl + df + footer_content

        with patch(
            "builtins.open", mock_open(read_data=mock_file_content)
        ), pytest.raises(ValueError):
            read_tortilla_metadata_local("dummy.tortilla")

    # Test for invalid footer content, expecting a JSONDecodeError
    def test_invalid_footer_content(self):
        # Simulate a file with an invalid footer and check for JSONDecodeError
        mb = b"#y"
        fo = struct.pack("<Q", 100)
        fl = struct.pack("<Q", 20)
        df = b"tortilla-file-format".ljust(32, b"\x00")
        invalid_footer_content = b"invalid-footer-json"

        mock_file_content = mb + fo + fl + df + invalid_footer_content

        with patch(
            "builtins.open", mock_open(read_data=mock_file_content)
        ), pytest.raises(json.JSONDecodeError):
            read_tortilla_metadata_local("dummy.tortilla")


class TestReadTortillaMetadataOnline:

    # Test when the footer is empty; it should raise a ValueError
    def test_read_tortilla_metadata_online_empty_footer(self, requests_mock):
        mock_url = "http://example.com/tortilla"
        footer_offset = 16
        footer_length = 24

        # Mocking response for the file's header and footer
        mock_bytes = (
            b"#y"
            + struct.pack("<Q", footer_offset)
            + struct.pack("<Q", footer_length)
            + b"TortillaFormat".ljust(32, b"\x00")  # Padded file format
        )
        requests_mock.get(mock_url, content=mock_bytes, status_code=200)

        # Mocking the footer content as empty
        requests_mock.get(
            mock_url,
            json={},
            headers={
                "Range": f"bytes={footer_offset}-{footer_offset + footer_length - 1}"
            },
        )

        # Expecting a ValueError when the footer is empty
        with pytest.raises(ValueError, match="You are not a tortilla 🫓"):
            read_tortilla_metadata_online(mock_url)

    # Test when there is a mismatch between footer offset and length; it should raise a ValueError
    def test_read_tortilla_metadata_online_footer_length_mismatch(self, requests_mock):
        mock_url = "http://example.com/tortilla"
        footer_offset = 16

        # Mocking file header with a footer length mismatch
        mock_bytes = (
            b"#y"
            + struct.pack("<Q", footer_offset)
            + struct.pack("<Q", 0)
            + b"TortillaFormat".ljust(32, b"\x00")  # Mismatch in footer length
        )
        requests_mock.get(mock_url, content=mock_bytes)

        # Simulating an incorrect footer range request
        requests_mock.get(
            mock_url,
            json={},
            headers={"Range": f"bytes={footer_offset}-{footer_offset - 1}"},
        )

        # Expecting a ValueError when footer length does not match
        with pytest.raises(ValueError, match="You are not a tortilla 🫓"):
            read_tortilla_metadata_online(mock_url)

    # Test when footer length is zero; it should raise a ValueError
    def test_read_tortilla_metadata_online_zero_footer_length(self, requests_mock):
        mock_url = "http://example.com/tortilla"
        footer_offset = 16
        footer_length = 0

        # Mocking file header with a footer length of zero
        mock_bytes = (
            b"#y"
            + struct.pack("<Q", footer_offset)
            + struct.pack("<Q", footer_length)
            + b"TortillaFormat".ljust(32, b"\x00")
        )
        requests_mock.get(mock_url, content=mock_bytes, status_code=200)

        # Mocking an empty footer
        requests_mock.get(
            mock_url,
            content=b"{}",
            headers={"Range": f"bytes={footer_offset}-{footer_offset - 1}"},
        )

        # Expecting a ValueError when footer length is zero
        with pytest.raises(ValueError, match="You are not a tortilla 🫓"):
            read_tortilla_metadata_online(mock_url)

    # Another test for footer length zero, similar to the previous case
    def test_read_tortilla_metadata_online_footer_length_zero(self, requests_mock):
        mock_url = "http://example.com/tortilla"
        footer_offset = 16
        footer_length = 0

        # Mocking file header with footer length zero
        mock_bytes = (
            b"#y"
            + struct.pack("<Q", footer_offset)
            + struct.pack("<Q", footer_length)
            + b"TortillaFormat".ljust(32, b"\x00")
        )
        requests_mock.get(mock_url, content=mock_bytes)

        # Mocking an empty footer with a zero-length range
        requests_mock.get(
            mock_url,
            json={},
            headers={"Range": f"bytes={footer_offset}-{footer_offset - 1}"},
        )

        # Expecting a ValueError when footer length is zero
        with pytest.raises(ValueError, match="You are not a tortilla 🫓"):
            read_tortilla_metadata_online(mock_url)


class TestCompileLocal:
    @pytest.fixture
    def sample_dataset(self):
        """Fixture providing a sample dataset for testing compile_local function."""
        return pd.DataFrame(
            {
                "tortilla:subfile": ["local_file1.tortilla", "local_file2.tortilla"],
                "tortilla:item_length": [100, 200],
                "tortilla:item_offset": [0, 100],
                "id": ["item1", "item2"],
                "tortilla:file_format": ["TORTILLA_v1", "TORTILLA_v1"],
            }
        )

    # Test when the target file for compilation doesn't exist, expecting a ValueError
    @patch("builtins.open", new_callable=mock_open)
    @patch("pytortilla.core.requests.get")
    def test_compile_local_no_file(self, mock_get, mock_open, sample_dataset):
        """Simulate file not found scenario and test if compile_local raises a ValueError."""
        mock_get.return_value.__enter__.return_value.content = (
            b""  # No content retrieved
        )
        mock_get.return_value.status_code = 404  # File not found response

        output_file = "output_file.tortilla"

        # Expecting a ValueError due to missing file
        with pytest.raises(ValueError):
            compile_local(
                sample_dataset, output_file, chunk_size=50, nworkers=2, quiet=False
            )

    # Test file writing without concurrency, focusing on sequential writing
    @patch("builtins.open", new_callable=MagicMock)
    @patch("pytortilla.core.mmap.mmap")
    def test_compile_local_writing_no_concurrency(self, mock_mmap, mock_open):
        """Test writing output without concurrency to validate behavior in single-threaded execution."""

        # Mock file handling and memory mapping (mmap) behavior
        mock_open.return_value.__enter__.return_value = MagicMock()
        mock_open.return_value.__enter__.return_value.truncate = MagicMock()

        mock_mm = MagicMock()  # Mock memory map methods
        mock_mm.write = MagicMock(return_value=None)
        mock_mm.flush = MagicMock(return_value=None)
        mock_mm.close = MagicMock(return_value=None)
        mock_mmap.return_value = mock_mm

        # Use the sample dataset
        sample_dataset = pd.DataFrame(
            {
                "tortilla:subfile": ["local_file1.tortilla", "local_file2.tortilla"],
                "tortilla:item_length": [100, 200],
                "tortilla:item_offset": [0, 100],
                "id": ["item1", "item2"],
                "tortilla:file_format": ["TORTILLA_v1", "TORTILLA_v1"],
            }
        )

        output_file = "output_file.tortilla"

        # Call the compile_local function with one worker to avoid concurrency
        compile_local(
            sample_dataset, output_file, chunk_size=50, nworkers=1, quiet=True
        )

        # Ensure the output file is opened and written correctly
        mock_open.assert_any_call(output_file, "wb")  # Check file opened for writing
        mock_open.return_value.__enter__.return_value.truncate.assert_called_once_with(
            391
        )  # Verify file size after writing
        mock_open.assert_any_call(
            output_file, "r+b"
        )  # Ensure file is reopened for reading/writing


class TestCompileOnline:

    @pytest.fixture
    def sample_dataset(self):
        """Provides a sample dataset with file URLs for testing."""
        data = {
            "tortilla:subfile": [
                "https://example.com/file",
                "https://example.com/file2",
                "https://example.com/file3",
            ],
            "tortilla:item_length": [5000, 3000, 2000],
            "id": ["item1", "item2", "item3"],
            "tortilla:file_format": ["TORTILLA_v1", "TORTILLA_v1", "TORTILLA_v1"],
            "tortilla:item_offset": [0, 5050, 8050],
        }
        return pd.DataFrame(data)

    @patch("requests.get")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.stat")
    @patch("builtins.open", new_callable=MagicMock)
    def test_compile_online_resume_download(
        self, mock_open, mock_stat, mock_exists, mock_get, sample_dataset
    ):
        # Simulates resuming a download when part of the file already exists

        mock_exists.return_value = (
            True  # Indicates that the file already partially exists
        )
        mock_stat.return_value.st_size = 5000  # Part of the file is already 5000 bytes

        # Simulates a server response with file chunks and total size
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"data1", b"data2"]
        mock_response.headers = {"content-length": "10000"}
        mock_get.return_value = mock_response

        # Calls the compile function
        result = compile_online(
            sample_dataset, "output_file.tortilla", chunk_size=1024, quiet=False
        )

        # Verifies that the correct byte range is requested to resume the download
        mock_get.assert_called_once_with(
            "https://example.com/file",
            headers={"Range": "bytes=0-4999,bytes=5050-10049"},
            stream=True,
            timeout=10,
        )

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.stat")
    @patch("builtins.open", new_callable=mock_open)
    @patch("requests.get")
    def test_download_fail(self, mock_get, mock_open, mock_stat, mock_exists):
        # Simulates a download failure and verifies that the error is handled correctly.

        mock_exists.return_value = False  # The file does not exist

        # Simulates a download request exception
        mock_get.side_effect = requests.exceptions.RequestException("Download error")

        sample_dataset = pd.DataFrame(
            {
                "tortilla:subfile": ["https://example.com/file"],
                "tortilla:item_length": [10000],
                "id": ["item1"],
                "tortilla:file_format": ["TORTILLA_v1"],
                "tortilla:item_offset": [0],
            }
        )

        result = compile_online(
            sample_dataset, "output_file.tortilla", chunk_size=1024, quiet=False
        )

        # Verifies that the result is None due to the download failure
        assert result is None

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.stat")
    def test_compile_online_no_download_when_complete(self, mock_stat, mock_exists):
        # Verifies that no download occurs if the file is already complete.

        mock_exists.return_value = True
        mock_stat.return_value.st_size = 10000  # The file is fully downloaded

        sample_dataset = pd.DataFrame(
            {
                "tortilla:subfile": ["https://example.com/file"],
                "tortilla:item_length": [5000],
                "id": ["item1"],
                "tortilla:file_format": ["TORTILLA_v1"],
                "tortilla:item_offset": [0],
            }
        )

        result = compile_online(
            sample_dataset, "output_file.tortilla", chunk_size=1024, quiet=True
        )

        # No action should be taken, as the file is already complete
        assert result is None

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.stat")
    @patch("requests.get")
    def test_compile_online_resume_download_message(
        self, mock_get, mock_stat, mock_exists
    ):
        # Verifies that an appropriate message is shown when resuming a partial download.

        mock_exists.return_value = True  # File exists
        mock_stat.return_value.st_size = (
            5050  # File partially downloaded up to 5050 bytes
        )

        # Simulates server response
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_response.headers = {"content-length": "10000"}
        mock_get.return_value = mock_response

        sample_dataset = pd.DataFrame(
            {
                "tortilla:subfile": ["https://example.com/file"],
                "tortilla:item_length": [10000],
                "id": ["item1"],
                "tortilla:file_format": ["TORTILLA_v1"],
                "tortilla:item_offset": [0],
            }
        )

        # Verifies that a message is printed when resuming the download
        with patch("builtins.print") as mock_print:
            compile_online(
                sample_dataset, "output_file.tortilla", chunk_size=1024, quiet=False
            )
            mock_print.assert_any_call(f"Resuming download from byte 5050 (0.00 GB)")

    @patch("requests.get")
    def test_compile_online_fallback_total_bytes(self, mock_get):
        # Verifies behavior when the content-length header is missing in the response.

        # Simulates response without content-length header
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_response.headers = {}
        mock_get.return_value = mock_response

        sample_dataset = pd.DataFrame(
            {
                "tortilla:subfile": ["https://example.com/file"],
                "tortilla:item_length": [5000],
                "id": ["item1"],
                "tortilla:file_format": ["TORTILLA_v1"],
                "tortilla:item_offset": [0],
            }
        )

        compile_online(
            sample_dataset, "output_file.tortilla", chunk_size=1024, quiet=True
        )

        # Verifies that the total byte size is calculated from the dataset
        total_bytes = sum(sample_dataset["tortilla:item_length"])
        assert mock_get.call_args[1]["headers"]["Range"] == f"bytes=0-{total_bytes - 1}"
