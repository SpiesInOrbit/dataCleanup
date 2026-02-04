"""Tests for CSV reader functionality."""

import tempfile
from pathlib import Path

import pytest

from datacleanup.ingestion.csv_reader import CSVReader


class TestCSVReader:
    """Test suite for CSVReader class."""

    def test_read_simple_csv(self, tmp_path: Path) -> None:
        """Test reading a simple CSV file."""
        csv_content = "name,email,phone\nJohn,john@example.com,555-1234\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        df = reader.read()

        assert len(df) == 1
        assert list(df.columns) == ["name", "email", "phone"]
        assert df.iloc[0]["name"] == "John"

    def test_header_normalization(self, tmp_path: Path) -> None:
        """Test that headers are normalized correctly."""
        csv_content = "First Name,E-Mail Address,Phone Number\nJohn,john@test.com,555\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        headers = reader.get_headers()

        assert headers == ["first_name", "e_mail_address", "phone_number"]

    def test_delimiter_detection_semicolon(self, tmp_path: Path) -> None:
        """Test semicolon delimiter detection."""
        csv_content = "name;email;phone\nJohn;john@test.com;555\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        assert reader.delimiter == ";"

    def test_delimiter_detection_tab(self, tmp_path: Path) -> None:
        """Test tab delimiter detection."""
        csv_content = "name\temail\tphone\nJohn\tjohn@test.com\t555\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        assert reader.delimiter == "\t"

    def test_file_not_found(self) -> None:
        """Test that FileNotFoundError is raised for missing files."""
        with pytest.raises(FileNotFoundError):
            CSVReader("/nonexistent/path/file.csv")

    def test_get_row_count(self, tmp_path: Path) -> None:
        """Test row counting."""
        csv_content = "name\nAlice\nBob\nCharlie\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        assert reader.get_row_count() == 3

    def test_get_sample(self, tmp_path: Path) -> None:
        """Test getting sample rows."""
        csv_content = "name\nA\nB\nC\nD\nE\nF\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(csv_file)
        sample = reader.get_sample(3)

        assert len(sample) == 3
        assert list(sample["name"]) == ["A", "B", "C"]
