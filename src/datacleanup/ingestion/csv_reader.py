"""CSV file reader with encoding detection and flexible parsing."""

from pathlib import Path
from typing import Iterator

import pandas as pd


class CSVReader:
    """
    Reads CSV files with automatic encoding detection and flexible delimiter handling.

    Attributes:
        file_path: Path to the CSV file.
        encoding: Detected or specified file encoding.
        delimiter: Detected or specified field delimiter.
    """

    COMMON_ENCODINGS = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]
    COMMON_DELIMITERS = [",", ";", "\t", "|"]

    def __init__(
        self,
        file_path: str | Path,
        encoding: str | None = None,
        delimiter: str | None = None,
    ) -> None:
        """
        Initialize the CSV reader.

        Args:
            file_path: Path to the CSV file to read.
            encoding: File encoding. If None, will be auto-detected.
            delimiter: Field delimiter. If None, will be auto-detected.
        """
        self.file_path = Path(file_path)
        self._encoding = encoding
        self._delimiter = delimiter
        self._dataframe: pd.DataFrame | None = None

        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

    @property
    def encoding(self) -> str:
        """Get the file encoding, detecting if necessary."""
        if self._encoding is None:
            self._encoding = self._detect_encoding()
        return self._encoding

    @property
    def delimiter(self) -> str:
        """Get the field delimiter, detecting if necessary."""
        if self._delimiter is None:
            self._delimiter = self._detect_delimiter()
        return self._delimiter

    def _detect_encoding(self) -> str:
        """
        Detect file encoding by trying common encodings.

        Returns:
            The detected encoding string.
        """
        sample_size = 10000  # Read first 10KB for detection

        for encoding in self.COMMON_ENCODINGS:
            try:
                with open(self.file_path, "r", encoding=encoding) as f:
                    f.read(sample_size)
                return encoding
            except UnicodeDecodeError:
                continue

        # Fallback to utf-8 with error handling
        return "utf-8"

    def _detect_delimiter(self) -> str:
        """
        Detect field delimiter by analyzing first few lines.

        Returns:
            The detected delimiter character.
        """
        with open(self.file_path, "r", encoding=self.encoding) as f:
            sample_lines = [f.readline() for _ in range(5)]

        sample_text = "".join(sample_lines)

        # Count occurrences of each delimiter
        delimiter_counts = {d: sample_text.count(d) for d in self.COMMON_DELIMITERS}

        # Return the most frequent delimiter
        return max(delimiter_counts, key=delimiter_counts.get)  # type: ignore[arg-type]

    def read(self) -> pd.DataFrame:
        """
        Read the CSV file into a pandas DataFrame.

        Returns:
            DataFrame containing the CSV data.
        """
        if self._dataframe is None:
            self._dataframe = pd.read_csv(
                self.file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                dtype=str,  # Read all as strings initially
                keep_default_na=False,  # Don't convert empty strings to NaN
            )
            # Normalize column names
            self._dataframe.columns = self._normalize_headers(
                list(self._dataframe.columns)
            )
        return self._dataframe

    def read_chunks(self, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """
        Read the CSV file in chunks for memory-efficient processing.

        Args:
            chunk_size: Number of rows per chunk.

        Yields:
            DataFrame chunks.
        """
        for chunk in pd.read_csv(
            self.file_path,
            encoding=self.encoding,
            delimiter=self.delimiter,
            dtype=str,
            keep_default_na=False,
            chunksize=chunk_size,
        ):
            chunk.columns = self._normalize_headers(list(chunk.columns))
            yield chunk

    @staticmethod
    def _normalize_headers(headers: list[str]) -> list[str]:
        """
        Normalize column headers for consistent matching.

        Args:
            headers: List of original header names.

        Returns:
            List of normalized header names.
        """
        normalized = []
        for header in headers:
            # Strip whitespace, convert to lowercase
            clean = str(header).strip().lower()
            # Replace common separators with underscores
            for char in [" ", "-", "."]:
                clean = clean.replace(char, "_")
            # Remove consecutive underscores
            while "__" in clean:
                clean = clean.replace("__", "_")
            # Strip leading/trailing underscores
            clean = clean.strip("_")
            normalized.append(clean)
        return normalized

    def get_headers(self) -> list[str]:
        """
        Get the normalized column headers.

        Returns:
            List of column header names.
        """
        df = self.read()
        return list(df.columns)

    def get_row_count(self) -> int:
        """
        Get the number of data rows (excluding header).

        Returns:
            Number of rows in the CSV.
        """
        df = self.read()
        return len(df)

    def get_sample(self, n: int = 5) -> pd.DataFrame:
        """
        Get a sample of the first n rows.

        Args:
            n: Number of rows to return.

        Returns:
            DataFrame with sample rows.
        """
        df = self.read()
        return df.head(n)
