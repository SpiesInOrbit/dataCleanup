"""CSV export functionality."""

from pathlib import Path
from typing import Any

import pandas as pd


class CSVWriter:
    """
    Writes cleaned data to CSV files.

    Supports column mapping, filtering, and various output formats.
    """

    def __init__(
        self,
        dataframe: pd.DataFrame,
        column_mapping: dict[str, str] | None = None,
    ) -> None:
        """
        Initialize the CSV writer.

        Args:
            dataframe: DataFrame to export.
            column_mapping: Optional mapping from source to output column names.
        """
        self.df = dataframe
        self.column_mapping = column_mapping

    def write(
        self,
        output_path: str | Path,
        columns: list[str] | None = None,
        include_index: bool = False,
        encoding: str = "utf-8",
        delimiter: str = ",",
    ) -> Path:
        """
        Write DataFrame to CSV file.

        Args:
            output_path: Path for output file.
            columns: Specific columns to include (None = all).
            include_index: Whether to include row index.
            encoding: Output file encoding.
            delimiter: Field delimiter.

        Returns:
            Path to written file.
        """
        output_path = Path(output_path)

        # Prepare DataFrame
        df = self._prepare_dataframe(columns)

        # Write to file
        df.to_csv(
            output_path,
            index=include_index,
            encoding=encoding,
            sep=delimiter,
        )

        return output_path

    def _prepare_dataframe(
        self,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Prepare DataFrame for export.

        Args:
            columns: Columns to include.

        Returns:
            Prepared DataFrame.
        """
        df = self.df.copy()

        # Apply column mapping
        if self.column_mapping:
            # Only rename columns that exist
            rename_map = {
                src: dst for src, dst in self.column_mapping.items()
                if src in df.columns
            }
            df = df.rename(columns=rename_map)

        # Filter columns
        if columns:
            available = [c for c in columns if c in df.columns]
            df = df[available]

        return df

    def write_chunks(
        self,
        output_dir: str | Path,
        chunk_size: int = 10000,
        prefix: str = "chunk",
    ) -> list[Path]:
        """
        Write DataFrame to multiple CSV files.

        Args:
            output_dir: Directory for output files.
            chunk_size: Rows per file.
            prefix: Filename prefix.

        Returns:
            List of written file paths.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        paths = []
        df = self._prepare_dataframe()

        for i, start in enumerate(range(0, len(df), chunk_size)):
            chunk = df.iloc[start:start + chunk_size]
            path = output_dir / f"{prefix}_{i:04d}.csv"
            chunk.to_csv(path, index=False)
            paths.append(path)

        return paths

    def to_string(
        self,
        columns: list[str] | None = None,
        max_rows: int | None = None,
    ) -> str:
        """
        Convert DataFrame to CSV string.

        Args:
            columns: Columns to include.
            max_rows: Maximum rows to include.

        Returns:
            CSV formatted string.
        """
        df = self._prepare_dataframe(columns)

        if max_rows:
            df = df.head(max_rows)

        return df.to_csv(index=False)

    def write_with_schema(
        self,
        output_path: str | Path,
        schema: dict[str, Any],
    ) -> Path:
        """
        Write CSV with columns ordered by schema.

        Args:
            output_path: Path for output file.
            schema: Schema dictionary with column definitions.

        Returns:
            Path to written file.
        """
        output_path = Path(output_path)
        df = self._prepare_dataframe()

        # Get column order from schema
        schema_columns = list(schema.get("columns", {}).keys())

        # Reorder columns: schema columns first, then any extras
        ordered_columns = [c for c in schema_columns if c in df.columns]
        extra_columns = [c for c in df.columns if c not in ordered_columns]
        final_order = ordered_columns + extra_columns

        df = df[final_order]
        df.to_csv(output_path, index=False)

        return output_path
