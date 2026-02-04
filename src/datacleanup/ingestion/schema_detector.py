"""Schema detection for CSV columns."""

import re
from dataclasses import dataclass
from enum import Enum

import pandas as pd


class ColumnType(Enum):
    """Detected column data types."""

    TEXT = "text"
    INTEGER = "integer"
    FLOAT = "float"
    EMAIL = "email"
    PHONE = "phone"
    DATE = "date"
    BOOLEAN = "boolean"
    URL = "url"
    EMPTY = "empty"


@dataclass
class ColumnSchema:
    """Schema information for a single column."""

    name: str
    detected_type: ColumnType
    null_count: int
    unique_count: int
    sample_values: list[str]
    fill_rate: float  # Percentage of non-empty values


class SchemaDetector:
    """
    Detects column types and schema from CSV data.

    Analyzes column contents to determine likely data types
    and provides statistics about data quality.
    """

    # Regex patterns for type detection
    EMAIL_PATTERN = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    PHONE_PATTERN = re.compile(r"^[\d\s\-\.\(\)\+]+$")
    URL_PATTERN = re.compile(r"^https?://[\w\.-]+")
    DATE_PATTERNS = [
        re.compile(r"^\d{4}-\d{2}-\d{2}$"),  # ISO format
        re.compile(r"^\d{2}/\d{2}/\d{4}$"),  # US format
        re.compile(r"^\d{2}-\d{2}-\d{4}$"),  # EU format
        re.compile(r"^\d{2}\.\d{2}\.\d{4}$"),  # Dot format
    ]

    def __init__(self, dataframe: pd.DataFrame) -> None:
        """
        Initialize the schema detector.

        Args:
            dataframe: DataFrame to analyze.
        """
        self.df = dataframe
        self._schemas: dict[str, ColumnSchema] | None = None

    def detect_all(self) -> dict[str, ColumnSchema]:
        """
        Detect schema for all columns.

        Returns:
            Dictionary mapping column names to their schemas.
        """
        if self._schemas is None:
            self._schemas = {}
            for column in self.df.columns:
                self._schemas[column] = self._detect_column(column)
        return self._schemas

    def _detect_column(self, column: str) -> ColumnSchema:
        """
        Detect schema for a single column.

        Args:
            column: Column name to analyze.

        Returns:
            ColumnSchema with detected information.
        """
        series = self.df[column].astype(str)

        # Calculate statistics
        total_count = len(series)
        non_empty = series[series.str.strip() != ""]
        null_count = total_count - len(non_empty)
        unique_count = non_empty.nunique()
        fill_rate = len(non_empty) / total_count if total_count > 0 else 0.0

        # Get sample values
        sample_values = non_empty.head(5).tolist()

        # Detect type
        detected_type = self._detect_type(non_empty)

        return ColumnSchema(
            name=column,
            detected_type=detected_type,
            null_count=null_count,
            unique_count=unique_count,
            sample_values=sample_values,
            fill_rate=fill_rate,
        )

    def _detect_type(self, series: pd.Series) -> ColumnType:  # type: ignore[type-arg]
        """
        Detect the most likely type for a column's values.

        Args:
            series: Series of non-empty values to analyze.

        Returns:
            Detected ColumnType.
        """
        if len(series) == 0:
            return ColumnType.EMPTY

        # Sample for efficiency on large datasets
        sample = series.head(100)

        # Check specific patterns first
        type_scores: dict[ColumnType, float] = {t: 0.0 for t in ColumnType}

        for value in sample:
            value = str(value).strip()

            if self.EMAIL_PATTERN.match(value):
                type_scores[ColumnType.EMAIL] += 1
            elif self._is_phone(value):
                type_scores[ColumnType.PHONE] += 1
            elif self.URL_PATTERN.match(value):
                type_scores[ColumnType.URL] += 1
            elif self._is_date(value):
                type_scores[ColumnType.DATE] += 1
            elif self._is_boolean(value):
                type_scores[ColumnType.BOOLEAN] += 1
            elif self._is_integer(value):
                type_scores[ColumnType.INTEGER] += 1
            elif self._is_float(value):
                type_scores[ColumnType.FLOAT] += 1
            else:
                type_scores[ColumnType.TEXT] += 1

        # Return type with highest score (minimum 50% match)
        sample_size = len(sample)
        for col_type, score in sorted(
            type_scores.items(), key=lambda x: x[1], reverse=True
        ):
            if score / sample_size >= 0.5:
                return col_type

        return ColumnType.TEXT

    def _is_phone(self, value: str) -> bool:
        """Check if value looks like a phone number."""
        if not self.PHONE_PATTERN.match(value):
            return False
        # Must have at least 7 digits
        digits = re.sub(r"\D", "", value)
        return 7 <= len(digits) <= 15

    def _is_date(self, value: str) -> bool:
        """Check if value matches common date patterns."""
        return any(pattern.match(value) for pattern in self.DATE_PATTERNS)

    def _is_boolean(self, value: str) -> bool:
        """Check if value is a boolean-like value."""
        return value.lower() in {
            "true", "false", "yes", "no", "y", "n", "1", "0",
            "on", "off", "enabled", "disabled"
        }

    def _is_integer(self, value: str) -> bool:
        """Check if value is an integer."""
        try:
            int(value)
            return "." not in value
        except ValueError:
            return False

    def _is_float(self, value: str) -> bool:
        """Check if value is a float."""
        try:
            float(value)
            return True
        except ValueError:
            return False

    def get_summary(self) -> pd.DataFrame:
        """
        Get a summary DataFrame of all column schemas.

        Returns:
            DataFrame with schema information for each column.
        """
        schemas = self.detect_all()

        rows = []
        for name, schema in schemas.items():
            rows.append({
                "column": name,
                "type": schema.detected_type.value,
                "fill_rate": f"{schema.fill_rate:.1%}",
                "unique_values": schema.unique_count,
                "null_count": schema.null_count,
                "sample": ", ".join(schema.sample_values[:3]),
            })

        return pd.DataFrame(rows)
