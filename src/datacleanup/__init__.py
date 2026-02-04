"""
DataCleanup - CSV ingestion utility with fuzzy matching and duplicate detection.

A tool for ingesting CSV files, matching columns via fuzzy logic,
detecting duplicates, and producing unified output for database import.
"""

__version__ = "0.1.0"
__author__ = "Your Name"

from datacleanup.ingestion.csv_reader import CSVReader
from datacleanup.ingestion.schema_detector import SchemaDetector
from datacleanup.matching.column_matcher import ColumnMatcher
from datacleanup.matching.record_matcher import RecordMatcher

__all__ = [
    "CSVReader",
    "SchemaDetector",
    "ColumnMatcher",
    "RecordMatcher",
]
