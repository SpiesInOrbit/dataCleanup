"""Ingestion module for reading and parsing CSV files."""

from datacleanup.ingestion.csv_reader import CSVReader
from datacleanup.ingestion.schema_detector import SchemaDetector

__all__ = ["CSVReader", "SchemaDetector"]
