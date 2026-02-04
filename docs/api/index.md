# API Reference

Complete API documentation for DataCleanup.

## Core Modules

### Ingestion

#### CSVReader

```python
from datacleanup import CSVReader
```

Read CSV files with automatic encoding and delimiter detection.

**Constructor:**
- `file_path: str | Path` - Path to CSV file
- `encoding: str | None` - File encoding (auto-detected if None)
- `delimiter: str | None` - Field delimiter (auto-detected if None)

**Methods:**
- `read() -> pd.DataFrame` - Read entire file
- `read_chunks(chunk_size: int) -> Iterator[pd.DataFrame]` - Read in chunks
- `get_headers() -> list[str]` - Get column names
- `get_row_count() -> int` - Get number of rows
- `get_sample(n: int) -> pd.DataFrame` - Get first n rows

---

#### SchemaDetector

```python
from datacleanup.ingestion import SchemaDetector
```

Detect column types and statistics.

**Constructor:**
- `dataframe: pd.DataFrame` - DataFrame to analyze

**Methods:**
- `detect_all() -> dict[str, ColumnSchema]` - Detect all columns
- `get_summary() -> pd.DataFrame` - Get summary table

**ColumnSchema Fields:**
- `name: str` - Column name
- `detected_type: ColumnType` - Detected data type
- `null_count: int` - Number of empty values
- `unique_count: int` - Number of unique values
- `sample_values: list[str]` - Sample values
- `fill_rate: float` - Percentage of non-empty values

---

### Matching

#### ColumnMatcher

```python
from datacleanup import ColumnMatcher
```

Match source columns to canonical schema.

**Constructor:**
- `canonical_schema: dict | None` - Schema dictionary
- `schema_path: str | Path | None` - Path to YAML schema

**Methods:**
- `match_column(source: str, threshold: float) -> ColumnMatch` - Match single column
- `match_all(columns: list[str], threshold: float) -> dict[str, ColumnMatch]` - Match all
- `get_mapping(columns: list[str], threshold: float) -> dict[str, str | None]` - Simple mapping
- `get_unmatched(columns: list[str], threshold: float) -> list[ColumnMatch]` - Unmatched columns

**ColumnMatch Fields:**
- `source_column: str` - Original column name
- `canonical_column: str | None` - Matched canonical name
- `confidence: float` - Match confidence (0.0-1.0)
- `match_type: str` - "exact", "fuzzy", "alias", or "none"
- `alternatives: list[tuple[str, float]]` - Other potential matches

---

#### RecordMatcher

```python
from datacleanup import RecordMatcher
from datacleanup.matching.record_matcher import MatchConfig
```

Find duplicate records.

**MatchConfig Fields:**
- `match_fields: dict[str, float]` - Fields with weights
- `duplicate_threshold: float` - Minimum score for duplicates
- `blocking_fields: list[str]` - Fields for blocking

**Constructor:**
- `dataframe: pd.DataFrame` - DataFrame to analyze
- `config: MatchConfig | None` - Configuration

**Methods:**
- `find_duplicates() -> list[DuplicateCluster]` - Find all duplicates
- `get_duplicate_summary() -> pd.DataFrame` - Summary table
- `get_cluster_records(cluster_id: int) -> pd.DataFrame` - Records in cluster

**DuplicateCluster Fields:**
- `cluster_id: int` - Cluster identifier
- `record_indices: list[int]` - Row indices in cluster
- `confidence: float` - Average similarity
- `field_similarities: dict[str, float]` - Per-field scores

---

### Normalization

#### Phone

```python
from datacleanup.normalization import normalize_phone
```

**Functions:**
- `normalize_phone(phone: str, default_region: str, format_type: str) -> str | None`
- `is_valid_phone(phone: str, default_region: str) -> bool`
- `extract_phone_parts(phone: str, default_region: str) -> dict`

---

#### Email

```python
from datacleanup.normalization import normalize_email
```

**Functions:**
- `normalize_email(email: str) -> str | None`
- `normalize_email_strict(email: str) -> str | None` - Gmail dot removal
- `is_valid_email(email: str) -> bool`
- `parse_email(email: str) -> EmailParts | None`
- `extract_domain(email: str) -> str | None`

---

#### Name

```python
from datacleanup.normalization import normalize_name, parse_full_name
```

**Functions:**
- `normalize_name(name: str) -> str`
- `parse_full_name(full_name: str) -> ParsedName`
- `combine_name(parsed: ParsedName, include_prefix: bool) -> str`

**ParsedName Fields:**
- `first_name: str`
- `last_name: str`
- `middle_name: str | None`
- `prefix: str | None`
- `suffix: str | None`

---

#### Address

```python
from datacleanup.normalization import normalize_address
```

**Functions:**
- `normalize_address(address: str) -> str`
- `normalize_state(state: str, country: str) -> str`
- `normalize_postal_code(postal_code: str, country: str) -> str`
- `normalize_country(country: str) -> str`

---

### Merge

#### MergeResolver

```python
from datacleanup.merge import MergeResolver, MergeStrategy
```

Resolve duplicate records.

**MergeStrategy Enum:**
- `KEEP_FIRST` - First record's value
- `KEEP_LAST` - Last record's value
- `KEEP_LONGEST` - Longest value
- `KEEP_MOST_COMPLETE` - From most complete record
- `CONCATENATE` - Join unique values
- `MANUAL` - Require manual resolution

**Constructor:**
- `dataframe: pd.DataFrame` - DataFrame with records
- `default_strategy: MergeStrategy` - Default merge strategy
- `field_strategies: dict[str, MergeStrategy] | None` - Per-field strategies

**Methods:**
- `merge_records(indices: list[int]) -> MergeResult` - Merge specific records
- `merge_cluster(indices: list[int], preview: bool) -> MergeResult | pd.DataFrame`
- `bulk_merge(clusters: list[list[int]]) -> tuple[pd.DataFrame, list[MergeResult]]`

---

### Export

#### CSVWriter

```python
from datacleanup.export import CSVWriter
```

Write cleaned data to CSV.

**Constructor:**
- `dataframe: pd.DataFrame` - Data to export
- `column_mapping: dict[str, str] | None` - Column name mapping

**Methods:**
- `write(path, columns, include_index, encoding, delimiter) -> Path`
- `write_chunks(output_dir, chunk_size, prefix) -> list[Path]`
- `to_string(columns, max_rows) -> str`
- `write_with_schema(path, schema) -> Path`

---

#### DatabaseLoader

```python
from datacleanup.export import DatabaseLoader
```

Load data into databases.

**Constructor:**
- `connection_string: str | None` - Database URL
- `sqlite_path: str | Path | None` - SQLite file path

**Methods:**
- `load(df, table_name, if_exists, column_mapping) -> int`
- `execute(sql, params) -> cursor`
- `query(sql, params) -> pd.DataFrame`
- `table_exists(table_name) -> bool`
- `get_table_columns(table_name) -> list[str]`
- `close() -> None`

---

### Configuration

#### CanonicalSchema

```python
from datacleanup.config import CanonicalSchema, load_schema
```

**Functions:**
- `load_schema(path: str | Path) -> CanonicalSchema`
- `default_contact_schema() -> CanonicalSchema`

**CanonicalSchema Methods:**
- `get_column_names() -> list[str]`
- `get_aliases(column: str) -> list[str]`
- `get_all_aliases() -> dict[str, str]`
- `to_dict() -> dict`
- `save(path: str | Path) -> None`
