# Getting Started

This guide walks you through the basic workflow of using DataCleanup to process CSV files.

## Prerequisites

- Python 3.10 or higher
- pip package manager

## Installation

### From Source

```bash
git clone https://github.com/yourusername/datacleanup.git
cd datacleanup
pip install -e .
```

### With Development Tools

```bash
pip install -e ".[dev]"
```

### With Documentation Tools

```bash
pip install -e ".[docs]"
```

## Basic Workflow

### Step 1: Analyze Your Data

Start by analyzing your CSV file to understand its structure:

```bash
datacleanup analyze contacts.csv
```

This shows:
- File encoding and delimiter
- Column names and detected types
- Fill rates and sample values

### Step 2: Review Column Mappings

See how your columns map to the canonical schema:

```bash
datacleanup match-columns contacts.csv
```

For a custom schema:

```bash
datacleanup match-columns contacts.csv --schema-path my_schema.yaml
```

### Step 3: Find Duplicates

Identify potential duplicate records:

```bash
datacleanup find-duplicates contacts.csv --threshold 0.8
```

The threshold (0.0-1.0) controls how strict the matching is.

### Step 4: Clean and Export

Process the file with all cleanup steps:

```bash
datacleanup clean input.csv output.csv
```

Options:
- `--merge-duplicates`: Merge detected duplicates (default: true)
- `--duplicate-threshold`: Confidence threshold for duplicates
- `--schema-path`: Custom schema file

## Using the Python API

### Reading CSV Files

```python
from datacleanup import CSVReader

# Auto-detect encoding and delimiter
reader = CSVReader("contacts.csv")
df = reader.read()

# Or specify explicitly
reader = CSVReader(
    "contacts.csv",
    encoding="utf-8",
    delimiter=","
)
```

### Analyzing Schema

```python
from datacleanup.ingestion import SchemaDetector

detector = SchemaDetector(df)
schemas = detector.detect_all()

for name, schema in schemas.items():
    print(f"{name}: {schema.detected_type.value} ({schema.fill_rate:.0%} filled)")
```

### Matching Columns

```python
from datacleanup import ColumnMatcher

matcher = ColumnMatcher()

# Match single column
match = matcher.match_column("FirstName")
print(f"{match.source_column} -> {match.canonical_column} ({match.confidence:.0%})")

# Match all columns
matches = matcher.match_all(df.columns, threshold=0.7)
```

### Finding Duplicates

```python
from datacleanup import RecordMatcher
from datacleanup.matching.record_matcher import MatchConfig

# Custom configuration
config = MatchConfig(
    match_fields={
        "email": 1.0,
        "phone": 0.8,
        "last_name": 0.6,
    },
    duplicate_threshold=0.8,
)

matcher = RecordMatcher(df, config)
clusters = matcher.find_duplicates()

for cluster in clusters:
    print(f"Cluster {cluster.cluster_id}: {len(cluster.record_indices)} records")
```

### Merging Duplicates

```python
from datacleanup.merge import MergeResolver, MergeStrategy

resolver = MergeResolver(
    df,
    default_strategy=MergeStrategy.KEEP_MOST_COMPLETE,
    field_strategies={
        "email": MergeStrategy.KEEP_FIRST,
    }
)

# Merge a single cluster
result = resolver.merge_records(cluster.record_indices)

# Bulk merge all clusters
cluster_indices = [c.record_indices for c in clusters]
merged_df, results = resolver.bulk_merge(cluster_indices)
```

### Normalizing Data

```python
from datacleanup.normalization import (
    normalize_phone,
    normalize_email,
    normalize_name,
    parse_full_name,
)

# Phone normalization
phone = normalize_phone("(555) 123-4567")  # -> "+15551234567"

# Email normalization
email = normalize_email("John.Doe@GMAIL.com")  # -> "johndoe@gmail.com"

# Name parsing
name = parse_full_name("Dr. John Michael Smith Jr.")
# -> ParsedName(first_name="John", middle_name="Michael",
#               last_name="Smith", prefix="Dr.", suffix="Jr.")
```

### Exporting Results

```python
from datacleanup.export import CSVWriter

writer = CSVWriter(df, column_mapping={
    "first_name": "FirstName",
    "last_name": "LastName",
})

# Write to file
writer.write("output.csv")

# Write with schema ordering
writer.write_with_schema("output.csv", schema.to_dict())
```

## Next Steps

- [Configuration Guide](configuration.md) - Customize schemas and settings
- [API Reference](../api/index.md) - Detailed API documentation
- [Examples](../examples/index.md) - More usage examples
