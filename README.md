# DataCleanup

A Python utility for ingesting CSV files, matching columns via fuzzy logic, detecting duplicates, and producing unified output for database import.

## Features

- **Fuzzy Column Matching**: Automatically maps source CSV columns to your target schema using string similarity algorithms
- **Duplicate Detection**: Identifies potential duplicate records using configurable weighted field matching
- **Data Normalization**: Standardizes phone numbers, emails, addresses, and names
- **Merge Resolution**: Intelligent merging of duplicate records with configurable strategies
- **CLI & API**: Use from command line or integrate into Python applications

## Installation

```bash
# Clone the repository
git clone https://github.com/SpiesInOrbit/datacleanup.git
cd datacleanup

# Install the package
pip install -e .

# With development dependencies
pip install -e ".[dev]"

# With documentation tools
pip install -e ".[docs]"
```

## Quick Start

### Command Line

```bash
# Analyze a CSV file
datacleanup analyze contacts.csv

# Match columns to canonical schema
datacleanup match-columns contacts.csv

# Find duplicate records
datacleanup find-duplicates contacts.csv

# Clean and deduplicate
datacleanup clean input.csv output.csv

# Generate default schema file
datacleanup init-schema schema.yaml
```

### Python API

```python
from datacleanup import CSVReader, ColumnMatcher, RecordMatcher
from datacleanup.merge import MergeResolver, MergeStrategy
from datacleanup.export import CSVWriter

# Read CSV with auto-detection
reader = CSVReader("contacts.csv")
df = reader.read()

# Match columns to canonical schema
matcher = ColumnMatcher()
mapping = matcher.get_mapping(list(df.columns))
df = df.rename(columns={k: v for k, v in mapping.items() if v})

# Find duplicates
record_matcher = RecordMatcher(df)
clusters = record_matcher.find_duplicates()

# Merge duplicates
resolver = MergeResolver(df, default_strategy=MergeStrategy.KEEP_MOST_COMPLETE)
cluster_indices = [c.record_indices for c in clusters]
df, results = resolver.bulk_merge(cluster_indices)

# Export cleaned data
writer = CSVWriter(df)
writer.write("cleaned_contacts.csv")
```

## Project Structure

```
datacleanup/
├── src/datacleanup/
│   ├── ingestion/        # CSV reading and schema detection
│   ├── matching/         # Column and record matching
│   ├── normalization/    # Data normalization (phone, email, name, address)
│   ├── merge/            # Duplicate merging strategies
│   ├── export/           # CSV and database export
│   ├── config/           # Schema configuration
│   └── cli.py            # Command-line interface
├── docs/                 # Documentation
│   ├── guides/           # User guides
│   ├── api/              # API reference
│   └── examples/         # Usage examples
├── tests/                # Test suite
└── data/sample/          # Sample data files
```

## Configuration

### Canonical Schema

Create a custom schema to define your target column structure:

```yaml
name: contacts
version: "1.0"
columns:
  first_name:
    type: text
    aliases: [firstname, fname, given_name]
  email:
    type: email
    aliases: [email_address, e_mail]
    unique: true
  phone:
    type: phone
    aliases: [phone_number, tel, mobile]
```

### Duplicate Detection

Configure matching rules:

```python
from datacleanup.matching.record_matcher import MatchConfig

config = MatchConfig(
    match_fields={
        "email": 1.0,      # Highest weight
        "phone": 0.8,
        "last_name": 0.6,
    },
    duplicate_threshold=0.8,
    blocking_fields=["email", "phone", "last_name"],
)
```

## Documentation

Full documentation available in the `docs/` directory:

- [Getting Started](docs/guides/getting-started.md)
- [Configuration Guide](docs/guides/configuration.md)
- [API Reference](docs/api/index.md)
- [Examples](docs/examples/index.md)

Build documentation locally:

```bash
pip install -e ".[docs]"
mkdocs serve -f docs/mkdocs.yml
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linter
ruff check src/

# Run type checker
mypy src/
```

## Requirements

- Python 3.10+
- pandas
- rapidfuzz
- phonenumbers
- pydantic
- typer
- rich
- pyyaml

## License

MIT License - see LICENSE file for details.
