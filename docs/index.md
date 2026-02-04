# DataCleanup

**CSV ingestion utility with fuzzy matching, duplicate detection, and contact merging.**

DataCleanup is a Python library and CLI tool designed to simplify the process of importing CSV data into databases. It handles the common pain points of data integration:

- **Fuzzy Column Matching**: Automatically maps source CSV columns to your target schema, even when column names don't match exactly
- **Duplicate Detection**: Identifies potential duplicate records using configurable matching rules
- **Data Normalization**: Standardizes phone numbers, emails, addresses, and names
- **Merge Suggestions**: Provides intelligent suggestions for merging duplicate records

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/datacleanup.git
cd datacleanup

# Install with pip
pip install -e .

# Or with development dependencies
pip install -e ".[dev]"
```

### Basic Usage

```bash
# Analyze a CSV file
datacleanup analyze contacts.csv

# Match columns to schema
datacleanup match-columns contacts.csv

# Find duplicates
datacleanup find-duplicates contacts.csv

# Clean and deduplicate
datacleanup clean input.csv output.csv
```

### Python API

```python
from datacleanup import CSVReader, ColumnMatcher, RecordMatcher

# Read CSV
reader = CSVReader("contacts.csv")
df = reader.read()

# Match columns to canonical schema
matcher = ColumnMatcher()
matches = matcher.match_all(df.columns)

# Find duplicates
record_matcher = RecordMatcher(df)
duplicates = record_matcher.find_duplicates()
```

## Features

### Column Matching

DataCleanup uses fuzzy string matching to map source columns to your target schema:

| Source Column | Matched To | Confidence |
|---------------|------------|------------|
| FirstName | first_name | 100% |
| E-Mail Address | email | 92% |
| Phone Number | phone | 95% |
| Zip | postal_code | 100% |

### Duplicate Detection

The duplicate detection system uses blocking and weighted field matching:

1. **Blocking**: Groups potential duplicates by common attributes (email domain, phone suffix, etc.)
2. **Scoring**: Calculates similarity across multiple fields with configurable weights
3. **Clustering**: Groups related duplicates together

### Data Normalization

Built-in normalizers for common data types:

- **Phone**: Converts to E.164 format using libphonenumber
- **Email**: Lowercase, validation, Gmail dot-removal
- **Names**: Title case, prefix/suffix parsing
- **Addresses**: Street type abbreviations, state codes

## Documentation

- [User Guide](guides/getting-started.md) - Complete walkthrough
- [API Reference](api/index.md) - Detailed API documentation
- [Examples](examples/index.md) - Usage examples
- [Configuration](guides/configuration.md) - Schema and settings

## License

MIT License - see LICENSE file for details.
