# Configuration Guide

This guide covers how to configure DataCleanup for your specific needs.

## Schema Configuration

### Default Schema

DataCleanup includes a default contact schema. Generate it with:

```bash
datacleanup init-schema schema.yaml
```

### Schema Structure

Schemas are defined in YAML format:

```yaml
name: contacts
version: "1.0"
description: Standard contact information schema

columns:
  first_name:
    type: text
    aliases:
      - firstname
      - fname
      - given_name
      - givenname
    required: false
    unique: false
    description: Contact's first/given name

  email:
    type: email
    aliases:
      - email_address
      - e_mail
      - mail
    required: false
    unique: true
    description: Primary email address
```

### Column Types

Supported column types:

| Type | Description | Validation |
|------|-------------|------------|
| `text` | General text | None |
| `email` | Email address | Format validation |
| `phone` | Phone number | Parsed with phonenumbers |
| `date` | Date value | Common date formats |
| `integer` | Whole number | Numeric validation |
| `float` | Decimal number | Numeric validation |
| `boolean` | True/false | Common boolean strings |
| `url` | Web URL | URL format validation |

### Custom Schema Example

```yaml
name: products
version: "1.0"
description: Product catalog schema

columns:
  sku:
    type: text
    aliases:
      - product_id
      - item_number
      - part_number
    required: true
    unique: true
    description: Stock keeping unit

  name:
    type: text
    aliases:
      - product_name
      - title
      - description
    required: true
    description: Product name

  price:
    type: float
    aliases:
      - unit_price
      - cost
      - msrp
    description: Product price

  category:
    type: text
    aliases:
      - product_category
      - type
      - department
    description: Product category
```

## Match Configuration

### Column Matching

Configure fuzzy matching thresholds:

```python
from datacleanup import ColumnMatcher

matcher = ColumnMatcher(
    schema_path="schema.yaml",
)

# Adjust threshold (0.0-1.0)
matches = matcher.match_all(columns, threshold=0.6)  # More lenient
matches = matcher.match_all(columns, threshold=0.9)  # Stricter
```

### Duplicate Detection

Configure which fields to use for duplicate detection:

```python
from datacleanup.matching.record_matcher import MatchConfig, RecordMatcher

config = MatchConfig(
    # Fields to compare with weights
    match_fields={
        "email": 1.0,      # Highest weight
        "phone": 0.8,
        "last_name": 0.6,
        "first_name": 0.5,
        "company": 0.4,
    },

    # Minimum score to be considered duplicate
    duplicate_threshold=0.8,

    # Fields used for blocking (must match on at least one)
    blocking_fields=["email", "phone", "last_name"],
)

matcher = RecordMatcher(df, config)
```

### Blocking Strategy

Blocking reduces the number of comparisons by only comparing records that share a blocking key:

- **email**: First 4 characters of local part
- **phone**: Last 4 digits
- **last_name**: First 3 characters
- **Other fields**: First 5 characters

## Merge Configuration

### Merge Strategies

Available strategies for resolving field conflicts:

| Strategy | Description |
|----------|-------------|
| `KEEP_FIRST` | Keep value from first record |
| `KEEP_LAST` | Keep value from last record |
| `KEEP_LONGEST` | Keep longest non-empty value |
| `KEEP_MOST_COMPLETE` | Keep from record with most fields |
| `CONCATENATE` | Join unique values with semicolon |
| `MANUAL` | Flag for manual resolution |

### Configuring Merge Rules

```python
from datacleanup.merge import MergeResolver, MergeStrategy

resolver = MergeResolver(
    df,
    default_strategy=MergeStrategy.KEEP_MOST_COMPLETE,
    field_strategies={
        "email": MergeStrategy.KEEP_FIRST,  # Primary email wins
        "phone": MergeStrategy.KEEP_LONGEST,  # Keep formatted number
        "notes": MergeStrategy.CONCATENATE,  # Combine all notes
    }
)
```

## Normalization Options

### Phone Numbers

```python
from datacleanup.normalization import normalize_phone

# Different output formats
phone = normalize_phone("555-123-4567", format_type="E164")
# -> "+15551234567"

phone = normalize_phone("555-123-4567", format_type="NATIONAL")
# -> "(555) 123-4567"

phone = normalize_phone("555-123-4567", format_type="INTERNATIONAL")
# -> "+1 555-123-4567"

# Different regions
phone = normalize_phone("020 7946 0958", default_region="GB")
# -> "+442079460958"
```

### Addresses

```python
from datacleanup.normalization.address import (
    normalize_address,
    normalize_state,
    normalize_postal_code,
)

# Standardize street types
address = normalize_address("123 Main Street Apartment 4B")
# -> "123 Main St Apt 4B"

# State abbreviations
state = normalize_state("California")
# -> "CA"

# Postal code formatting
zip_code = normalize_postal_code("123456789")
# -> "12345-6789"
```

## Environment Variables

DataCleanup supports configuration via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATACLEANUP_SCHEMA_PATH` | Default schema file path | None |
| `DATACLEANUP_LOG_LEVEL` | Logging verbosity | INFO |

## Programmatic Configuration

### Using Pydantic Settings

```python
from pydantic_settings import BaseSettings

class DataCleanupSettings(BaseSettings):
    schema_path: str = "schema.yaml"
    duplicate_threshold: float = 0.8
    match_threshold: float = 0.7
    default_region: str = "US"

    class Config:
        env_prefix = "DATACLEANUP_"
```

## Next Steps

- [API Reference](../api/index.md) - Detailed API documentation
- [Examples](../examples/index.md) - Real-world examples
