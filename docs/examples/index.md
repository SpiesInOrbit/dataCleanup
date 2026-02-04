# Examples

Practical examples for common DataCleanup use cases.

## Example 1: Basic CSV Cleaning

Clean a contact list from a spreadsheet export:

```python
from datacleanup import CSVReader, ColumnMatcher, RecordMatcher
from datacleanup.merge import MergeResolver, MergeStrategy
from datacleanup.export import CSVWriter

# Load the CSV
reader = CSVReader("raw_contacts.csv")
df = reader.read()

print(f"Loaded {len(df)} records")

# Match columns to standard schema
matcher = ColumnMatcher()
mapping = matcher.get_mapping(list(df.columns))

# Rename columns
rename_map = {src: dst for src, dst in mapping.items() if dst}
df = df.rename(columns=rename_map)

print(f"Mapped columns: {rename_map}")

# Find duplicates
record_matcher = RecordMatcher(df)
clusters = record_matcher.find_duplicates()

print(f"Found {len(clusters)} duplicate clusters")

# Merge duplicates
if clusters:
    resolver = MergeResolver(df, default_strategy=MergeStrategy.KEEP_MOST_COMPLETE)
    cluster_indices = [c.record_indices for c in clusters]
    df, _ = resolver.bulk_merge(cluster_indices)

print(f"Result: {len(df)} unique records")

# Export
writer = CSVWriter(df)
writer.write("cleaned_contacts.csv")
```

## Example 2: Multiple File Merge

Combine contacts from multiple sources:

```python
from datacleanup import CSVReader, ColumnMatcher
from datacleanup.matching.record_matcher import RecordMatcher, MatchConfig
from datacleanup.merge import MergeResolver, MergeStrategy
import pandas as pd

# Load multiple files
files = ["hubspot_export.csv", "mailchimp_export.csv", "manual_list.csv"]
dataframes = []

matcher = ColumnMatcher()

for file in files:
    reader = CSVReader(file)
    df = reader.read()

    # Standardize columns
    mapping = matcher.get_mapping(list(df.columns))
    rename_map = {src: dst for src, dst in mapping.items() if dst}
    df = df.rename(columns=rename_map)

    # Add source tracking
    df["_source"] = file

    dataframes.append(df)
    print(f"Loaded {len(df)} from {file}")

# Combine all
combined = pd.concat(dataframes, ignore_index=True)
print(f"Combined total: {len(combined)} records")

# Deduplicate across all sources
config = MatchConfig(
    match_fields={
        "email": 1.0,
        "phone": 0.8,
        "first_name": 0.5,
        "last_name": 0.6,
    },
    duplicate_threshold=0.85,
)

record_matcher = RecordMatcher(combined, config)
clusters = record_matcher.find_duplicates()

print(f"Found {len(clusters)} cross-source duplicates")

# Review a cluster
if clusters:
    cluster = clusters[0]
    print(f"\nCluster {cluster.cluster_id} (confidence: {cluster.confidence:.0%}):")
    print(combined.iloc[cluster.record_indices][["email", "first_name", "last_name", "_source"]])
```

## Example 3: Data Normalization Pipeline

Normalize all fields before export:

```python
from datacleanup import CSVReader
from datacleanup.normalization import (
    normalize_phone,
    normalize_email,
    normalize_name,
    parse_full_name,
)
from datacleanup.normalization.address import (
    normalize_address,
    normalize_state,
    normalize_postal_code,
)

reader = CSVReader("raw_data.csv")
df = reader.read()

# Normalize emails
if "email" in df.columns:
    df["email"] = df["email"].apply(
        lambda x: normalize_email(str(x)) if x else None
    )

# Normalize phones
if "phone" in df.columns:
    df["phone"] = df["phone"].apply(
        lambda x: normalize_phone(str(x), format_type="NATIONAL") if x else None
    )

# Parse full names into components
if "full_name" in df.columns and "first_name" not in df.columns:
    def parse_name(name):
        if not name:
            return None, None
        parsed = parse_full_name(str(name))
        return parsed.first_name, parsed.last_name

    df[["first_name", "last_name"]] = df["full_name"].apply(
        lambda x: pd.Series(parse_name(x))
    )

# Normalize addresses
if "address" in df.columns:
    df["address"] = df["address"].apply(
        lambda x: normalize_address(str(x)) if x else None
    )

if "state" in df.columns:
    df["state"] = df["state"].apply(
        lambda x: normalize_state(str(x)) if x else None
    )

if "postal_code" in df.columns:
    df["postal_code"] = df["postal_code"].apply(
        lambda x: normalize_postal_code(str(x)) if x else None
    )

# Show results
print(df.head())
df.to_csv("normalized_data.csv", index=False)
```

## Example 4: Custom Schema Matching

Create and use a custom schema for product data:

```python
from datacleanup.config.schema import CanonicalSchema, ColumnConfig
from datacleanup import ColumnMatcher

# Define product schema
schema = CanonicalSchema(
    name="products",
    version="1.0",
    columns={
        "sku": ColumnConfig(
            type="text",
            aliases=["product_id", "item_number", "part_number", "upc"],
            unique=True,
        ),
        "name": ColumnConfig(
            type="text",
            aliases=["product_name", "title", "item_name"],
        ),
        "description": ColumnConfig(
            type="text",
            aliases=["desc", "product_description", "details"],
        ),
        "price": ColumnConfig(
            type="float",
            aliases=["unit_price", "cost", "msrp", "retail_price"],
        ),
        "quantity": ColumnConfig(
            type="integer",
            aliases=["qty", "stock", "inventory", "stock_level"],
        ),
        "category": ColumnConfig(
            type="text",
            aliases=["product_category", "type", "department"],
        ),
    },
)

# Save for reuse
schema.save("product_schema.yaml")

# Use with matcher
matcher = ColumnMatcher(canonical_schema=schema.to_dict())

# Test with sample columns
sample_columns = ["Item Number", "Product Title", "Unit Price", "Stock Level"]
matches = matcher.match_all(sample_columns)

for col, match in matches.items():
    print(f"{col} -> {match.canonical_column} ({match.confidence:.0%})")
```

## Example 5: Database Import

Load cleaned data into SQLite:

```python
from datacleanup import CSVReader, ColumnMatcher, RecordMatcher
from datacleanup.merge import MergeResolver, MergeStrategy
from datacleanup.export import DatabaseLoader

# Clean the data first
reader = CSVReader("contacts.csv")
df = reader.read()

# ... (column matching, deduplication as in previous examples)

# Load into SQLite
with DatabaseLoader(sqlite_path="contacts.db") as db:
    # Check if table exists
    if db.table_exists("contacts"):
        print("Table exists, appending...")
        rows = db.load(df, "contacts", if_exists="append")
    else:
        print("Creating new table...")
        rows = db.load(df, "contacts", if_exists="replace")

    print(f"Loaded {rows} records")

    # Query to verify
    result = db.query("SELECT COUNT(*) as count FROM contacts")
    print(f"Total records in database: {result['count'].iloc[0]}")
```

## Example 6: Interactive Duplicate Review

Build a review workflow for uncertain duplicates:

```python
from datacleanup import CSVReader, RecordMatcher
from datacleanup.matching.record_matcher import MatchConfig
from datacleanup.merge import MergeResolver, MergeStrategy

reader = CSVReader("contacts.csv")
df = reader.read()

# Find duplicates with lower threshold to catch more potential matches
config = MatchConfig(duplicate_threshold=0.6)
matcher = RecordMatcher(df, config)
clusters = matcher.find_duplicates()

# Separate high-confidence from uncertain
high_confidence = [c for c in clusters if c.confidence >= 0.9]
needs_review = [c for c in clusters if c.confidence < 0.9]

print(f"Auto-merge: {len(high_confidence)} clusters")
print(f"Needs review: {len(needs_review)} clusters")

# Auto-merge high confidence
resolver = MergeResolver(df, default_strategy=MergeStrategy.KEEP_MOST_COMPLETE)

if high_confidence:
    cluster_indices = [c.record_indices for c in high_confidence]
    df, _ = resolver.bulk_merge(cluster_indices)

# Export uncertain for manual review
if needs_review:
    review_records = []
    for cluster in needs_review:
        for idx in cluster.record_indices:
            record = df.iloc[idx].to_dict()
            record["_cluster_id"] = cluster.cluster_id
            record["_confidence"] = cluster.confidence
            review_records.append(record)

    import pandas as pd
    review_df = pd.DataFrame(review_records)
    review_df.to_csv("needs_review.csv", index=False)
    print("Exported uncertain matches to needs_review.csv")
```

## Example 7: CLI Scripting

Chain CLI commands for automation:

```bash
#!/bin/bash

# Create schema if not exists
if [ ! -f "schema.yaml" ]; then
    datacleanup init-schema schema.yaml
fi

# Process each file in input directory
for file in input/*.csv; do
    filename=$(basename "$file" .csv)

    echo "Processing: $filename"

    # Analyze first
    datacleanup analyze "$file" > "reports/${filename}_analysis.txt"

    # Clean and deduplicate
    datacleanup clean "$file" "output/${filename}_cleaned.csv" \
        --schema-path schema.yaml \
        --duplicate-threshold 0.85

    echo "Done: $filename"
done

echo "All files processed!"
```
