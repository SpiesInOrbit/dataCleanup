"""Command-line interface for DataCleanup."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from datacleanup.config.schema import default_contact_schema
from datacleanup.ingestion.csv_reader import CSVReader
from datacleanup.ingestion.schema_detector import SchemaDetector
from datacleanup.matching.column_matcher import ColumnMatcher
from datacleanup.matching.record_matcher import MatchConfig, RecordMatcher
from datacleanup.merge.resolver import MergeResolver, MergeStrategy
from datacleanup.export.csv_writer import CSVWriter

app = typer.Typer(
    name="datacleanup",
    help="CSV ingestion utility with fuzzy matching and duplicate detection.",
    add_completion=False,
)
console = Console()


@app.command()
def analyze(
    file_path: Path = typer.Argument(..., help="Path to CSV file to analyze"),
    show_sample: bool = typer.Option(True, help="Show sample data"),
    sample_rows: int = typer.Option(5, help="Number of sample rows to show"),
) -> None:
    """Analyze a CSV file and show schema information."""
    console.print(f"\n[bold]Analyzing:[/bold] {file_path}\n")

    # Read file
    reader = CSVReader(file_path)
    df = reader.read()

    console.print(f"[green]Encoding:[/green] {reader.encoding}")
    console.print(f"[green]Delimiter:[/green] {repr(reader.delimiter)}")
    console.print(f"[green]Rows:[/green] {len(df)}")
    console.print(f"[green]Columns:[/green] {len(df.columns)}\n")

    # Detect schema
    detector = SchemaDetector(df)
    summary = detector.get_summary()

    # Display schema table
    table = Table(title="Column Schema")
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Fill Rate", justify="right")
    table.add_column("Unique", justify="right")
    table.add_column("Sample Values")

    for _, row in summary.iterrows():
        table.add_row(
            row["column"],
            row["type"],
            row["fill_rate"],
            str(row["unique_values"]),
            row["sample"][:50] + "..." if len(row["sample"]) > 50 else row["sample"],
        )

    console.print(table)

    if show_sample:
        console.print(f"\n[bold]Sample Data ({sample_rows} rows):[/bold]\n")
        console.print(df.head(sample_rows).to_string())


@app.command()
def match_columns(
    file_path: Path = typer.Argument(..., help="Path to CSV file"),
    schema_path: Optional[Path] = typer.Option(None, help="Path to schema YAML"),
    threshold: float = typer.Option(0.7, help="Match confidence threshold"),
) -> None:
    """Match CSV columns to canonical schema."""
    console.print(f"\n[bold]Matching columns for:[/bold] {file_path}\n")

    # Read file
    reader = CSVReader(file_path)
    headers = reader.get_headers()

    # Set up matcher
    if schema_path:
        matcher = ColumnMatcher(schema_path=schema_path)
    else:
        matcher = ColumnMatcher()

    matches = matcher.match_all(headers, threshold)

    # Display matches
    table = Table(title="Column Mapping")
    table.add_column("Source Column", style="cyan")
    table.add_column("Canonical Column", style="green")
    table.add_column("Confidence", justify="right")
    table.add_column("Match Type")
    table.add_column("Alternatives")

    for source, match in matches.items():
        conf_style = "green" if match.confidence >= 0.8 else "yellow" if match.confidence >= 0.5 else "red"
        alternatives = ", ".join(f"{alt}({score:.0%})" for alt, score in match.alternatives[:2])

        table.add_row(
            source,
            match.canonical_column or "[red]No match[/red]",
            f"[{conf_style}]{match.confidence:.0%}[/{conf_style}]",
            match.match_type,
            alternatives,
        )

    console.print(table)

    # Summary
    matched = sum(1 for m in matches.values() if m.canonical_column)
    console.print(f"\n[bold]Matched:[/bold] {matched}/{len(matches)} columns")


@app.command()
def find_duplicates(
    file_path: Path = typer.Argument(..., help="Path to CSV file"),
    threshold: float = typer.Option(0.8, help="Duplicate confidence threshold"),
    show_clusters: int = typer.Option(5, help="Number of clusters to show"),
) -> None:
    """Find duplicate records in a CSV file."""
    console.print(f"\n[bold]Finding duplicates in:[/bold] {file_path}\n")

    # Read file
    reader = CSVReader(file_path)
    df = reader.read()

    # Configure matcher
    config = MatchConfig(duplicate_threshold=threshold)
    matcher = RecordMatcher(df, config)

    clusters = matcher.find_duplicates()

    if not clusters:
        console.print("[green]No duplicates found![/green]")
        return

    console.print(f"[yellow]Found {len(clusters)} duplicate clusters[/yellow]\n")

    # Show top clusters
    for cluster in clusters[:show_clusters]:
        console.print(f"\n[bold]Cluster {cluster.cluster_id}[/bold] - "
                     f"Confidence: {cluster.confidence:.0%} - "
                     f"Records: {len(cluster.record_indices)}")

        records = df.iloc[cluster.record_indices]
        console.print(records.to_string())


@app.command()
def clean(
    input_path: Path = typer.Argument(..., help="Input CSV file"),
    output_path: Path = typer.Argument(..., help="Output CSV file"),
    schema_path: Optional[Path] = typer.Option(None, help="Schema YAML file"),
    merge_duplicates: bool = typer.Option(True, help="Merge duplicate records"),
    duplicate_threshold: float = typer.Option(0.8, help="Duplicate threshold"),
) -> None:
    """Clean and deduplicate a CSV file."""
    console.print(f"\n[bold]Cleaning:[/bold] {input_path}")
    console.print(f"[bold]Output:[/bold] {output_path}\n")

    # Read file
    reader = CSVReader(input_path)
    df = reader.read()
    console.print(f"[green]Loaded {len(df)} records[/green]")

    # Match columns
    matcher = ColumnMatcher(schema_path=schema_path) if schema_path else ColumnMatcher()
    column_mapping = matcher.get_mapping(list(df.columns))

    # Rename columns to canonical names
    rename_map = {src: dst for src, dst in column_mapping.items() if dst}
    df = df.rename(columns=rename_map)

    console.print(f"[green]Mapped {len(rename_map)} columns[/green]")

    if merge_duplicates:
        # Find and merge duplicates
        config = MatchConfig(duplicate_threshold=duplicate_threshold)
        record_matcher = RecordMatcher(df, config)
        clusters = record_matcher.find_duplicates()

        if clusters:
            console.print(f"[yellow]Found {len(clusters)} duplicate clusters[/yellow]")

            resolver = MergeResolver(df, default_strategy=MergeStrategy.KEEP_MOST_COMPLETE)
            cluster_indices = [c.record_indices for c in clusters]
            df, results = resolver.bulk_merge(cluster_indices)

            console.print(f"[green]Merged to {len(df)} unique records[/green]")
        else:
            console.print("[green]No duplicates found[/green]")

    # Write output
    writer = CSVWriter(df)
    writer.write(output_path)

    console.print(f"\n[bold green]Wrote {len(df)} records to {output_path}[/bold green]")


@app.command()
def init_schema(
    output_path: Path = typer.Argument(
        Path("schema.yaml"),
        help="Output path for schema file",
    ),
) -> None:
    """Generate a default contact schema file."""
    schema = default_contact_schema()
    schema.save(output_path)
    console.print(f"[green]Created schema file:[/green] {output_path}")


@app.callback()
def main() -> None:
    """DataCleanup - CSV ingestion with fuzzy matching and deduplication."""
    pass


if __name__ == "__main__":
    app()
