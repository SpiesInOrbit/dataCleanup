"""Fuzzy matching for CSV column headers to canonical schema."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from rapidfuzz import fuzz, process


@dataclass
class ColumnMatch:
    """Result of matching a source column to canonical schema."""

    source_column: str
    canonical_column: str | None
    confidence: float  # 0.0 to 1.0
    match_type: str  # "exact", "fuzzy", "alias", "none"
    alternatives: list[tuple[str, float]]  # Other potential matches


class ColumnMatcher:
    """
    Matches CSV column headers to a canonical schema using fuzzy matching.

    Supports exact matching, alias matching, and fuzzy string matching
    to map source columns to standardized database column names.
    """

    # Default confidence threshold for auto-matching
    DEFAULT_THRESHOLD = 0.7

    def __init__(
        self,
        canonical_schema: dict[str, Any] | None = None,
        schema_path: str | Path | None = None,
    ) -> None:
        """
        Initialize the column matcher.

        Args:
            canonical_schema: Dictionary defining the canonical schema.
            schema_path: Path to YAML file containing canonical schema.
        """
        if schema_path:
            self.schema = self._load_schema(Path(schema_path))
        elif canonical_schema:
            self.schema = canonical_schema
        else:
            self.schema = self._default_schema()

        # Build lookup structures
        self._canonical_columns = list(self.schema.get("columns", {}).keys())
        self._alias_map = self._build_alias_map()

    def _load_schema(self, path: Path) -> dict[str, Any]:
        """Load schema from YAML file."""
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def _default_schema(self) -> dict[str, Any]:
        """Return a default contact-focused schema."""
        return {
            "name": "contacts",
            "columns": {
                "first_name": {
                    "type": "text",
                    "aliases": ["firstname", "fname", "given_name", "givenname"],
                },
                "last_name": {
                    "type": "text",
                    "aliases": ["lastname", "lname", "surname", "family_name"],
                },
                "email": {
                    "type": "email",
                    "aliases": ["email_address", "e_mail", "mail"],
                },
                "phone": {
                    "type": "phone",
                    "aliases": ["phone_number", "telephone", "tel", "mobile", "cell"],
                },
                "company": {
                    "type": "text",
                    "aliases": ["organization", "org", "employer", "company_name"],
                },
                "title": {
                    "type": "text",
                    "aliases": ["job_title", "position", "role"],
                },
                "address": {
                    "type": "text",
                    "aliases": ["street", "street_address", "address_line_1"],
                },
                "city": {
                    "type": "text",
                    "aliases": ["town", "locality"],
                },
                "state": {
                    "type": "text",
                    "aliases": ["province", "region", "state_province"],
                },
                "postal_code": {
                    "type": "text",
                    "aliases": ["zip", "zip_code", "zipcode", "postcode"],
                },
                "country": {
                    "type": "text",
                    "aliases": ["nation", "country_code"],
                },
            },
        }

    def _build_alias_map(self) -> dict[str, str]:
        """Build a mapping from aliases to canonical column names."""
        alias_map = {}
        for canonical, config in self.schema.get("columns", {}).items():
            # Map canonical name to itself
            alias_map[canonical] = canonical
            # Map all aliases
            for alias in config.get("aliases", []):
                alias_map[alias.lower()] = canonical
        return alias_map

    def match_column(
        self,
        source_column: str,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> ColumnMatch:
        """
        Match a single source column to the canonical schema.

        Args:
            source_column: The source column name to match.
            threshold: Minimum confidence for a match.

        Returns:
            ColumnMatch with match results.
        """
        normalized = source_column.lower().strip()

        # Check exact match with canonical columns
        if normalized in self._canonical_columns:
            return ColumnMatch(
                source_column=source_column,
                canonical_column=normalized,
                confidence=1.0,
                match_type="exact",
                alternatives=[],
            )

        # Check alias match
        if normalized in self._alias_map:
            return ColumnMatch(
                source_column=source_column,
                canonical_column=self._alias_map[normalized],
                confidence=1.0,
                match_type="alias",
                alternatives=[],
            )

        # Fuzzy match against canonical columns and aliases
        all_targets = list(self._alias_map.keys())
        matches = process.extract(
            normalized,
            all_targets,
            scorer=fuzz.token_sort_ratio,
            limit=5,
        )

        if matches:
            best_match, best_score, _ = matches[0]
            confidence = best_score / 100.0

            # Map alias back to canonical name
            canonical = self._alias_map.get(best_match, best_match)

            # Build alternatives (excluding the best match)
            alternatives = []
            seen_canonical = {canonical}
            for match, score, _ in matches[1:]:
                alt_canonical = self._alias_map.get(match, match)
                if alt_canonical not in seen_canonical:
                    alternatives.append((alt_canonical, score / 100.0))
                    seen_canonical.add(alt_canonical)

            if confidence >= threshold:
                return ColumnMatch(
                    source_column=source_column,
                    canonical_column=canonical,
                    confidence=confidence,
                    match_type="fuzzy",
                    alternatives=alternatives[:3],
                )
            else:
                return ColumnMatch(
                    source_column=source_column,
                    canonical_column=None,
                    confidence=confidence,
                    match_type="none",
                    alternatives=[(canonical, confidence)] + alternatives[:2],
                )

        return ColumnMatch(
            source_column=source_column,
            canonical_column=None,
            confidence=0.0,
            match_type="none",
            alternatives=[],
        )

    def match_all(
        self,
        source_columns: list[str],
        threshold: float = DEFAULT_THRESHOLD,
    ) -> dict[str, ColumnMatch]:
        """
        Match all source columns to the canonical schema.

        Args:
            source_columns: List of source column names.
            threshold: Minimum confidence for auto-matching.

        Returns:
            Dictionary mapping source columns to their matches.
        """
        return {col: self.match_column(col, threshold) for col in source_columns}

    def get_mapping(
        self,
        source_columns: list[str],
        threshold: float = DEFAULT_THRESHOLD,
    ) -> dict[str, str | None]:
        """
        Get a simple mapping from source to canonical columns.

        Args:
            source_columns: List of source column names.
            threshold: Minimum confidence for auto-matching.

        Returns:
            Dictionary mapping source columns to canonical names (or None).
        """
        matches = self.match_all(source_columns, threshold)
        return {col: match.canonical_column for col, match in matches.items()}

    def get_unmatched(
        self,
        source_columns: list[str],
        threshold: float = DEFAULT_THRESHOLD,
    ) -> list[ColumnMatch]:
        """
        Get columns that couldn't be matched above the threshold.

        Args:
            source_columns: List of source column names.
            threshold: Minimum confidence for matching.

        Returns:
            List of ColumnMatch objects for unmatched columns.
        """
        matches = self.match_all(source_columns, threshold)
        return [m for m in matches.values() if m.canonical_column is None]
