"""Tests for column matching functionality."""

import pytest

from datacleanup.matching.column_matcher import ColumnMatcher


class TestColumnMatcher:
    """Test suite for ColumnMatcher class."""

    def test_exact_match(self) -> None:
        """Test exact matching of canonical column names."""
        matcher = ColumnMatcher()
        match = matcher.match_column("email")

        assert match.canonical_column == "email"
        assert match.confidence == 1.0
        assert match.match_type == "exact"

    def test_alias_match(self) -> None:
        """Test matching via aliases."""
        matcher = ColumnMatcher()
        match = matcher.match_column("firstname")

        assert match.canonical_column == "first_name"
        assert match.confidence == 1.0
        assert match.match_type == "alias"

    def test_fuzzy_match(self) -> None:
        """Test fuzzy matching for similar column names."""
        matcher = ColumnMatcher()
        match = matcher.match_column("first_nam")  # Typo

        assert match.canonical_column == "first_name"
        assert match.confidence > 0.7
        assert match.match_type == "fuzzy"

    def test_no_match_below_threshold(self) -> None:
        """Test that low-confidence matches return None."""
        matcher = ColumnMatcher()
        match = matcher.match_column("xyz_random_column", threshold=0.9)

        assert match.canonical_column is None
        assert match.match_type == "none"

    def test_match_all(self) -> None:
        """Test matching multiple columns at once."""
        matcher = ColumnMatcher()
        columns = ["email", "firstname", "phone_number"]
        matches = matcher.match_all(columns)

        assert len(matches) == 3
        assert matches["email"].canonical_column == "email"
        assert matches["firstname"].canonical_column == "first_name"
        assert matches["phone_number"].canonical_column == "phone"

    def test_get_mapping(self) -> None:
        """Test getting simple column mapping."""
        matcher = ColumnMatcher()
        columns = ["email", "FirstName", "unknown_col"]
        mapping = matcher.get_mapping(columns, threshold=0.9)

        assert mapping["email"] == "email"
        assert mapping["FirstName"] == "first_name"
        assert mapping["unknown_col"] is None

    def test_alternatives_provided(self) -> None:
        """Test that alternatives are provided for fuzzy matches."""
        matcher = ColumnMatcher()
        match = matcher.match_column("name")

        # Should have alternatives since "name" is ambiguous
        assert match.canonical_column is not None or len(match.alternatives) > 0

    def test_custom_schema(self) -> None:
        """Test matching with custom schema."""
        custom_schema = {
            "columns": {
                "product_id": {
                    "type": "text",
                    "aliases": ["sku", "item_number"],
                },
                "product_name": {
                    "type": "text",
                    "aliases": ["name", "title"],
                },
            }
        }
        matcher = ColumnMatcher(canonical_schema=custom_schema)

        match = matcher.match_column("sku")
        assert match.canonical_column == "product_id"
        assert match.match_type == "alias"

    def test_get_unmatched(self) -> None:
        """Test getting list of unmatched columns."""
        matcher = ColumnMatcher()
        columns = ["email", "xyz_random", "abc_unknown"]
        unmatched = matcher.get_unmatched(columns, threshold=0.9)

        assert len(unmatched) == 2
        assert all(m.canonical_column is None for m in unmatched)
