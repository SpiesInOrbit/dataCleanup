"""Tests for data normalization functions."""

import pytest

from datacleanup.normalization.email import (
    normalize_email,
    normalize_email_strict,
    is_valid_email,
    parse_email,
)
from datacleanup.normalization.name import (
    normalize_name,
    parse_full_name,
    combine_name,
)
from datacleanup.normalization.phone import (
    normalize_phone,
    is_valid_phone,
)
from datacleanup.normalization.address import (
    normalize_address,
    normalize_state,
    normalize_postal_code,
)


class TestEmailNormalization:
    """Test suite for email normalization."""

    def test_lowercase_email(self) -> None:
        """Test email is lowercased."""
        assert normalize_email("John.Doe@Example.COM") == "john.doe@example.com"

    def test_strip_whitespace(self) -> None:
        """Test whitespace is stripped."""
        assert normalize_email("  test@example.com  ") == "test@example.com"

    def test_invalid_email_returns_none(self) -> None:
        """Test invalid email returns None."""
        assert normalize_email("not-an-email") is None
        assert normalize_email("missing@domain") is None

    def test_empty_email_returns_none(self) -> None:
        """Test empty string returns None."""
        assert normalize_email("") is None
        assert normalize_email("   ") is None

    def test_gmail_dot_removal(self) -> None:
        """Test Gmail dot removal in strict mode."""
        assert normalize_email_strict("john.doe@gmail.com") == "johndoe@gmail.com"

    def test_valid_email_check(self) -> None:
        """Test email validation."""
        assert is_valid_email("test@example.com") is True
        assert is_valid_email("invalid") is False

    def test_parse_email(self) -> None:
        """Test email parsing."""
        parts = parse_email("user@example.com")
        assert parts is not None
        assert parts.local == "user"
        assert parts.domain == "example.com"


class TestNameNormalization:
    """Test suite for name normalization."""

    def test_title_case(self) -> None:
        """Test name is title cased."""
        assert normalize_name("JOHN DOE") == "John Doe"
        assert normalize_name("jane smith") == "Jane Smith"

    def test_mcname_handling(self) -> None:
        """Test Mc/Mac name handling."""
        assert normalize_name("MCDONALD") == "McDonald"

    def test_parse_simple_name(self) -> None:
        """Test parsing simple two-part name."""
        parsed = parse_full_name("John Smith")
        assert parsed.first_name == "John"
        assert parsed.last_name == "Smith"

    def test_parse_name_with_middle(self) -> None:
        """Test parsing name with middle name."""
        parsed = parse_full_name("John Michael Smith")
        assert parsed.first_name == "John"
        assert parsed.middle_name == "Michael"
        assert parsed.last_name == "Smith"

    def test_parse_name_with_prefix(self) -> None:
        """Test parsing name with prefix."""
        parsed = parse_full_name("Dr. John Smith")
        assert parsed.prefix == "Dr."
        assert parsed.first_name == "John"
        assert parsed.last_name == "Smith"

    def test_parse_name_with_suffix(self) -> None:
        """Test parsing name with suffix."""
        parsed = parse_full_name("John Smith Jr.")
        assert parsed.first_name == "John"
        assert parsed.last_name == "Smith"
        assert parsed.suffix == "Jr."

    def test_parse_last_first_format(self) -> None:
        """Test parsing 'Last, First' format."""
        parsed = parse_full_name("Smith, John")
        assert parsed.first_name == "John"
        assert parsed.last_name == "Smith"

    def test_combine_name(self) -> None:
        """Test combining parsed name back to string."""
        parsed = parse_full_name("Dr. John Michael Smith Jr.")
        combined = combine_name(parsed, include_prefix=True)
        assert combined == "Dr. John Michael Smith Jr."


class TestPhoneNormalization:
    """Test suite for phone normalization."""

    def test_us_phone_e164(self) -> None:
        """Test US phone to E.164 format."""
        result = normalize_phone("(555) 123-4567")
        assert result == "+15551234567"

    def test_us_phone_national(self) -> None:
        """Test US phone to national format."""
        result = normalize_phone("5551234567", format_type="NATIONAL")
        assert result == "(555) 123-4567"

    def test_invalid_phone_returns_none(self) -> None:
        """Test invalid phone returns None."""
        assert normalize_phone("123") is None
        assert normalize_phone("not-a-phone") is None

    def test_empty_phone_returns_none(self) -> None:
        """Test empty phone returns None."""
        assert normalize_phone("") is None
        assert normalize_phone("   ") is None

    def test_is_valid_phone(self) -> None:
        """Test phone validation."""
        assert is_valid_phone("(555) 123-4567") is True
        assert is_valid_phone("123") is False


class TestAddressNormalization:
    """Test suite for address normalization."""

    def test_street_type_abbreviation(self) -> None:
        """Test street type abbreviation."""
        assert "St" in normalize_address("123 Main Street")
        assert "Ave" in normalize_address("456 Oak Avenue")

    def test_direction_abbreviation(self) -> None:
        """Test direction abbreviation."""
        result = normalize_address("123 North Main St")
        assert "N" in result

    def test_apartment_normalization(self) -> None:
        """Test apartment format normalization."""
        result = normalize_address("123 Main St Apartment 4B")
        assert "Apt 4B" in result

    def test_state_abbreviation(self) -> None:
        """Test state abbreviation."""
        assert normalize_state("California") == "CA"
        assert normalize_state("New York") == "NY"
        assert normalize_state("TX") == "TX"

    def test_postal_code_formatting(self) -> None:
        """Test postal code formatting."""
        assert normalize_postal_code("123456789") == "12345-6789"
        assert normalize_postal_code("12345") == "12345"
