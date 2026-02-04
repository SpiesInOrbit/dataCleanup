"""Normalization module for standardizing data values."""

from datacleanup.normalization.phone import normalize_phone
from datacleanup.normalization.email import normalize_email
from datacleanup.normalization.name import normalize_name, parse_full_name
from datacleanup.normalization.address import normalize_address

__all__ = [
    "normalize_phone",
    "normalize_email",
    "normalize_name",
    "parse_full_name",
    "normalize_address",
]
