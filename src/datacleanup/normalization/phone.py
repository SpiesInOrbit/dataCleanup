"""Phone number normalization using phonenumbers library."""

import phonenumbers
from phonenumbers import NumberParseException


def normalize_phone(
    phone: str,
    default_region: str = "US",
    format_type: str = "E164",
) -> str | None:
    """
    Normalize a phone number to a standard format.

    Args:
        phone: Raw phone number string.
        default_region: Default region code if not specified in number.
        format_type: Output format - "E164", "INTERNATIONAL", "NATIONAL".

    Returns:
        Normalized phone number string, or None if invalid.
    """
    if not phone or not phone.strip():
        return None

    try:
        parsed = phonenumbers.parse(phone, default_region)

        if not phonenumbers.is_valid_number(parsed):
            return None

        format_map = {
            "E164": phonenumbers.PhoneNumberFormat.E164,
            "INTERNATIONAL": phonenumbers.PhoneNumberFormat.INTERNATIONAL,
            "NATIONAL": phonenumbers.PhoneNumberFormat.NATIONAL,
        }

        fmt = format_map.get(format_type, phonenumbers.PhoneNumberFormat.E164)
        return phonenumbers.format_number(parsed, fmt)

    except NumberParseException:
        return None


def extract_phone_parts(
    phone: str,
    default_region: str = "US",
) -> dict[str, str | None]:
    """
    Extract parts from a phone number.

    Args:
        phone: Raw phone number string.
        default_region: Default region code.

    Returns:
        Dictionary with country_code, national_number, and extension.
    """
    if not phone or not phone.strip():
        return {
            "country_code": None,
            "national_number": None,
            "extension": None,
        }

    try:
        parsed = phonenumbers.parse(phone, default_region)

        return {
            "country_code": str(parsed.country_code) if parsed.country_code else None,
            "national_number": str(parsed.national_number) if parsed.national_number else None,
            "extension": parsed.extension,
        }

    except NumberParseException:
        return {
            "country_code": None,
            "national_number": None,
            "extension": None,
        }


def is_valid_phone(phone: str, default_region: str = "US") -> bool:
    """
    Check if a phone number is valid.

    Args:
        phone: Phone number to validate.
        default_region: Default region code.

    Returns:
        True if valid, False otherwise.
    """
    if not phone or not phone.strip():
        return False

    try:
        parsed = phonenumbers.parse(phone, default_region)
        return phonenumbers.is_valid_number(parsed)
    except NumberParseException:
        return False
