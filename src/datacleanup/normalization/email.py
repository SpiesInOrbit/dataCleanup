"""Email normalization and validation."""

import re
from dataclasses import dataclass


# Basic email regex pattern
EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)


@dataclass
class EmailParts:
    """Parsed email components."""
    local: str
    domain: str
    subdomain: str | None
    tld: str


def normalize_email(email: str) -> str | None:
    """
    Normalize an email address.

    - Converts to lowercase
    - Strips whitespace
    - Removes dots from Gmail local parts (optional)
    - Validates format

    Args:
        email: Raw email string.

    Returns:
        Normalized email string, or None if invalid.
    """
    if not email or not email.strip():
        return None

    # Basic cleanup
    email = email.strip().lower()

    # Remove mailto: prefix if present
    if email.startswith("mailto:"):
        email = email[7:]

    # Validate format
    if not is_valid_email(email):
        return None

    return email


def normalize_email_strict(email: str) -> str | None:
    """
    Strictly normalize an email, including Gmail dot removal.

    Args:
        email: Raw email string.

    Returns:
        Normalized email string, or None if invalid.
    """
    normalized = normalize_email(email)
    if not normalized:
        return None

    parts = parse_email(normalized)
    if not parts:
        return None

    # Gmail ignores dots in local part
    if parts.domain in ("gmail.com", "googlemail.com"):
        local = parts.local.replace(".", "")
        # Also handle plus addressing
        if "+" in local:
            local = local.split("+")[0]
        return f"{local}@gmail.com"

    return normalized


def is_valid_email(email: str) -> bool:
    """
    Validate email format.

    Args:
        email: Email to validate.

    Returns:
        True if valid format, False otherwise.
    """
    if not email:
        return False

    email = email.strip().lower()
    return bool(EMAIL_PATTERN.match(email))


def parse_email(email: str) -> EmailParts | None:
    """
    Parse email into components.

    Args:
        email: Email address to parse.

    Returns:
        EmailParts object or None if invalid.
    """
    if not is_valid_email(email):
        return None

    email = email.strip().lower()
    local, domain = email.split("@")

    domain_parts = domain.split(".")
    tld = domain_parts[-1]

    subdomain = None
    if len(domain_parts) > 2:
        subdomain = ".".join(domain_parts[:-2])
        domain = ".".join(domain_parts[-2:])

    return EmailParts(
        local=local,
        domain=domain,
        subdomain=subdomain,
        tld=tld,
    )


def extract_domain(email: str) -> str | None:
    """
    Extract the domain from an email address.

    Args:
        email: Email address.

    Returns:
        Domain string or None if invalid.
    """
    parts = parse_email(email)
    return parts.domain if parts else None
