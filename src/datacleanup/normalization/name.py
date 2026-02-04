"""Name parsing and normalization."""

import re
from dataclasses import dataclass


@dataclass
class ParsedName:
    """Parsed name components."""

    first_name: str
    last_name: str
    middle_name: str | None = None
    prefix: str | None = None  # Mr., Dr., etc.
    suffix: str | None = None  # Jr., III, PhD, etc.


# Common name prefixes
PREFIXES = {
    "mr", "mr.", "mrs", "mrs.", "ms", "ms.", "miss", "dr", "dr.",
    "prof", "prof.", "rev", "rev.", "hon", "hon.", "sir", "dame",
}

# Common name suffixes
SUFFIXES = {
    "jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v",
    "phd", "ph.d", "ph.d.", "md", "m.d", "m.d.",
    "esq", "esq.", "cpa", "dds", "dvm",
}


def normalize_name(name: str) -> str:
    """
    Normalize a name string.

    - Strips whitespace
    - Title cases the name
    - Handles ALL CAPS or all lowercase

    Args:
        name: Raw name string.

    Returns:
        Normalized name string.
    """
    if not name or not name.strip():
        return ""

    name = name.strip()

    # Handle ALL CAPS or all lowercase
    if name.isupper() or name.islower():
        name = name.title()

    # Fix common issues with title case
    # e.g., "Mcdonald" -> "McDonald"
    name = _fix_name_casing(name)

    return name


def _fix_name_casing(name: str) -> str:
    """Fix common name casing issues."""
    # Handle Mc/Mac prefixes
    patterns = [
        (r"\bMc([a-z])", lambda m: f"Mc{m.group(1).upper()}"),
        (r"\bMac([a-z])", lambda m: f"Mac{m.group(1).upper()}"),
        (r"\bO'([a-z])", lambda m: f"O'{m.group(1).upper()}"),
    ]

    for pattern, replacement in patterns:
        name = re.sub(pattern, replacement, name)

    return name


def parse_full_name(full_name: str) -> ParsedName:
    """
    Parse a full name into components.

    Handles formats like:
    - "John Smith"
    - "John Michael Smith"
    - "Dr. John Smith Jr."
    - "Smith, John"

    Args:
        full_name: Full name string.

    Returns:
        ParsedName object with parsed components.
    """
    if not full_name or not full_name.strip():
        return ParsedName(first_name="", last_name="")

    name = full_name.strip()

    # Check for "Last, First" format
    if "," in name:
        return _parse_last_first(name)

    parts = name.split()

    # Extract prefix
    prefix = None
    if parts and parts[0].lower().rstrip(".") in {p.rstrip(".") for p in PREFIXES}:
        prefix = parts.pop(0)

    # Extract suffix
    suffix = None
    if parts and parts[-1].lower().rstrip(".") in {s.rstrip(".") for s in SUFFIXES}:
        suffix = parts.pop()

    # Parse remaining parts
    if len(parts) == 0:
        return ParsedName(first_name="", last_name="", prefix=prefix, suffix=suffix)
    elif len(parts) == 1:
        return ParsedName(
            first_name=normalize_name(parts[0]),
            last_name="",
            prefix=prefix,
            suffix=suffix,
        )
    elif len(parts) == 2:
        return ParsedName(
            first_name=normalize_name(parts[0]),
            last_name=normalize_name(parts[1]),
            prefix=prefix,
            suffix=suffix,
        )
    else:
        # Multiple middle names - treat all but first and last as middle
        return ParsedName(
            first_name=normalize_name(parts[0]),
            middle_name=normalize_name(" ".join(parts[1:-1])),
            last_name=normalize_name(parts[-1]),
            prefix=prefix,
            suffix=suffix,
        )


def _parse_last_first(name: str) -> ParsedName:
    """Parse name in 'Last, First Middle' format."""
    parts = [p.strip() for p in name.split(",", 1)]

    if len(parts) != 2:
        return ParsedName(first_name=name, last_name="")

    last_name = parts[0]
    first_parts = parts[1].split()

    # Check for suffix in last name part
    suffix = None
    last_parts = last_name.split()
    if last_parts and last_parts[-1].lower().rstrip(".") in {s.rstrip(".") for s in SUFFIXES}:
        suffix = last_parts.pop()
        last_name = " ".join(last_parts)

    if len(first_parts) == 0:
        return ParsedName(
            first_name="",
            last_name=normalize_name(last_name),
            suffix=suffix,
        )
    elif len(first_parts) == 1:
        return ParsedName(
            first_name=normalize_name(first_parts[0]),
            last_name=normalize_name(last_name),
            suffix=suffix,
        )
    else:
        return ParsedName(
            first_name=normalize_name(first_parts[0]),
            middle_name=normalize_name(" ".join(first_parts[1:])),
            last_name=normalize_name(last_name),
            suffix=suffix,
        )


def combine_name(parsed: ParsedName, include_prefix: bool = False) -> str:
    """
    Combine parsed name back into a string.

    Args:
        parsed: ParsedName object.
        include_prefix: Whether to include prefix (Mr., Dr., etc.)

    Returns:
        Combined name string.
    """
    parts = []

    if include_prefix and parsed.prefix:
        parts.append(parsed.prefix)

    if parsed.first_name:
        parts.append(parsed.first_name)

    if parsed.middle_name:
        parts.append(parsed.middle_name)

    if parsed.last_name:
        parts.append(parsed.last_name)

    if parsed.suffix:
        parts.append(parsed.suffix)

    return " ".join(parts)
