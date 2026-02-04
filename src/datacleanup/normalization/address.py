"""Address normalization utilities."""

import re
from dataclasses import dataclass


@dataclass
class ParsedAddress:
    """Parsed address components."""

    street: str
    city: str
    state: str
    postal_code: str
    country: str


# US state abbreviations
US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

# Street type abbreviations
STREET_TYPES = {
    "avenue": "Ave", "ave": "Ave",
    "boulevard": "Blvd", "blvd": "Blvd",
    "circle": "Cir", "cir": "Cir",
    "court": "Ct", "ct": "Ct",
    "drive": "Dr", "dr": "Dr",
    "expressway": "Expy", "expy": "Expy",
    "freeway": "Fwy", "fwy": "Fwy",
    "highway": "Hwy", "hwy": "Hwy",
    "lane": "Ln", "ln": "Ln",
    "parkway": "Pkwy", "pkwy": "Pkwy",
    "place": "Pl", "pl": "Pl",
    "road": "Rd", "rd": "Rd",
    "street": "St", "st": "St",
    "terrace": "Ter", "ter": "Ter",
    "trail": "Trl", "trl": "Trl",
    "way": "Way",
}

# Direction abbreviations
DIRECTIONS = {
    "north": "N", "n": "N",
    "south": "S", "s": "S",
    "east": "E", "e": "E",
    "west": "W", "w": "W",
    "northeast": "NE", "ne": "NE",
    "northwest": "NW", "nw": "NW",
    "southeast": "SE", "se": "SE",
    "southwest": "SW", "sw": "SW",
}


def normalize_address(address: str) -> str:
    """
    Normalize a street address.

    - Standardizes street type abbreviations
    - Standardizes directional abbreviations
    - Title cases street names

    Args:
        address: Raw address string.

    Returns:
        Normalized address string.
    """
    if not address or not address.strip():
        return ""

    address = address.strip()

    # Handle ALL CAPS
    if address.isupper():
        address = address.title()

    # Standardize street types
    for full, abbrev in STREET_TYPES.items():
        pattern = rf"\b{full}\b\.?"
        address = re.sub(pattern, abbrev, address, flags=re.IGNORECASE)

    # Standardize directions
    for full, abbrev in DIRECTIONS.items():
        pattern = rf"\b{full}\b\.?"
        address = re.sub(pattern, abbrev, address, flags=re.IGNORECASE)

    # Normalize apartment/unit/suite
    address = re.sub(r"\b(apt|apartment)\.?\s*#?\s*", "Apt ", address, flags=re.IGNORECASE)
    address = re.sub(r"\b(ste|suite)\.?\s*#?\s*", "Suite ", address, flags=re.IGNORECASE)
    address = re.sub(r"\b(unit)\.?\s*#?\s*", "Unit ", address, flags=re.IGNORECASE)

    # Clean up multiple spaces
    address = re.sub(r"\s+", " ", address)

    return address.strip()


def normalize_state(state: str, country: str = "US") -> str:
    """
    Normalize a state/province to standard abbreviation.

    Args:
        state: State name or abbreviation.
        country: Country code for context.

    Returns:
        Standardized state abbreviation.
    """
    if not state or not state.strip():
        return ""

    state = state.strip().lower()

    if country.upper() == "US":
        # Check if already an abbreviation
        if state.upper() in US_STATES.values():
            return state.upper()

        # Look up full name
        if state in US_STATES:
            return US_STATES[state]

    # Return as-is if not found
    return state.upper() if len(state) <= 3 else state.title()


def normalize_postal_code(postal_code: str, country: str = "US") -> str:
    """
    Normalize a postal/zip code.

    Args:
        postal_code: Raw postal code.
        country: Country code for format.

    Returns:
        Normalized postal code.
    """
    if not postal_code or not postal_code.strip():
        return ""

    postal_code = postal_code.strip()

    if country.upper() == "US":
        # Remove non-digits except hyphen
        digits = re.sub(r"[^\d-]", "", postal_code)

        # Format as 5 or 5+4
        if "-" in digits:
            parts = digits.split("-")
            if len(parts) == 2 and len(parts[0]) == 5 and len(parts[1]) == 4:
                return f"{parts[0]}-{parts[1]}"

        # Just digits
        digits = re.sub(r"\D", "", postal_code)
        if len(digits) == 9:
            return f"{digits[:5]}-{digits[5:]}"
        elif len(digits) >= 5:
            return digits[:5]

    elif country.upper() in ("CA", "CAN"):
        # Canadian postal code: A1A 1A1
        code = re.sub(r"\s", "", postal_code.upper())
        if len(code) == 6:
            return f"{code[:3]} {code[3:]}"

    elif country.upper() in ("GB", "UK"):
        # UK postcode - just uppercase
        return postal_code.upper()

    return postal_code


def normalize_country(country: str) -> str:
    """
    Normalize country name to standard form.

    Args:
        country: Country name or code.

    Returns:
        Normalized country name.
    """
    if not country or not country.strip():
        return ""

    country = country.strip().lower()

    # Common variations
    country_map = {
        "us": "United States",
        "usa": "United States",
        "u.s.": "United States",
        "u.s.a.": "United States",
        "united states of america": "United States",
        "uk": "United Kingdom",
        "gb": "United Kingdom",
        "great britain": "United Kingdom",
        "ca": "Canada",
        "can": "Canada",
    }

    if country in country_map:
        return country_map[country]

    return country.title()
