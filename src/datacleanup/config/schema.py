"""Canonical schema configuration."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class ColumnConfig(BaseModel):
    """Configuration for a single column."""

    type: str = "text"
    aliases: list[str] = Field(default_factory=list)
    required: bool = False
    unique: bool = False
    description: str = ""


class CanonicalSchema(BaseModel):
    """Canonical schema definition."""

    name: str
    version: str = "1.0"
    description: str = ""
    columns: dict[str, ColumnConfig] = Field(default_factory=dict)

    def get_column_names(self) -> list[str]:
        """Get list of canonical column names."""
        return list(self.columns.keys())

    def get_aliases(self, column: str) -> list[str]:
        """Get aliases for a column."""
        if column in self.columns:
            return self.columns[column].aliases
        return []

    def get_all_aliases(self) -> dict[str, str]:
        """Get mapping of all aliases to canonical names."""
        alias_map = {}
        for name, config in self.columns.items():
            alias_map[name] = name
            for alias in config.aliases:
                alias_map[alias.lower()] = name
        return alias_map

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "columns": {
                name: {
                    "type": config.type,
                    "aliases": config.aliases,
                    "required": config.required,
                    "unique": config.unique,
                    "description": config.description,
                }
                for name, config in self.columns.items()
            }
        }

    def save(self, path: str | Path) -> None:
        """Save schema to YAML file."""
        path = Path(path)
        with open(path, "w") as f:
            yaml.safe_dump(self.to_dict(), f, default_flow_style=False, sort_keys=False)


def load_schema(path: str | Path) -> CanonicalSchema:
    """
    Load schema from YAML file.

    Args:
        path: Path to YAML schema file.

    Returns:
        CanonicalSchema object.
    """
    path = Path(path)

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    # Convert column dictionaries to ColumnConfig objects
    columns = {}
    for name, config in data.get("columns", {}).items():
        if isinstance(config, dict):
            columns[name] = ColumnConfig(**config)
        else:
            columns[name] = ColumnConfig()

    return CanonicalSchema(
        name=data.get("name", "unknown"),
        version=data.get("version", "1.0"),
        description=data.get("description", ""),
        columns=columns,
    )


def default_contact_schema() -> CanonicalSchema:
    """Get default contact schema."""
    return CanonicalSchema(
        name="contacts",
        version="1.0",
        description="Standard contact information schema",
        columns={
            "first_name": ColumnConfig(
                type="text",
                aliases=["firstname", "fname", "given_name", "givenname"],
                description="Contact's first/given name",
            ),
            "last_name": ColumnConfig(
                type="text",
                aliases=["lastname", "lname", "surname", "family_name"],
                description="Contact's last/family name",
            ),
            "email": ColumnConfig(
                type="email",
                aliases=["email_address", "e_mail", "mail"],
                unique=True,
                description="Primary email address",
            ),
            "phone": ColumnConfig(
                type="phone",
                aliases=["phone_number", "telephone", "tel", "mobile", "cell"],
                description="Primary phone number",
            ),
            "company": ColumnConfig(
                type="text",
                aliases=["organization", "org", "employer", "company_name"],
                description="Company or organization name",
            ),
            "title": ColumnConfig(
                type="text",
                aliases=["job_title", "position", "role"],
                description="Job title or position",
            ),
            "address": ColumnConfig(
                type="text",
                aliases=["street", "street_address", "address_line_1"],
                description="Street address",
            ),
            "city": ColumnConfig(
                type="text",
                aliases=["town", "locality"],
                description="City name",
            ),
            "state": ColumnConfig(
                type="text",
                aliases=["province", "region", "state_province"],
                description="State or province",
            ),
            "postal_code": ColumnConfig(
                type="text",
                aliases=["zip", "zip_code", "zipcode", "postcode"],
                description="Postal or ZIP code",
            ),
            "country": ColumnConfig(
                type="text",
                aliases=["nation", "country_code"],
                description="Country name",
            ),
        },
    )
