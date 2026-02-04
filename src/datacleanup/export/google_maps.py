"""
Google Maps export functionality for address data.

This module provides functionality to export cleaned address data
to formats compatible with Google My Maps for creating map pins.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List


class GoogleMapsExporter:
    """
    Exports address data to Google My Maps compatible CSV format.

    Google My Maps requires at minimum:
    - A name/title column
    - An address column (or latitude/longitude)

    This exporter will create a CSV with:
    - Name: Combination of first_name, last_name, and/or company
    - Address: Full formatted address
    - Description: Additional contact information (phone, email, etc.)
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the Google Maps exporter.

        Args:
            df: DataFrame containing cleaned contact data with address fields
        """
        self.df = df.copy()

    def _format_name(self, row: pd.Series) -> str:
        """
        Create a name for the map pin from available data.

        Priority order:
        1. first_name + last_name
        2. company
        3. email
        4. "Unknown"

        Args:
            row: DataFrame row

        Returns:
            Formatted name string
        """
        parts = []

        # Try first_name and last_name
        first = row.get('first_name', '').strip() if pd.notna(row.get('first_name')) else ''
        last = row.get('last_name', '').strip() if pd.notna(row.get('last_name')) else ''

        if first or last:
            name_parts = [p for p in [first, last] if p]
            parts.append(' '.join(name_parts))

        # Add company if available
        company = row.get('company', '').strip() if pd.notna(row.get('company')) else ''
        if company:
            if parts:
                parts.append(f"({company})")
            else:
                parts.append(company)

        # If still no name, try email
        if not parts:
            email = row.get('email', '').strip() if pd.notna(row.get('email')) else ''
            if email:
                parts.append(email)

        # Last resort
        if not parts:
            parts.append("Unknown Contact")

        return ' '.join(parts)

    def _format_address(self, row: pd.Series) -> str:
        """
        Create a full address string from component fields.

        Args:
            row: DataFrame row

        Returns:
            Formatted address string suitable for Google Maps geocoding
        """
        components = []

        # Street address
        address = row.get('address', '').strip() if pd.notna(row.get('address')) else ''
        if address:
            components.append(address)

        # Address line 2 (apartment, suite, etc.)
        address_2 = row.get('address_2', '').strip() if pd.notna(row.get('address_2')) else ''
        if address_2:
            components.append(address_2)

        # City, State ZIP
        city = row.get('city', '').strip() if pd.notna(row.get('city')) else ''
        state = row.get('state', '').strip() if pd.notna(row.get('state')) else ''
        postal_code = row.get('postal_code', '').strip() if pd.notna(row.get('postal_code')) else ''

        city_state_zip = []
        if city:
            city_state_zip.append(city)
        if state:
            city_state_zip.append(state)
        if postal_code:
            city_state_zip.append(postal_code)

        if city_state_zip:
            components.append(', '.join(city_state_zip))

        # Country
        country = row.get('country', '').strip() if pd.notna(row.get('country')) else ''
        if country:
            components.append(country)

        return ', '.join(components) if components else ''

    def _format_description(self, row: pd.Series) -> str:
        """
        Create a description with contact details.

        Args:
            row: DataFrame row

        Returns:
            Formatted description string with contact information
        """
        parts = []

        # Title
        title = row.get('title', '').strip() if pd.notna(row.get('title')) else ''
        if title:
            parts.append(f"Title: {title}")

        # Phone
        phone = row.get('phone', '').strip() if pd.notna(row.get('phone')) else ''
        if phone:
            parts.append(f"Phone: {phone}")

        # Email
        email = row.get('email', '').strip() if pd.notna(row.get('email')) else ''
        if email:
            parts.append(f"Email: {email}")

        # Website
        website = row.get('website', '').strip() if pd.notna(row.get('website')) else ''
        if website:
            parts.append(f"Website: {website}")

        # Notes
        notes = row.get('notes', '').strip() if pd.notna(row.get('notes')) else ''
        if notes:
            parts.append(f"Notes: {notes}")

        return '\n'.join(parts)

    def export(
        self,
        output_path: str | Path,
        include_description: bool = True,
        additional_columns: Optional[List[str]] = None
    ) -> Path:
        """
        Export data to Google My Maps compatible CSV format.

        Args:
            output_path: Path where the CSV file should be written
            include_description: Whether to include a description column with contact details
            additional_columns: Additional columns from the original data to include

        Returns:
            Path object pointing to the created file

        Raises:
            ValueError: If no valid addresses are found in the data
        """
        output_path = Path(output_path)

        # Create the export DataFrame
        export_df = pd.DataFrame()

        # Generate Name column
        export_df['Name'] = self.df.apply(self._format_name, axis=1)

        # Generate Address column
        export_df['Address'] = self.df.apply(self._format_address, axis=1)

        # Filter out rows without addresses
        has_address = export_df['Address'].str.strip() != ''
        if not has_address.any():
            raise ValueError(
                "No valid addresses found in the data. "
                "Ensure your data has address, city, state, or postal_code columns."
            )

        export_df = export_df[has_address].copy()

        # Add description if requested
        if include_description:
            export_df['Description'] = self.df[has_address].apply(
                self._format_description, axis=1
            )

        # Add any additional columns requested
        if additional_columns:
            for col in additional_columns:
                if col in self.df.columns:
                    export_df[col] = self.df.loc[has_address, col]

        # Write to CSV
        export_df.to_csv(output_path, index=False, encoding='utf-8')

        return output_path

    def preview(self, max_rows: int = 5) -> str:
        """
        Generate a preview of what the Google Maps export will look like.

        Args:
            max_rows: Maximum number of rows to include in preview

        Returns:
            String representation of the export preview
        """
        preview_df = pd.DataFrame()
        preview_df['Name'] = self.df.head(max_rows).apply(self._format_name, axis=1)
        preview_df['Address'] = self.df.head(max_rows).apply(self._format_address, axis=1)
        preview_df['Description'] = self.df.head(max_rows).apply(self._format_description, axis=1)

        return preview_df.to_string()
