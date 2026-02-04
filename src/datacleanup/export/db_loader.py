"""Database loading functionality."""

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd


class DatabaseLoader:
    """
    Loads cleaned data into databases.

    Supports SQLite and provides hooks for other databases.
    Note: For production use with PostgreSQL/MySQL, install
    appropriate drivers (psycopg2, mysqlclient).
    """

    def __init__(
        self,
        connection_string: str | None = None,
        sqlite_path: str | Path | None = None,
    ) -> None:
        """
        Initialize the database loader.

        Args:
            connection_string: Database connection URL.
            sqlite_path: Path to SQLite database file.
        """
        self.connection_string = connection_string
        self.sqlite_path = Path(sqlite_path) if sqlite_path else None
        self._connection: Any = None

    def _get_connection(self) -> Any:
        """Get or create database connection."""
        if self._connection is not None:
            return self._connection

        if self.sqlite_path:
            import sqlite3
            self._connection = sqlite3.connect(self.sqlite_path)
            return self._connection

        if self.connection_string:
            parsed = urlparse(self.connection_string)

            if parsed.scheme == "sqlite":
                import sqlite3
                db_path = parsed.path.lstrip("/")
                self._connection = sqlite3.connect(db_path)
            elif parsed.scheme in ("postgresql", "postgres"):
                try:
                    import psycopg2
                    self._connection = psycopg2.connect(self.connection_string)
                except ImportError:
                    raise ImportError(
                        "psycopg2 is required for PostgreSQL. "
                        "Install with: pip install psycopg2-binary"
                    )
            elif parsed.scheme == "mysql":
                try:
                    import MySQLdb
                    self._connection = MySQLdb.connect(
                        host=parsed.hostname,
                        user=parsed.username,
                        passwd=parsed.password or "",
                        db=parsed.path.lstrip("/"),
                    )
                except ImportError:
                    raise ImportError(
                        "mysqlclient is required for MySQL. "
                        "Install with: pip install mysqlclient"
                    )
            else:
                raise ValueError(f"Unsupported database scheme: {parsed.scheme}")

        if self._connection is None:
            raise ValueError("No database connection configured")

        return self._connection

    def load(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        column_mapping: dict[str, str] | None = None,
    ) -> int:
        """
        Load DataFrame into database table.

        Args:
            dataframe: DataFrame to load.
            table_name: Target table name.
            if_exists: How to handle existing table ("fail", "replace", "append").
            column_mapping: Optional column name mapping.

        Returns:
            Number of rows loaded.
        """
        df = dataframe.copy()

        # Apply column mapping
        if column_mapping:
            rename_map = {
                src: dst for src, dst in column_mapping.items()
                if src in df.columns
            }
            df = df.rename(columns=rename_map)

        conn = self._get_connection()

        # Use pandas to_sql for simplicity
        rows = df.to_sql(
            table_name,
            conn,
            if_exists=if_exists,
            index=False,
        )

        conn.commit()
        return rows if rows else len(df)

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> Any:
        """
        Execute raw SQL query.

        Args:
            sql: SQL query string.
            params: Query parameters.

        Returns:
            Query result cursor.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        conn.commit()
        return cursor

    def query(self, sql: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame.

        Args:
            sql: SQL query string.
            params: Query parameters.

        Returns:
            Query results as DataFrame.
        """
        conn = self._get_connection()

        if params:
            return pd.read_sql_query(sql, conn, params=params)
        return pd.read_sql_query(sql, conn)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Table name to check.

        Returns:
            True if table exists.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if self.sqlite_path or (
            self.connection_string and "sqlite" in self.connection_string
        ):
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
        else:
            # Generic approach for other databases
            try:
                cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                return True
            except Exception:
                return False

        return cursor.fetchone() is not None

    def get_table_columns(self, table_name: str) -> list[str]:
        """
        Get column names for a table.

        Args:
            table_name: Table name.

        Returns:
            List of column names.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if self.sqlite_path or (
            self.connection_string and "sqlite" in self.connection_string
        ):
            cursor.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        else:
            # Use information_schema for other databases
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = %s",
                (table_name,)
            )
            return [row[0] for row in cursor.fetchall()]

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "DatabaseLoader":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
