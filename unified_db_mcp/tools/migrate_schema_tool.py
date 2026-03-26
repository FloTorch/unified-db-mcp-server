"""
High-level migration wrapper.
"""

import json
import os
from typing import Any, Dict
from typing import List, Optional

from unified_db_mcp.database_connectors.base_connector import DatabaseConnector
from unified_db_mcp.schema_migrate import convert_schema_between_databases


def parse_tables_arg(tables: Optional[str]) -> Optional[List[str]]:
    if not tables:
        return None
    parsed = [name.strip() for name in tables.split(",") if name.strip()]
    return parsed or None


def migrate_schema_text(
    source_db: str,
    target_db: str,
    tables: Optional[str] = None,
    dry_run: bool = False,
    require_confirmation: bool = False,
    source_credentials_json: Optional[str] = None,
    target_credentials_json: Optional[str] = None,
    source_sqlite_path: Optional[str] = None,
    target_sqlite_path: Optional[str] = None,
) -> str:
    details = migrate_schema_details(
        source_db=source_db,
        target_db=target_db,
        tables=tables,
        dry_run=dry_run,
        require_confirmation=require_confirmation,
        source_credentials_json=source_credentials_json,
        target_credentials_json=target_credentials_json,
        source_sqlite_path=source_sqlite_path,
        target_sqlite_path=target_sqlite_path,
    )
    return details["result"]


def migrate_schema_details(
    source_db: str,
    target_db: str,
    tables: Optional[str] = None,
    dry_run: bool = False,
    require_confirmation: bool = False,
    source_credentials_json: Optional[str] = None,
    target_credentials_json: Optional[str] = None,
    source_sqlite_path: Optional[str] = None,
    target_sqlite_path: Optional[str] = None,
) -> Dict[str, Any]:
    if require_confirmation:
        raise ValueError("require_confirmation is not supported in MCP header-driven mode")

    source_connector = DatabaseConnector.get_connector(source_db)
    target_connector = DatabaseConnector.get_connector(target_db)

    source_credentials = _build_credentials_from_json(
        db_type=source_db,
        credentials_json=source_credentials_json,
        sqlite_path=source_sqlite_path,
    )
    target_credentials = _build_credentials_from_json(
        db_type=target_db,
        credentials_json=target_credentials_json,
        sqlite_path=target_sqlite_path,
    )

    source_connection = source_connector.connect(source_credentials)
    try:
        source_schema = source_connector.extract_schema(source_connection, source_credentials)
    finally:
        if hasattr(source_connection, "close"):
            source_connection.close()

    table_names = parse_tables_arg(tables)
    if table_names:
        source_schema.tables = [t for t in source_schema.tables if t.name in table_names]

    if not source_schema.tables:
        return {
            "success": False,
            "result": f"Migration failed: no tables found for {source_db}",
            "tables": [],
            "table_count": 0,
        }

    target_schema = convert_schema_between_databases(
        source_schema=source_schema,
        source_db_type=source_db,
        target_db_type=target_db,
    )
    migrated_tables = [t.name for t in target_schema.tables]

    if dry_run:
        return {
            "success": True,
            "result": f"Migration completed: {source_db} -> {target_db} (dry-run)",
            "tables": migrated_tables,
            "table_count": len(migrated_tables),
        }

    target_connection = target_connector.connect(target_credentials)
    try:
        target_connector.apply_schema(target_connection, target_schema, target_credentials)
    finally:
        if hasattr(target_connection, "close"):
            target_connection.close()

    return {
        "success": True,
        "result": f"Migration completed: {source_db} -> {target_db}",
        "tables": migrated_tables,
        "table_count": len(migrated_tables),
    }


def _build_credentials_from_json(
    db_type: str,
    credentials_json: Optional[str],
    sqlite_path: Optional[str],
) -> Dict[str, Any]:
    # Header/body driven MCP mode: do not fallback to .env credentials.
    creds: Dict[str, Any] = {}

    if credentials_json:
        loaded = json.loads(credentials_json)
        if not isinstance(loaded, dict):
            raise ValueError("credentials must be a JSON object")
        creds.update(loaded)

    if db_type.lower() == "sqlite":
        if sqlite_path:
            creds["database_path"] = sqlite_path
        if "database_path" not in creds:
            raise ValueError("SQLite requires database_path (via header or body)")
    elif db_type.lower() == "supabase":
        # Keep region handling in code to avoid requiring users to pass it in every request.
        creds.setdefault("region", os.getenv("SUPABASE_REGION", "ap-southeast-1"))
        if "api_key" not in creds and "key" not in creds:
            raise ValueError("Supabase requires api_key in source/target header credentials")
    else:
        if not creds:
            raise ValueError(f"{db_type} requires credentials in header (x-source-db-credentials / x-target-db-credentials)")

    return creds

