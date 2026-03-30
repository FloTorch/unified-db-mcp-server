"""
Low-level connector tools: connect, extract_schema, apply_schema.
"""

import json
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from unified_db_mcp.database_connectors.base_connector import DatabaseConnector
from unified_db_mcp.helpers.schema_utils import ColumnInfo, SchemaInfo, TableInfo
from unified_db_mcp.schema_migrate import Config


def _build_credentials(
    db_type: str,
    sqlite_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> Dict[str, Any]:
    creds = Config.get_credentials(db_type)
    if db_type.lower() == "sqlite" and sqlite_path:
        creds["database_path"] = sqlite_path

    if credentials_json:
        overrides = json.loads(credentials_json)
        if not isinstance(overrides, dict):
            raise ValueError("credentials_json must decode to an object")
        creds.update(overrides)
    return creds


def connect_db(
    db_type: str,
    sqlite_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> str:
    connector = DatabaseConnector.get_connector(db_type)
    creds = _build_credentials(db_type, sqlite_path=sqlite_path, credentials_json=credentials_json)
    connection = connector.connect(creds)
    try:
        return f"Connected successfully to {db_type}"
    finally:
        if hasattr(connection, "close"):
            connection.close()


def extract_schema_tool(
    db_type: str,
    tables: Optional[str] = None,
    sqlite_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> str:
    connector = DatabaseConnector.get_connector(db_type)
    creds = _build_credentials(db_type, sqlite_path=sqlite_path, credentials_json=credentials_json)
    connection = connector.connect(creds)
    try:
        schema = connector.extract_schema(connection, creds)
    finally:
        if hasattr(connection, "close"):
            connection.close()

    if tables:
        wanted = [name.strip() for name in tables.split(",") if name.strip()]
        schema.tables = [table for table in schema.tables if table.name in wanted]

    payload = {
        "database_type": schema.database_type,
        "database_name": schema.database_name,
        "table_count": len(schema.tables),
        "tables": [table.name for table in schema.tables],
        "schema": asdict(schema),
    }
    return json.dumps(payload, default=str)


def _column_from_dict(data: Dict[str, Any]) -> ColumnInfo:
    return ColumnInfo(**data)


def _table_from_dict(data: Dict[str, Any]) -> TableInfo:
    columns = [_column_from_dict(col) for col in data.get("columns", [])]
    return TableInfo(
        name=data["name"],
        columns=columns,
        indexes=data.get("indexes", []),
        constraints=data.get("constraints", []),
        comment=data.get("comment"),
    )


def _schema_from_dict(data: Dict[str, Any]) -> SchemaInfo:
    tables = [_table_from_dict(table) for table in data.get("tables", [])]
    return SchemaInfo(
        database_type=data["database_type"],
        database_name=data.get("database_name", ""),
        tables=tables,
        views=data.get("views", []),
        sequences=data.get("sequences", []),
        version=data.get("version", "1.0"),
    )


def apply_schema_tool(
    target_db: str,
    schema_json: str,
    sqlite_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> str:
    schema_data = json.loads(schema_json)
    if not isinstance(schema_data, dict):
        raise ValueError("schema_json must decode to an object")

    # Accept common orchestration payload wrappers.
    if "migration_preview" in schema_data and isinstance(schema_data["migration_preview"], dict):
        preview = schema_data["migration_preview"]
        if isinstance(preview.get("target_schema"), dict):
            schema_data = preview["target_schema"]
    elif "target_schema" in schema_data and isinstance(schema_data["target_schema"], dict):
        schema_data = schema_data["target_schema"]

    if "schema" in schema_data and isinstance(schema_data["schema"], dict):
        schema_data = schema_data["schema"]

    # Some clients send table-only schema payloads; infer the target db type.
    if "database_type" not in schema_data:
        schema_data = dict(schema_data)
        schema_data["database_type"] = target_db

    schema = _schema_from_dict(schema_data)
    connector = DatabaseConnector.get_connector(target_db)
    creds = _build_credentials(target_db, sqlite_path=sqlite_path, credentials_json=credentials_json)
    connection = connector.connect(creds)
    try:
        connector.apply_schema(connection, schema, creds)
    finally:
        if hasattr(connection, "close"):
            connection.close()
    return f"Schema applied successfully to {target_db}"

