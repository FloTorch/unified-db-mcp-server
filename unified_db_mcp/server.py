"""
Unified DB FastMCP server.
"""

import asyncio
import base64
import json
import logging

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse

from unified_db_mcp.config import APP_NAME, HOST, MCP_PATH, PORT, SUPPORTED_DATABASES
from unified_db_mcp.tools.migrate_schema_tool import (
    migrate_schema_details,
    migrate_schema_text,
    parse_tables_arg,
)
from unified_db_mcp.tools.schema_connector_tools import apply_schema_tool, connect_db, extract_schema_tool

logging.basicConfig(
    format="[%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    APP_NAME,
    instructions=(
        "Unified DB schema migration MCP server. "
        "Migrate schema between supported databases using schema_migrate.py."
    ),
    json_response=True,
    streamable_http_path=MCP_PATH,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    stateless_http=True,
)


def _extract_headers_from_context(ctx: Context = None) -> dict:
    """Extract request headers from FastMCP context as lowercase keys."""
    if not ctx:
        return {}
    try:
        request_context = ctx.request_context
        if hasattr(request_context, "request") and request_context.request:
            request = request_context.request
            return {name.lower(): request.headers[name] for name in request.headers.keys()}
    except Exception:
        return {}
    return {}


def _normalize_credentials_value(value: str) -> str:
    """
    Normalize credentials string from headers.

    Supports:
    - raw JSON string
    - base64-encoded JSON string
    """
    if not value:
        return ""
    candidate = value.strip()
    if not candidate:
        return ""

    # If already valid JSON object text, use it directly.
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return json.dumps(parsed)
    except Exception:
        pass

    # Try base64 decode then parse as JSON object.
    try:
        decoded = base64.b64decode(candidate).decode("utf-8")
        parsed = json.loads(decoded)
        if isinstance(parsed, dict):
            return json.dumps(parsed)
    except Exception:
        pass

    # Fallback to raw value; downstream validation will raise clear errors.
    return candidate


def _resolve_credentials_from_headers(
    db_type: str,
    credentials_json: str = "",
    sqlite_path: str = "",
    ctx: Context = None,
) -> tuple[str, str]:
    """
    Resolve credentials/sqlite_path from function args or HTTP headers.

    Priority:
    1) explicit function args
    2) database-specific header: x-<db>-credentials
    3) global header: x-db-credentials
    """
    headers = _extract_headers_from_context(ctx)
    normalized_db = db_type.lower().strip()

    resolved_credentials = credentials_json or ""
    resolved_sqlite_path = sqlite_path or ""

    if not resolved_credentials:
        db_header = f"x-{normalized_db}-credentials"
        resolved_credentials = headers.get(db_header, "") or headers.get("x-db-credentials", "")
    resolved_credentials = _normalize_credentials_value(resolved_credentials)

    if not resolved_sqlite_path and normalized_db == "sqlite":
        resolved_sqlite_path = headers.get("x-sqlite-path", "")

    return resolved_credentials, resolved_sqlite_path


def _resolve_migration_credentials_from_headers(
    source_db: str,
    target_db: str,
    source_credentials_json: str = "",
    target_credentials_json: str = "",
    source_sqlite_path: str = "",
    target_sqlite_path: str = "",
    ctx: Context = None,
) -> tuple[str, str, str, str]:
    headers = _extract_headers_from_context(ctx)
    source_key = source_db.lower().strip()
    target_key = target_db.lower().strip()

    src_creds = source_credentials_json or headers.get("x-source-db-credentials", "")
    tgt_creds = target_credentials_json or headers.get("x-target-db-credentials", "")

    if not src_creds:
        src_creds = headers.get(f"x-{source_key}-credentials", "") or headers.get("x-db-credentials", "")
    if not tgt_creds:
        tgt_creds = headers.get(f"x-{target_key}-credentials", "") or headers.get("x-db-credentials", "")

    src_sqlite = source_sqlite_path or headers.get("x-source-sqlite-path", "")
    tgt_sqlite = target_sqlite_path or headers.get("x-target-sqlite-path", "")
    if not src_sqlite and source_key == "sqlite":
        src_sqlite = headers.get("x-sqlite-path", "")
    if not tgt_sqlite and target_key == "sqlite":
        tgt_sqlite = headers.get("x-sqlite-path", "")

    return (
        _normalize_credentials_value(src_creds),
        _normalize_credentials_value(tgt_creds),
        src_sqlite,
        tgt_sqlite,
    )


@mcp.custom_route(MCP_PATH, methods=["GET"])
async def discovery(_request: StarletteRequest) -> JSONResponse:
    return JSONResponse(
        {
            "transport": "HTTP_STREAMABLE",
            "protocol": "streamable-http",
            "message": "Unified DB MCP Server - Set transport to HTTP_STREAMABLE",
            "supported_databases": SUPPORTED_DATABASES,
        }
    )


@mcp.custom_route("/migrate_schema", methods=["POST"])
async def migrate_schema_simple(request: StarletteRequest) -> JSONResponse:
    """
    Simple Postman-friendly route:
    {
      "source_db": "sqlite",
      "target_db": "mysql",
      "tables": "users,orders"   // optional; omit/empty => all tables
    }
    """
    client = request.client.host if request.client else "unknown"
    logger.info("POST /migrate_schema from %s", client)
    try:
        body = await request.json()
        source_db = body.get("source_db")
        target_db = body.get("target_db")
        tables = body.get("tables")
        dry_run = bool(body.get("dry_run", False))
        require_confirmation = bool(body.get("require_confirmation", False))
        source_credentials_json = body.get("source_credentials_json", "")
        target_credentials_json = body.get("target_credentials_json", "")
        source_db_credentials = body.get("source_db_credentials")
        target_db_credentials = body.get("target_db_credentials")
        source_sqlite_path = body.get("source_sqlite_path", "")
        target_sqlite_path = body.get("target_sqlite_path", "")

        # Body object credentials are optional fallback when headers are not provided.
        if source_db_credentials and isinstance(source_db_credentials, dict):
            source_credentials_json = json.dumps(source_db_credentials)
        if target_db_credentials and isinstance(target_db_credentials, dict):
            target_credentials_json = json.dumps(target_db_credentials)

        headers = {name.lower(): value for name, value in request.headers.items()}
        logger.info(
            "migrate_schema incoming headers: %s",
            {k: v for k, v in headers.items() if k.startswith("x-")},
        )
        # Header-first resolution (user-provided headers take priority).
        source_credentials_json = (
            headers.get("x-source-db-credentials", "")
            or headers.get(f"x-{str(source_db).lower()}-credentials", "")
            or headers.get("x-db-credentials", "")
            or source_credentials_json
        )
        target_credentials_json = (
            headers.get("x-target-db-credentials", "")
            or headers.get(f"x-{str(target_db).lower()}-credentials", "")
            or headers.get("x-db-credentials", "")
            or target_credentials_json
        )
        source_credentials_json = _normalize_credentials_value(source_credentials_json)
        target_credentials_json = _normalize_credentials_value(target_credentials_json)
        logger.info(
            "migrate_schema: source_db=%s target_db=%s dry_run=%s tables=%s "
            "has_source_creds=%s has_target_creds=%s",
            source_db,
            target_db,
            dry_run,
            tables,
            bool(source_credentials_json),
            bool(target_credentials_json),
        )
        source_sqlite_path = (
            headers.get("x-source-sqlite-path", "")
            or (headers.get("x-sqlite-path", "") if str(source_db).lower() == "sqlite" else "")
            or source_sqlite_path
        )
        target_sqlite_path = (
            headers.get("x-target-sqlite-path", "")
            or (headers.get("x-sqlite-path", "") if str(target_db).lower() == "sqlite" else "")
            or target_sqlite_path
        )

        if str(source_db).lower() != "sqlite" and not source_credentials_json:
            return JSONResponse(
                {
                    "success": False,
                    "error": "Missing source credentials header. Provide x-source-db-credentials (or x-<source_db>-credentials).",
                },
                status_code=400,
            )
        if str(target_db).lower() != "sqlite" and not target_credentials_json:
            return JSONResponse(
                {
                    "success": False,
                    "error": "Missing target credentials header. Provide x-target-db-credentials (or x-<target_db>-credentials).",
                },
                status_code=400,
            )

        result_details = migrate_schema_details(
            source_db=source_db,
            target_db=target_db,
            tables=tables,
            dry_run=dry_run,
            require_confirmation=require_confirmation,
            source_credentials_json=source_credentials_json or None,
            target_credentials_json=target_credentials_json or None,
            source_sqlite_path=source_sqlite_path or None,
            target_sqlite_path=target_sqlite_path or None,
        )

        ok = bool(result_details.get("success", False))
        logger.info(
            "migrate_schema finished: success=%s table_count=%s",
            ok,
            result_details.get("table_count", 0),
        )
        return JSONResponse(
            {
                "success": ok,
                "result": result_details.get("result", ""),
                "source_db": source_db,
                "target_db": target_db,
                "tables": result_details.get("tables", parse_tables_arg(tables)),
                "table_count": result_details.get("table_count", 0),
                "dry_run": dry_run,
            }
        )
    except Exception as exc:
        logger.exception("Error while handling /migrate_schema")
        return JSONResponse(
            {"success": False, "error": str(exc), "error_type": type(exc).__name__},
            status_code=500,
        )


@mcp.tool()
def connect_database(
    db_type: str,
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Connect to a database using connector credentials/config (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=db_type,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "connect_database: db_type=%s sqlite_path=%s credentials_json=%s",
        db_type,
        sqlite_path,
        credentials_json,
    )
    return connect_db(
        db_type=db_type,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def extract_schema(
    db_type: str,
    tables: str = "",
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Extract schema from a source database and return JSON text (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=db_type,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "extract_schema: db_type=%s tables=%s sqlite_path=%s credentials_json=%s",
        db_type,
        tables,
        sqlite_path,
        credentials_json,
    )
    return extract_schema_tool(
        db_type=db_type,
        tables=tables or None,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def apply_schema(
    target_db: str,
    schema_json: str,
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Apply provided schema JSON to target database (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=target_db,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "apply_schema: target_db=%s schema_json=%s sqlite_path=%s credentials_json=%s",
        target_db,
        schema_json,
        sqlite_path,
        credentials_json,
    )
    return apply_schema_tool(
        target_db=target_db,
        schema_json=schema_json,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def migrate_schema(
    source_db: str,
    target_db: str,
    tables: str = "",
    dry_run: bool = False,
    require_confirmation: bool = False,
    source_credentials_json: str = "",
    target_credentials_json: str = "",
    source_sqlite_path: str = "",
    target_sqlite_path: str = "",
    ctx: Context = None,
) -> str:
    """
    High-level migration tool.
    - tables: optional comma-separated names; empty means migrate all tables.
    """
    (
        source_credentials_json,
        target_credentials_json,
        source_sqlite_path,
        target_sqlite_path,
    ) = _resolve_migration_credentials_from_headers(
        source_db=source_db,
        target_db=target_db,
        source_credentials_json=source_credentials_json,
        target_credentials_json=target_credentials_json,
        source_sqlite_path=source_sqlite_path,
        target_sqlite_path=target_sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "migrate_schema: source_db=%s target_db=%s tables=%s dry_run=%s require_confirmation=%s source_credentials_json=%s target_credentials_json=%s source_sqlite_path=%s target_sqlite_path=%s",
        source_db,
        target_db,
        tables,
        dry_run,
        require_confirmation,
    )

    return migrate_schema_text(
        source_db=source_db,
        target_db=target_db,
        tables=tables or None,
        dry_run=dry_run,
        require_confirmation=require_confirmation,
        source_credentials_json=source_credentials_json or None,
        target_credentials_json=target_credentials_json or None,
        source_sqlite_path=source_sqlite_path or None,
        target_sqlite_path=target_sqlite_path or None,
    )


async def main() -> None:
    mcp.settings.host = HOST
    mcp.settings.port = PORT
    mcp.settings.log_level = "INFO"

    logger.info("Starting Unified DB MCP on http://%s:%s", HOST, PORT)
    logger.info(
        "Registered custom routes: %s",
        [(r.path, r.methods) for r in mcp._custom_starlette_routes],
    )
    await mcp.run_streamable_http_async()


if __name__ == "__main__":
    asyncio.run(main())

