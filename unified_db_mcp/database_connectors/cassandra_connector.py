"""Apache Cassandra connector (basic schema support)."""
import logging
from typing import Dict, Any, List

from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:  # pragma: no cover
    Cluster = None
    PlainTextAuthProvider = None


class CassandraConnector(DatabaseConnector):
    """Cassandra connector.

    Notes:
    - Cassandra has no foreign keys/joins.
    - This connector extracts/applies core column metadata and primary keys.
    """

    def connect(self, credentials: Dict[str, Any]):
        if Cluster is None:
            raise ImportError(
                "cassandra-driver is required for Cassandra support. "
                "Install with: pip install cassandra-driver"
            )

        host = credentials.get("host", "127.0.0.1")
        port = int(credentials.get("port", 9042))
        keyspace = credentials.get("keyspace", "testdb")
        username = credentials.get("user") or credentials.get("username")
        password = credentials.get("password")
        datacenter = credentials.get("datacenter", "datacenter1")

        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        logger.info("Connecting to Cassandra: %s:%s/%s", host, port, keyspace)
        cluster = Cluster([host], port=port, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(keyspace)
        return {"cluster": cluster, "session": session, "keyspace": keyspace}

    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        session = connection["session"] if isinstance(connection, dict) else connection
        keyspace = (
            connection.get("keyspace")
            if isinstance(connection, dict)
            else (credentials or {}).get("keyspace", "testdb")
        )

        rows = session.execute(
            """
            SELECT table_name, column_name, type, kind, position
            FROM system_schema.columns
            WHERE keyspace_name = %s
            """,
            [keyspace],
        )

        table_map: Dict[str, List[ColumnInfo]] = {}
        for row in rows:
            table_name = row.table_name
            col_name = row.column_name
            data_type = str(row.type).upper()
            is_pk = row.kind in ("partition_key", "clustering")
            column = ColumnInfo(
                name=col_name,
                data_type=data_type,
                is_nullable=True,  # Cassandra is sparse and doesn't enforce nullability
                default_value=None,
                is_primary_key=is_pk,
                is_foreign_key=False,
            )
            table_map.setdefault(table_name, []).append(column)

        tables: List[TableInfo] = []
        for table_name, columns in table_map.items():
            tables.append(TableInfo(name=table_name, columns=columns, indexes=[]))

        return SchemaInfo(database_type="cassandra", database_name=keyspace, tables=tables)

    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        session = connection["session"] if isinstance(connection, dict) else connection
        keyspace = (
            connection.get("keyspace")
            if isinstance(connection, dict)
            else (credentials or {}).get("keyspace", schema.database_name or "testdb")
        )

        for table in schema.tables:
            pk_cols = [c.name for c in table.columns if c.is_primary_key]
            if not pk_cols and table.columns:
                pk_cols = [table.columns[0].name]

            col_defs = [f'"{c.name}" {c.data_type}' for c in table.columns]
            pk_cols_quoted = ", ".join([f'"{c}"' for c in pk_cols])
            pk_def = f"PRIMARY KEY ({pk_cols_quoted})"
            cql = (
                f'CREATE TABLE IF NOT EXISTS "{keyspace}"."{table.name}" '
                f"({', '.join(col_defs + [pk_def])})"
            )
            session.execute(cql)

        logger.info("Schema applied successfully to Cassandra keyspace '%s'", keyspace)

