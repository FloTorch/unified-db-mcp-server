"""Apache Cassandra connector (basic schema support)."""
import logging
import ssl
from typing import Dict, Any, List

from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy
except ImportError:  # pragma: no cover
    Cluster = None
    PlainTextAuthProvider = None
    DCAwareRoundRobinPolicy = None


class CassandraConnector(DatabaseConnector):
    """Cassandra connector.

    Notes:
    - Cassandra has no foreign keys/joins.
    - This connector extracts/applies core column metadata and primary keys.
    """

    @staticmethod
    def _coerce_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _parse_contact_points(credentials: Dict[str, Any]) -> List[str]:
        raw = (
            credentials.get("contact_points")
            or credentials.get("hosts")
            or credentials.get("host")
            or "127.0.0.1"
        )
        if isinstance(raw, list):
            points = [str(item).strip() for item in raw if str(item).strip()]
            return points or ["127.0.0.1"]
        if isinstance(raw, str):
            points = [item.strip() for item in raw.split(",") if item.strip()]
            return points or ["127.0.0.1"]
        return [str(raw).strip() or "127.0.0.1"]

    @staticmethod
    def _build_ssl_context(credentials: Dict[str, Any]):
        use_ssl = CassandraConnector._coerce_bool(
            credentials.get("use_ssl", credentials.get("ssl_enabled")),
            default=False,
        )
        ssl_ca = credentials.get("ssl_ca")
        ssl_cert = credentials.get("ssl_cert")
        ssl_key = credentials.get("ssl_key")
        ssl_verify = CassandraConnector._coerce_bool(credentials.get("ssl_verify"), default=True)
        ssl_check_hostname = CassandraConnector._coerce_bool(
            credentials.get("ssl_check_hostname"),
            default=ssl_verify,
        )

        if not use_ssl and not any([ssl_ca, ssl_cert, ssl_key]):
            return None

        context = ssl.create_default_context()
        if not ssl_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context.check_hostname = ssl_check_hostname
            if ssl_ca:
                context.load_verify_locations(cafile=str(ssl_ca))
        if ssl_cert:
            context.load_cert_chain(certfile=str(ssl_cert), keyfile=str(ssl_key) if ssl_key else None)
        return context

    def connect(self, credentials: Dict[str, Any]):
        if Cluster is None:
            raise ImportError(
                "cassandra-driver is required for Cassandra support. "
                "Install with: pip install cassandra-driver"
            )

        contact_points = self._parse_contact_points(credentials)
        port = int(credentials.get("port", 9042))
        keyspace = credentials.get("keyspace", "testdb")
        username = credentials.get("user") or credentials.get("username")
        password = credentials.get("password")
        datacenter = credentials.get("datacenter", "datacenter1")
        secure_connect_bundle = credentials.get("secure_connect_bundle")
        ssl_context = self._build_ssl_context(credentials)
        extra_ssl_options = credentials.get("ssl_options")

        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        if secure_connect_bundle:
            logger.info("Connecting to Cassandra via secure connect bundle: %s", secure_connect_bundle)
            cluster_kwargs: Dict[str, Any] = {
                "cloud": {"secure_connect_bundle": str(secure_connect_bundle)},
                "auth_provider": auth_provider,
            }
        else:
            logger.info("Connecting to Cassandra: %s:%s/%s", contact_points, port, keyspace)
            cluster_kwargs = {
                "contact_points": contact_points,
                "port": port,
                "auth_provider": auth_provider,
            }
            if datacenter and DCAwareRoundRobinPolicy is not None:
                cluster_kwargs["load_balancing_policy"] = DCAwareRoundRobinPolicy(local_dc=str(datacenter))
            if ssl_context is not None:
                cluster_kwargs["ssl_context"] = ssl_context
            if isinstance(extra_ssl_options, dict) and extra_ssl_options:
                cluster_kwargs["ssl_options"] = extra_ssl_options

        cluster = Cluster(**cluster_kwargs)
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

