"""MariaDB database connector.

Implementation note:
- MariaDB is wire-compatible with MySQL for most use-cases supported here.
- We reuse the MySQL connector implementation and only adjust logging defaults.
"""

import logging
from typing import Dict, Any

from unified_db_mcp.database_connectors.mysql_connector import MySQLConnector

logger = logging.getLogger(__name__)


class MariaDBConnector(MySQLConnector):
    """MariaDB connector (reuses MySQL connector behavior)."""

    def connect(self, credentials: Dict[str, Any]):
        # MariaDB commonly runs on 3306, but user may override (e.g. 3307)
        host = credentials.get("host")
        port = credentials.get("port", 3306)
        database = credentials.get("database")
        user = credentials.get("user")
        logger.info(f"Connecting to MariaDB: {user}@{host}:{port}/{database}")
        return super().connect(credentials)

