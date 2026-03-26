"""Microsoft SQL Server database connector"""
import logging
from typing import Dict, Any, List

try:
    import pyodbc
except ImportError:  # pragma: no cover - handled at runtime
    pyodbc = None

from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class SQLServerConnector(DatabaseConnector):
    """SQL Server connector (basic schema support)"""

    def connect(self, credentials: Dict[str, Any]):
        """Connect to SQL Server using pyodbc"""
        if pyodbc is None:
            raise ImportError(
                "pyodbc is required for SQL Server support. "
                "Please install it with: pip install pyodbc"
            )

        connection_string = credentials.get("connection_string")
        if connection_string:
            logger.info("Connecting to SQL Server using connection string")
            return pyodbc.connect(connection_string)

        host = credentials.get("host", "localhost")
        port = credentials.get("port", 1433)
        database = credentials.get("database")
        user = credentials.get("user")
        password = credentials.get("password")
        instance = credentials.get("instance")
        driver = credentials.get("driver", "ODBC Driver 17 for SQL Server")

        # Build SERVER part
        # For named instances, try both formats:
        # 1. host\instance (requires SQL Server Browser service)
        # 2. host,port (direct connection if port is known)
        if instance:
            # Try named instance format first (requires SQL Server Browser)
            server = f"{host}\\{instance}"
        else:
            # Default instance: use host,port format
            server = f"{host},{port}"

        # Check if Windows Authentication should be used (when user/password are empty)
        use_windows_auth = not user or not password
        
        # pyodbc requires driver name in curly braces: DRIVER={{ODBC Driver 17 for SQL Server}}
        if use_windows_auth:
            # Windows Authentication
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=10"
            )
            logger.info(f"Connecting to SQL Server using Windows Authentication: {server}/{database}")
        else:
            # SQL Server Authentication
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"TrustServerCertificate=yes;"
                f"Connection Timeout=10"
            )
            logger.info(f"Connecting to SQL Server: {user}@{server}/{database}")
        try:
            return pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            error_str = str(e)
            # Handle authentication errors
            if "Login failed" in error_str or "18456" in error_str:
                logger.error(f"Authentication failed for user '{user}'")
                logger.info("")
                logger.info("To fix SQL Server Authentication:")
                logger.info("1. Open SQL Server Management Studio (SSMS)")
                logger.info("2. Connect using Windows Authentication")
                logger.info("3. Right-click server -> Properties -> Security")
                logger.info("   -> Select 'SQL Server and Windows Authentication mode'")
                logger.info("4. Restart SQL Server service")
                logger.info("")
                logger.info("To enable 'sa' account:")
                logger.info("1. In SSMS: Security -> Logins -> sa -> Right-click -> Properties")
                logger.info("2. Status -> Login: Enabled")
                logger.info("3. General -> Set/change password")
                logger.info("")
                logger.info("Alternative: Use Windows Authentication")
                logger.info("   Set SQLSERVER_USER='' and SQLSERVER_PASSWORD='' in .env")
                raise
            # Handle connection refused errors
            elif "actively refused" in error_str or "10061" in error_str:
                if instance:
                    logger.warning(f"Named instance connection failed, trying with port {port}...")
                    server_with_port = f"{host}\\{instance},{port}"
                    conn_str_alt = (
                        f"DRIVER={{{driver}}};"
                        f"SERVER={server_with_port};"
                        f"DATABASE={database};"
                        f"UID={user};"
                        f"PWD={password};"
                        f"TrustServerCertificate=yes;"
                        f"Connection Timeout=10"
                    )
                    try:
                        logger.info(f"Retrying connection: {user}@{server_with_port}/{database}")
                        return pyodbc.connect(conn_str_alt)
                    except pyodbc.Error as e2:
                        logger.error(f"Both connection attempts failed. Last error: {e2}")
                        logger.info("Troubleshooting tips:")
                        logger.info("1. Ensure SQL Server is running (check Windows Services)")
                        logger.info("2. Ensure SQL Server Browser service is running (for named instances)")
                        logger.info("3. Check SQL Server Configuration Manager -> SQL Server Network Configuration -> Protocols -> TCP/IP is enabled")
                        logger.info("4. Verify the instance name and port are correct")
                        raise e2
                else:
                    logger.error(f"Connection refused to SQL Server")
                    logger.info("Troubleshooting tips:")
                    logger.info("1. Ensure SQL Server is running (check Windows Services)")
                    logger.info("2. Check SQL Server Configuration Manager -> SQL Server Network Configuration -> Protocols -> TCP/IP is enabled")
                    raise
            else:
                logger.error(f"Failed to connect to SQL Server. Error: {e}")
                logger.info("Troubleshooting tips:")
                logger.info("1. Ensure SQL Server is running (check Windows Services)")
                logger.info("2. Verify instance name and credentials are correct")
                raise

    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """
        Extract basic schema from SQL Server.
        - Tables
        - Columns (name, data type, nullability, default)
        """
        cursor = connection.cursor()

        try:
            # Get current database name
            cursor.execute("SELECT DB_NAME()")
            db_name = cursor.fetchone()[0]

            # Get user tables
            cursor.execute("""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            tables = [row[0] for row in cursor.fetchall()]

            table_infos: List[TableInfo] = []

            for table_name in tables:
                # Columns
                cursor.execute("""
                    SELECT 
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.COLUMN_DEFAULT,
                        c.CHARACTER_MAXIMUM_LENGTH,
                        c.NUMERIC_PRECISION,
                        c.NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_NAME = ?
                    ORDER BY c.ORDINAL_POSITION
                """, (table_name,))

                columns: List[ColumnInfo] = []
                for col in cursor.fetchall():
                    column_info = ColumnInfo(
                        name=col.COLUMN_NAME,
                        data_type=col.DATA_TYPE,
                        is_nullable=(col.IS_NULLABLE == "YES"),
                        default_value=col.COLUMN_DEFAULT,
                        character_maximum_length=col.CHARACTER_MAXIMUM_LENGTH,
                        numeric_precision=col.NUMERIC_PRECISION,
                        numeric_scale=col.NUMERIC_SCALE,
                        is_primary_key=False,  # filled below
                    )
                    columns.append(column_info)

                # Mark primary keys
                cursor.execute("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE OBJECTPROPERTY(
                        OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 
                        'IsPrimaryKey'
                    ) = 1
                    AND TABLE_NAME = ?
                """, (table_name,))
                pk_cols = {row[0] for row in cursor.fetchall()}

                for col in columns:
                    if col.name in pk_cols:
                        col.is_primary_key = True

                table_infos.append(TableInfo(
                    name=table_name,
                    columns=columns,
                    indexes=[],
                ))

            return SchemaInfo(
                database_type="sqlserver",
                database_name=db_name,
                tables=table_infos,
            )
        finally:
            cursor.close()

    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """
        Apply schema to SQL Server.
        This is a minimal implementation that:
        - Drops existing tables with the same name
        - Creates tables with columns and primary keys
        """
        cursor = connection.cursor()

        try:
            for table_info in schema.tables:
                # Drop existing table if it exists
                logger.info(f"Applying table '{table_info.name}' to SQL Server")
                # Check if table exists and drop it
                table_name_quoted = f"[{table_info.name}]"
                drop_sql = f"IF OBJECT_ID('{table_info.name}', 'U') IS NOT NULL DROP TABLE {table_name_quoted}"
                cursor.execute(drop_sql)

                column_defs = []
                # Track primary key columns to handle TEXT/MAX type conversion
                pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                
                for col in table_info.columns:
                    # Normalize data type from PostgreSQL-style to SQL Server-style
                    raw_type = (col.data_type or "").strip()
                    upper_type = raw_type.upper()

                    # Basic type mappings (extend as needed)
                    if upper_type in ["UUID"]:
                        data_type = "UNIQUEIDENTIFIER"
                    elif upper_type in ["BOOLEAN", "BOOL"]:
                        data_type = "BIT"
                    elif upper_type in ["TEXT"]:
                        # SQL Server doesn't allow TEXT/NVARCHAR(MAX) in primary keys
                        # Convert to NVARCHAR(900) for primary keys (max index key size)
                        if col.is_primary_key:
                            data_type = "NVARCHAR(900)"
                            logger.info(f"  Converting TEXT primary key '{col.name}' to NVARCHAR(900) for SQL Server compatibility")
                        else:
                            data_type = "NVARCHAR(MAX)"
                    elif upper_type in ["JSON", "JSONB"]:
                        # JSON/JSONB primary keys also need fixed length
                        if col.is_primary_key:
                            data_type = "NVARCHAR(900)"
                            logger.info(f"  Converting JSON primary key '{col.name}' to NVARCHAR(900) for SQL Server compatibility")
                        else:
                            data_type = "NVARCHAR(MAX)"
                    # PostgreSQL timestamp forms -> SQL Server DATETIME2
                    elif "TIMESTAMP WITHOUT TIME ZONE" in upper_type:
                        data_type = "DATETIME2"
                    elif "TIMESTAMP WITH TIME ZONE" in upper_type:
                        data_type = "DATETIME2"
                    elif upper_type == "TIMESTAMP":
                        data_type = "DATETIME2"
                    else:
                        data_type = raw_type

                    # Add length/precision if appropriate
                    if (
                        col.character_maximum_length
                        and data_type.upper() in ["VARCHAR", "NVARCHAR", "CHAR", "NCHAR"]
                    ):
                        # For primary keys, ensure we don't exceed SQL Server's 900 byte limit
                        if col.is_primary_key:
                            # SQL Server index key size limit is 900 bytes
                            # For NVARCHAR, each character is 2 bytes, so max is 450 characters
                            # For VARCHAR, each character is 1 byte, so max is 900 characters
                            max_length = col.character_maximum_length
                            if "NVARCHAR" in data_type.upper() or "NCHAR" in data_type.upper():
                                max_length = min(max_length, 450)  # 450 * 2 bytes = 900 bytes
                            else:
                                max_length = min(max_length, 900)  # 900 * 1 byte = 900 bytes
                            
                            if max_length < col.character_maximum_length:
                                logger.warning(f"  Reducing primary key '{col.name}' length from {col.character_maximum_length} to {max_length} for SQL Server index limit")
                            
                            data_type = f"{data_type.split('(')[0]}({max_length})"
                        else:
                            data_type = f"{data_type}({col.character_maximum_length})"
                    elif (
                        col.numeric_precision
                        and col.numeric_scale is not None
                        and data_type.upper() in ["DECIMAL", "NUMERIC"]
                    ):
                        data_type = f"{data_type}({col.numeric_precision},{col.numeric_scale})"
                    
                    # Check for VARCHAR(MAX) or NVARCHAR(MAX) in primary keys
                    if col.is_primary_key and "MAX" in data_type.upper():
                        # Convert to fixed length for primary key
                        if "NVARCHAR" in data_type.upper():
                            data_type = "NVARCHAR(900)"
                        elif "VARCHAR" in data_type.upper():
                            data_type = "VARCHAR(900)"
                        logger.info(f"  Converting MAX length primary key '{col.name}' to fixed length for SQL Server compatibility")

                    col_def = f"[{col.name}] {data_type}"
                    if not col.is_nullable:
                        col_def += " NOT NULL"

                    # For compatibility between PostgreSQL and SQL Server, and to avoid
                    # invalid expressions (e.g. identifiers like \"name\" used as defaults),
                    # we currently skip all column DEFAULT expressions when applying
                    # schema to SQL Server. Data can still be migrated separately.

                    column_defs.append(col_def)

                # Primary key
                if pk_cols:
                    pk_cols_sql = ", ".join(f"[{c}]" for c in pk_cols)
                    column_defs.append(f"CONSTRAINT [PK_{table_info.name}] PRIMARY KEY ({pk_cols_sql})")

                create_sql = f"CREATE TABLE [{table_info.name}] ({', '.join(column_defs)})"
                cursor.execute(create_sql)

            connection.commit()
            logger.info(f"Schema applied successfully to SQL Server database '{schema.database_name}'")
        except Exception as e:
            connection.rollback()
            logger.error(f"Error applying schema to SQL Server: {e}")
            raise
        finally:
            cursor.close()

