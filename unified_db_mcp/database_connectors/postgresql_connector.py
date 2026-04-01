"""PostgreSQL database connector"""
import logging
import re
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any
from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL connector"""
    
    def connect(self, credentials: Dict[str, Any]):
        """Connect to PostgreSQL"""
        connection_string = credentials.get("connection_string") or credentials.get("dsn")
        if connection_string:
            conn = psycopg2.connect(connection_string)
            return conn

        conn_params = {
            "host": credentials.get("host"),
            "port": credentials.get("port", 5432),
            "database": credentials.get("database"),
            "user": credentials.get("user"),
            "password": credentials.get("password"),
        }
        optional_fields = ("sslmode", "sslcert", "sslkey", "sslrootcert", "connect_timeout", "options")
        for field in optional_fields:
            if field in credentials and credentials.get(field) is not None:
                conn_params[field] = credentials.get(field)  

        conn = psycopg2.connect(**conn_params)
        return conn
    
    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """Extract schema from PostgreSQL"""
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get database name
            cursor.execute("SELECT current_database()")
            db_name = cursor.fetchone()['current_database']
            
            # Get all tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row['table_name'] for row in cursor.fetchall()]
            
            table_infos = []
            for table_name in tables:
                # Get foreign keys for this table once (including ON UPDATE/ON DELETE rules).
                cursor.execute("""
                    SELECT
                        kcu.column_name AS local_column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name,
                        rc.update_rule,
                        rc.delete_rule
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.referential_constraints rc
                        ON tc.constraint_name = rc.constraint_name
                        AND tc.table_schema = rc.constraint_schema
                    JOIN information_schema.key_column_usage ccu
                        ON rc.unique_constraint_name = ccu.constraint_name
                        AND rc.unique_constraint_schema = ccu.table_schema
                        AND kcu.position_in_unique_constraint = ccu.ordinal_position
                    WHERE tc.table_schema = 'public'
                      AND tc.table_name = %s
                      AND tc.constraint_type = 'FOREIGN KEY'
                """, (table_name,))
                fk_rows = cursor.fetchall()
                fk_by_column = {
                    row['local_column_name']: row
                    for row in fk_rows
                    if row.get('local_column_name')
                }

                # Get columns
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_name,))
                
                columns = []
                for col in cursor.fetchall():
                    # Check for primary key
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        WHERE tc.table_name = %s
                        AND kcu.column_name = %s
                        AND tc.constraint_type = 'PRIMARY KEY'
                    """, (table_name, col['column_name']))
                    is_pk = cursor.fetchone()['count'] > 0
                    
                    fk_info = fk_by_column.get(col['column_name'])
                    
                    column_info = ColumnInfo(
                        name=col['column_name'],
                        data_type=col['data_type'],
                        is_nullable=col['is_nullable'] == 'YES',
                        default_value=col['column_default'],
                        character_maximum_length=col['character_maximum_length'],
                        numeric_precision=col['numeric_precision'],
                        numeric_scale=col['numeric_scale'],
                        is_primary_key=is_pk,
                        is_foreign_key=fk_info is not None,
                        foreign_key_table=fk_info['foreign_table_name'] if fk_info else None,
                        foreign_key_column=fk_info['foreign_column_name'] if fk_info else None,
                        foreign_key_on_delete=fk_info['delete_rule'] if fk_info else None,
                        foreign_key_on_update=fk_info['update_rule'] if fk_info else None,
                    )
                    columns.append(column_info)
                
                # Get indexes
                cursor.execute("""
                    SELECT 
                        indexname,
                        indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public' 
                    AND tablename = %s
                """, (table_name,))
                indexes = [{'name': row['indexname'], 'definition': row['indexdef']} 
                          for row in cursor.fetchall()]
                
                table_infos.append(TableInfo(
                    name=table_name,
                    columns=columns,
                    indexes=indexes
                ))
            
            return SchemaInfo(
                database_type='postgresql',
                database_name=db_name,
                tables=table_infos
            )
        finally:
            cursor.close()
    
    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """Apply schema to PostgreSQL database"""
        cursor = connection.cursor()
        
        try:
            pending_foreign_keys = []
            for table_info in schema.tables:
                # Ensure sequences referenced by nextval(...) defaults exist before table creation.
                sequences_to_create = set()
                for col in table_info.columns:
                    if not col.default_value:
                        continue
                    default_str = str(col.default_value)
                    seq_match = re.search(r"nextval\s*\(\s*'([^']+)'", default_str, re.IGNORECASE)
                    if seq_match:
                        sequences_to_create.add(seq_match.group(1))

                for seq_name in sequences_to_create:
                    try:
                        # Handle names like "public.pages_id_seq" or "pages_id_seq".
                        if "." in seq_name:
                            seq_schema, seq_obj = seq_name.split(".", 1)
                            cursor.execute(f'CREATE SEQUENCE IF NOT EXISTS "{seq_schema}"."{seq_obj}"')
                        else:
                            cursor.execute(f'CREATE SEQUENCE IF NOT EXISTS "{seq_name}"')
                    except Exception as seq_error:
                        logger.warning(f"  Could not create sequence '{seq_name}' for '{table_info.name}': {seq_error}")

                # Build CREATE TABLE statement
                column_defs = []
                for col in table_info.columns:
                    # Clean data type
                    data_type = col.data_type

                    # Normalize common SQL Server/MySQL types to PostgreSQL equivalents
                    upper_type = (data_type or "").upper()

                    # NVARCHAR / VARCHAR(MAX) from SQL Server -> TEXT or VARCHAR
                    if upper_type.startswith("NVARCHAR"):
                        # If max length is unbounded or negative (e.g., -1 from MAX), use TEXT
                        if col.character_maximum_length is None or (isinstance(col.character_maximum_length, int) and col.character_maximum_length < 0):
                            data_type = "TEXT"
                            col.character_maximum_length = None
                        else:
                            data_type = "VARCHAR"
                    elif upper_type.startswith("VARCHAR") and isinstance(col.character_maximum_length, int) and col.character_maximum_length < 0:
                        # VARCHAR with negative length (e.g., -1 from SQL Server MAX) -> TEXT
                        data_type = "TEXT"
                        col.character_maximum_length = None
                    # SQL Server datetime types -> PostgreSQL TIMESTAMP
                    elif upper_type in ["DATETIME2", "DATETIME", "SMALLDATETIME", "DATETIMEOFFSET"]:
                        data_type = "TIMESTAMP"
                    # PostgreSQL does not support TEXT(n) modifiers.
                    elif upper_type.startswith("TEXT("):
                        data_type = "TEXT"

                    # Clean data type - remove length from integer types (PostgreSQL doesn't support SMALLINT(1))
                    # Remove length parameters from integer types using regex
                    # Pattern: INTEGER(1), SMALLINT(1), etc. -> INTEGER, SMALLINT
                    integer_pattern = r'^(INTEGER|INT|SMALLINT|BIGINT|TINYINT)\(\d+\)'
                    if re.match(integer_pattern, data_type.upper()):
                        data_type = re.sub(r'\(\d+\)', '', data_type.upper())
                    
                    # Check if it's an integer type (should not have length parameters)
                    is_integer_type = data_type.upper() in ['INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'TINYINT', 'SERIAL', 'BIGSERIAL']
                    
                    col_def = f'"{col.name}" {data_type}'
                    
                    # Check if data_type already includes length/precision (e.g., VARCHAR(255))
                    has_length_in_type = '(' in data_type.upper()
                    
                    # Only add length/precision if NOT an integer type and NOT already in data_type
                    if not has_length_in_type and not is_integer_type:
                        if col.character_maximum_length and data_type.upper() in ["VARCHAR", "CHAR", "CHARACTER VARYING", "CHARACTER"]:
                            col_def += f'({col.character_maximum_length})'
                        elif col.numeric_precision and col.numeric_scale:
                            col_def += f'({col.numeric_precision},{col.numeric_scale})'
                        elif col.numeric_precision:
                            col_def += f'({col.numeric_precision})'
                    
                    if not col.is_nullable:
                        col_def += ' NOT NULL'
                    
                    if col.default_value:
                        default_str = str(col.default_value)
                        
                        # Normalize MySQL/MariaDB timestamp functions to PostgreSQL format
                        default_lower = default_str.lower().strip()
                        if default_lower in ['current_timestamp()', 'current_timestamp', 'now()', 'now']:
                            # PostgreSQL uses CURRENT_TIMESTAMP without parentheses
                            default_str = 'CURRENT_TIMESTAMP'
                        elif default_lower == 'current_date()':
                            default_str = 'CURRENT_DATE'
                        # Map SQL Server GETDATE() to PostgreSQL CURRENT_TIMESTAMP
                        elif isinstance(col.default_value, str) and 'getdate()' in default_lower:
                            default_str = 'CURRENT_TIMESTAMP'
                        # PostgreSQL BIT columns require bit literals, not integer defaults.
                        elif data_type.upper().startswith('BIT'):
                            stripped = default_str.strip()
                            # Remove casts and outer parentheses from expressions like ((1))::int
                            if "::" in stripped:
                                stripped = stripped.split("::", 1)[0].strip()
                            while stripped.startswith("(") and stripped.endswith(")") and len(stripped) > 2:
                                stripped = stripped[1:-1].strip()
                            stripped = stripped.strip("'").strip('"')
                            if stripped in {'0', '1'}:
                                default_str = f"B'{stripped}'"
                        else:
                            # Quote plain string defaults like: DEFAULT active -> DEFAULT 'active'
                            # Keep numbers, NULL/TRUE/FALSE, already-quoted strings, and expressions/functions.
                            stripped = default_str.strip()
                            is_already_quoted = (
                                (stripped.startswith("'") and stripped.endswith("'"))
                                or (stripped.startswith('"') and stripped.endswith('"'))
                            )
                            is_numeric = stripped.replace(".", "", 1).replace("-", "", 1).isdigit()
                            is_keyword = stripped.upper() in {"NULL", "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}
                            looks_like_expression = "(" in stripped or ")" in stripped or "::" in stripped
                            if not (is_already_quoted or is_numeric or is_keyword or looks_like_expression):
                                escaped = stripped.replace("'", "''")
                                default_str = f"'{escaped}'"
                        
                        col_def += f' DEFAULT {default_str}'
                    
                    column_defs.append(col_def)
                
                # Add primary key constraint
                pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                if pk_cols:
                    pk_cols_quoted = ', '.join([f'"{c}"' for c in pk_cols])
                    column_defs.append(f'PRIMARY KEY ({pk_cols_quoted})')
                
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS "{table_info.name}" (
                        {', '.join(column_defs)}
                    )
                """
                cursor.execute(create_table_sql)
                
                # Create indexes
                for index in table_info.indexes:
                    index_name = index.get('name', '')
                    # Skip primary key indexes (already created as PRIMARY KEY constraint)
                    if index_name and ('_pkey' in index_name.lower() or 'primary' in index_name.lower()):
                        continue
                    if index['name'] not in [col.name for col in table_info.columns if col.is_primary_key]:
                        # Convert MySQL backticks to PostgreSQL double quotes
                        index_sql = index['definition']
                        # Replace MySQL backticks with PostgreSQL double quotes
                        index_sql = re.sub(r'`([^`]+)`', r'"\1"', index_sql)
                        try:
                            # Isolate index failures so one bad index does not abort the whole transaction.
                            cursor.execute("SAVEPOINT sp_create_index")
                            # Do not drop existing indexes: some are owned by constraints (e.g. unique),
                            # and dropping them will abort the transaction in PostgreSQL.
                            if index_name and index_sql.lstrip().upper().startswith("CREATE INDEX"):
                                index_sql = re.sub(
                                    r"^CREATE\s+INDEX\s+",
                                    "CREATE INDEX IF NOT EXISTS ",
                                    index_sql,
                                    flags=re.IGNORECASE,
                                )
                            cursor.execute(index_sql)
                            cursor.execute("RELEASE SAVEPOINT sp_create_index")
                        except Exception as idx_error:
                            cursor.execute("ROLLBACK TO SAVEPOINT sp_create_index")
                            cursor.execute("RELEASE SAVEPOINT sp_create_index")
                            logger.warning(f"  Could not create index '{index_name}' for '{table_info.name}': {idx_error}")
                
                # Defer foreign key creation until all tables are created.
                for col in table_info.columns:
                    if col.is_foreign_key and col.foreign_key_table and col.foreign_key_column:
                        pending_foreign_keys.append((table_info.name, col))

            for table_name, col in pending_foreign_keys:
                raw_fk_name = f"fk_{table_name}_{col.name}_{col.foreign_key_table}_{col.foreign_key_column}"
                if len(raw_fk_name) > 60:
                    digest = hashlib.md5(raw_fk_name.encode("utf-8")).hexdigest()[:8]
                    raw_fk_name = f"fk_{table_name}_{col.name}_{digest}"
                fk_name = raw_fk_name[:63]
                valid_fk_actions = {"CASCADE", "RESTRICT", "SET NULL", "SET DEFAULT", "NO ACTION"}

                try:
                    cursor.execute("SAVEPOINT sp_create_fk")
                    # Ensure both local and referenced columns still exist.
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = %s
                              AND column_name = %s
                        )
                        """,
                        (table_name, col.name),
                    )
                    local_col_exists = cursor.fetchone()[0]
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = %s
                              AND column_name = %s
                        )
                        """,
                        (col.foreign_key_table, col.foreign_key_column),
                    )
                    ref_col_exists = cursor.fetchone()[0]
                    if not local_col_exists or not ref_col_exists:
                        cursor.execute("RELEASE SAVEPOINT sp_create_fk")
                        logger.warning(
                            f"  Skipping foreign key '{table_name}.{col.name}' -> "
                            f"'{col.foreign_key_table}.{col.foreign_key_column}' (column/table missing)"
                        )
                        continue

                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM information_schema.table_constraints
                            WHERE table_schema = 'public'
                              AND table_name = %s
                              AND constraint_name = %s
                              AND constraint_type = 'FOREIGN KEY'
                        )
                        """,
                        (table_name, fk_name),
                    )
                    if cursor.fetchone()[0]:
                        cursor.execute("RELEASE SAVEPOINT sp_create_fk")
                        continue

                    fk_sql = f'''
                        ALTER TABLE "{table_name}"
                        ADD CONSTRAINT "{fk_name}"
                        FOREIGN KEY ("{col.name}")
                        REFERENCES "{col.foreign_key_table}" ("{col.foreign_key_column}")
                    '''
                    on_delete = (col.foreign_key_on_delete or "").strip().upper()
                    on_update = (col.foreign_key_on_update or "").strip().upper()
                    if on_delete in valid_fk_actions:
                        fk_sql += f" ON DELETE {on_delete}"
                    if on_update in valid_fk_actions:
                        fk_sql += f" ON UPDATE {on_update}"
                    cursor.execute(fk_sql)
                    cursor.execute("RELEASE SAVEPOINT sp_create_fk")
                except Exception as fk_error:
                    cursor.execute("ROLLBACK TO SAVEPOINT sp_create_fk")
                    cursor.execute("RELEASE SAVEPOINT sp_create_fk")
                    logger.warning(f"  Could not create foreign key for '{table_name}.{col.name}': {fk_error}")
            
            connection.commit()
            logger.info(f"Schema applied successfully to {schema.database_name}")
        
        except Exception as e:
            connection.rollback()
            logger.error(f"Error applying schema: {e}")
            raise
        finally:
            cursor.close()
