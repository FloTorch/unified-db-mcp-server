"""SQLite database connector"""
import logging
import sqlite3
from typing import Dict, Any, List

from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class SQLiteConnector(DatabaseConnector):
    """SQLite connector (basic schema support)"""

    def connect(self, credentials: Dict[str, Any]):
        """Connect to SQLite using sqlite3"""
        database_path = credentials.get("database_path")
        if not database_path:
            raise ValueError("database_path is required for SQLite connections")

        logger.info(f"Connecting to SQLite database: {database_path}")
        try:
            # sqlite3.connect creates the database file if it doesn't exist
            connection = sqlite3.connect(database_path)
            # Enable foreign key constraints
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.close()
            return connection
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise

    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """
        Extract basic schema from SQLite.
        - Tables
        - Columns (name, data type, nullability, default)
        """
        cursor = connection.cursor()

        try:
            # Get database name from path
            database_path = credentials.get("database_path", "") if credentials else ""
            db_name = database_path.split("/")[-1].split("\\")[-1] if database_path else "sqlite_db"

            # Get all tables (excluding sqlite system tables)
            cursor.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]

            table_infos: List[TableInfo] = []

            for table_name in tables:
                # Get table schema using PRAGMA table_info
                cursor.execute(f"PRAGMA table_info([{table_name}])")
                columns_data = cursor.fetchall()

                columns: List[ColumnInfo] = []
                for col in columns_data:
                    # PRAGMA table_info returns:
                    # (cid, name, type, notnull, default_value, pk)
                    col_name = col[1]
                    col_type = col[2] or "TEXT"  # Default to TEXT if no type specified
                    is_not_null = col[3] == 1
                    default_value = col[4]
                    is_pk = col[5] == 1

                    # Parse type for length/precision if present
                    character_maximum_length = None
                    numeric_precision = None
                    numeric_scale = None

                    # Try to extract length from type string (e.g., VARCHAR(255))
                    import re
                    type_match = re.match(r'(\w+)\((\d+)(?:,(\d+))?\)', col_type.upper())
                    if type_match:
                        base_type = type_match.group(1)
                        if base_type in ['VARCHAR', 'CHAR', 'TEXT']:
                            character_maximum_length = int(type_match.group(2))
                        elif base_type in ['DECIMAL', 'NUMERIC']:
                            numeric_precision = int(type_match.group(2))
                            if type_match.group(3):
                                numeric_scale = int(type_match.group(3))

                    # Check for foreign key
                    cursor.execute(f"PRAGMA foreign_key_list([{table_name}])")
                    fk_list = cursor.fetchall()
                    is_fk = False
                    fk_table = None
                    fk_column = None
                    
                    # PRAGMA foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
                    fk_on_delete = None
                    fk_on_update = None
                    for fk in fk_list:
                        if fk[3] == col_name:  # fk[3] is the 'from' column (local column)
                            is_fk = True
                            fk_table = fk[2]  # fk[2] is the referenced table
                            fk_column = fk[4] if fk[4] else 'id'  # fk[4] is the referenced column, default to 'id'
                            fk_on_delete = fk[6] if fk[6] else 'RESTRICT'  # fk[6] is on_delete action
                            fk_on_update = fk[5] if fk[5] else 'RESTRICT'  # fk[5] is on_update action
                            break
                    
                    # Check for UNIQUE constraint
                    is_unique = False
                    cursor.execute(f"PRAGMA index_list([{table_name}])")
                    index_list = cursor.fetchall()
                    for idx in index_list:
                        idx_name = idx[1]
                        if idx[2] == 1:  # unique index
                            cursor.execute(f"PRAGMA index_info([{idx_name}])")
                            idx_info = cursor.fetchall()
                            if idx_info and len(idx_info) == 1 and idx_info[0][2] == col_name:
                                is_unique = True
                                break

                    column_info = ColumnInfo(
                        name=col_name,
                        data_type=col_type,
                        is_nullable=not is_not_null,
                        default_value=default_value,
                        character_maximum_length=character_maximum_length,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        is_primary_key=is_pk,
                        is_foreign_key=is_fk,
                        foreign_key_table=fk_table,
                        foreign_key_column=fk_column,
                        foreign_key_on_delete=fk_on_delete,
                        foreign_key_on_update=fk_on_update,
                        is_unique=is_unique,
                    )
                    columns.append(column_info)

                table_infos.append(TableInfo(
                    name=table_name,
                    columns=columns,
                    indexes=[],  # Could be extended to extract indexes
                ))

            return SchemaInfo(
                database_type="sqlite",
                database_name=db_name,
                tables=table_infos,
            )
        finally:
            cursor.close()
    
    def _sort_tables_by_dependencies(self, tables):
        """Sort tables so parent tables (referenced by foreign keys) are created first"""
        # Build dependency graph
        table_names = {t.name for t in tables}
        dependencies = {}  # table_name -> set of tables it depends on
        
        for table in tables:
            deps = set()
            for col in table.columns:
                if col.is_foreign_key and col.foreign_key_table and col.foreign_key_table in table_names:
                    deps.add(col.foreign_key_table)
            dependencies[table.name] = deps
        
        # Topological sort
        sorted_list = []
        remaining = set(table.name for table in tables)
        
        while remaining:
            # Find tables with no dependencies
            ready = [name for name in remaining if not dependencies[name] & remaining]
            
            if not ready:
                # Circular dependency or missing table - just add remaining tables
                sorted_list.extend([t for t in tables if t.name in remaining])
                break
            
            # Add ready tables to sorted list
            for name in ready:
                sorted_list.append(next(t for t in tables if t.name == name))
                remaining.remove(name)
        
        return sorted_list

    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """
        Apply schema to SQLite.
        This implementation:
        - Drops existing tables with the same name
        - Creates tables with columns and primary keys
        - Creates foreign key constraints
        """
        cursor = connection.cursor()

        try:
            # Sort tables by foreign key dependencies (parent tables first)
            sorted_tables = self._sort_tables_by_dependencies(schema.tables)
            
            for table_info in sorted_tables:
                # Drop existing table if it exists
                logger.info(f"Applying table '{table_info.name}' to SQLite")
                table_name_quoted = f'"{table_info.name}"'
                drop_sql = f"DROP TABLE IF EXISTS {table_name_quoted}"
                cursor.execute(drop_sql)

                column_defs = []
                for col in table_info.columns:
                    # Normalize data type from other database types to SQLite
                    raw_type = (col.data_type or "TEXT").strip()
                    upper_type = raw_type.upper()

                    # SQLite type affinity mapping
                    # SQLite uses dynamic typing but we map to standard types
                    if upper_type in ["UUID", "UNIQUEIDENTIFIER"]:
                        data_type = "TEXT"
                    elif upper_type in ["BOOLEAN", "BOOL", "BIT"]:
                        data_type = "INTEGER"
                    elif upper_type in ["TEXT", "LONGTEXT", "CLOB"]:
                        data_type = "TEXT"
                    elif upper_type in ["JSON", "JSONB"]:
                        data_type = "TEXT"
                    elif upper_type in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"]:
                        data_type = "INTEGER"
                    elif upper_type in ["REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION"]:
                        data_type = "REAL"
                    elif upper_type in ["DECIMAL", "NUMERIC"]:
                        data_type = "NUMERIC"
                    elif "TIMESTAMP" in upper_type or "DATETIME" in upper_type or "DATE" in upper_type:
                        data_type = "TEXT"  # SQLite doesn't have native datetime, use TEXT
                    elif upper_type in ["BLOB", "BINARY", "VARBINARY"]:
                        data_type = "BLOB"
                    elif upper_type.startswith("VARCHAR") or upper_type.startswith("CHAR"):
                        # Preserve VARCHAR(n) or CHAR(n) format
                        data_type = raw_type
                    else:
                        # Use the type as-is, SQLite will handle it
                        data_type = raw_type

                    # Add length/precision if appropriate and not already in type
                    if (
                        col.character_maximum_length
                        and "(" not in data_type.upper()
                        and data_type.upper() in ["VARCHAR", "CHAR", "TEXT"]
                    ):
                        data_type = f"{data_type}({col.character_maximum_length})"
                    elif (
                        col.numeric_precision
                        and col.numeric_scale is not None
                        and "(" not in data_type.upper()
                        and data_type.upper() in ["DECIMAL", "NUMERIC"]
                    ):
                        data_type = f"{data_type}({col.numeric_precision},{col.numeric_scale})"

                    # Check if this is an INTEGER PRIMARY KEY with sequence (needs AUTOINCREMENT)
                    # In SQLite, AUTOINCREMENT only works when PRIMARY KEY is in column definition
                    needs_autoincrement = False
                    is_single_pk_integer = False
                    pk_cols_count = len([c for c in table_info.columns if c.is_primary_key])
                    
                    if col.is_primary_key and data_type.upper() in ['INTEGER', 'INT'] and pk_cols_count == 1:
                        # Only single-column INTEGER PRIMARY KEY can use AUTOINCREMENT
                        is_single_pk_integer = True
                        if col.default_value:
                            default_val = str(col.default_value)
                            default_val_lower = default_val.lower()
                            is_sequence = 'nextval' in default_val_lower or '_seq' in default_val_lower
                            if is_sequence:
                                needs_autoincrement = True
                    
                    # Build column definition
                    if is_single_pk_integer and needs_autoincrement:
                        # For INTEGER PRIMARY KEY AUTOINCREMENT, include PRIMARY KEY in column definition
                        # SQLite syntax: INTEGER PRIMARY KEY AUTOINCREMENT (all in column definition)
                        col_def = f'"{col.name}" {data_type} PRIMARY KEY AUTOINCREMENT'
                        if not col.is_nullable:
                            col_def += " NOT NULL"
                        logger.debug(f"  Using INTEGER PRIMARY KEY AUTOINCREMENT for column '{col.name}' in table '{table_info.name}'")
                    else:
                        # Regular column definition
                        col_def = f'"{col.name}" {data_type}'
                        if not col.is_nullable:
                            col_def += " NOT NULL"

                    # Handle default values - skip PostgreSQL sequences (they become AUTOINCREMENT for INTEGER PRIMARY KEY)
                    if col.default_value:
                        default_val = str(col.default_value)
                        default_val_lower = default_val.lower()
                        
                        # Check if it's a PostgreSQL sequence (nextval)
                        is_sequence = 'nextval' in default_val_lower or '_seq' in default_val_lower
                        
                        if is_sequence:
                            # PostgreSQL sequence - already handled with AUTOINCREMENT for PK, skip DEFAULT
                            if not needs_autoincrement:
                                logger.debug(f"  Skipping PostgreSQL sequence default for column '{col.name}' in table '{table_info.name}'")
                        elif default_val_lower.startswith(("current_", "now()")):
                            # PostgreSQL functions like CURRENT_TIMESTAMP, NOW()
                            if 'timestamp' in default_val_lower or 'now()' in default_val_lower:
                                # SQLite doesn't support CURRENT_TIMESTAMP in DEFAULT, skip it
                                logger.debug(f"  Skipping CURRENT_TIMESTAMP default for column '{col.name}' in table '{table_info.name}'")
                            else:
                                col_def += f" DEFAULT {default_val}"
                        elif isinstance(col.default_value, str) and not default_val.upper().startswith(("CURRENT_", "(", "NEXTVAL")):
                            # Simple string default - escape single quotes
                            escaped = default_val.replace("'", "''")
                            col_def += f" DEFAULT '{escaped}'"
                        elif not is_sequence:
                            # Numeric or other literal defaults
                            col_def += f" DEFAULT {default_val}"

                    column_defs.append(col_def)

                # Primary key constraint (skip if already in column definition with AUTOINCREMENT)
                pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                if pk_cols:
                    # Check if PRIMARY KEY is already in column definition (for AUTOINCREMENT case)
                    # Look for column definitions that already contain "PRIMARY KEY AUTOINCREMENT"
                    pk_in_col_def = any('PRIMARY KEY AUTOINCREMENT' in col_def.upper() for col_def in column_defs)
                    
                    if not pk_in_col_def:
                        pk_cols_sql = ", ".join(f'"{c}"' for c in pk_cols)
                        column_defs.append(f"PRIMARY KEY ({pk_cols_sql})")

                # UNIQUE constraints (column-level)
                for col in table_info.columns:
                    if col.is_unique and not col.is_primary_key:
                        column_defs.append(f'UNIQUE ("{col.name}")')
                
                # Foreign key constraints
                for col in table_info.columns:
                    if col.is_foreign_key and col.foreign_key_table and col.foreign_key_column:
                        fk_def = f'FOREIGN KEY ("{col.name}") REFERENCES "{col.foreign_key_table}" ("{col.foreign_key_column}")'
                        if col.foreign_key_on_delete:
                            fk_def += f' ON DELETE {col.foreign_key_on_delete}'
                        if col.foreign_key_on_update:
                            fk_def += f' ON UPDATE {col.foreign_key_on_update}'
                        column_defs.append(fk_def)
                
                # CHECK constraints from table constraints
                for constraint in table_info.constraints or []:
                    if constraint.get('type') == 'CHECK':
                        column_defs.append(f"CHECK ({constraint.get('definition', '')})")

                create_sql = f'CREATE TABLE "{table_info.name}" ({", ".join(column_defs)})'
                cursor.execute(create_sql)

            connection.commit()
            logger.info(f"Schema applied successfully to SQLite database '{schema.database_name}'")
        except Exception as e:
            # SQLite doesn't support rollback in the same way, but we can try
            try:
                connection.rollback()
            except:
                pass
            logger.error(f"Error applying schema to SQLite: {e}")
            raise
        finally:
            cursor.close()
    
    def _sort_tables_by_dependencies(self, tables):
        """Sort tables so parent tables (referenced by foreign keys) are created first"""
        # Build dependency graph
        table_names = {t.name for t in tables}
        dependencies = {}  # table_name -> set of tables it depends on
        
        for table in tables:
            deps = set()
            for col in table.columns:
                if col.is_foreign_key and col.foreign_key_table and col.foreign_key_table in table_names:
                    deps.add(col.foreign_key_table)
            dependencies[table.name] = deps
        
        # Topological sort
        sorted_list = []
        remaining = set(table.name for table in tables)
        
        while remaining:
            # Find tables with no dependencies
            ready = [name for name in remaining if not dependencies[name] & remaining]
            
            if not ready:
                # Circular dependency or missing table - just add remaining tables
                sorted_list.extend([t for t in tables if t.name in remaining])
                break
            
            # Add ready tables to sorted list
            for name in ready:
                sorted_list.append(next(t for t in tables if t.name == name))
                remaining.remove(name)
        
        return sorted_list