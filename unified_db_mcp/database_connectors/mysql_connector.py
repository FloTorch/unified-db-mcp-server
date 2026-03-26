"""MySQL database connector"""
import logging
import re
import mysql.connector
from typing import Dict, Any, List
from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)


class MySQLConnector(DatabaseConnector):
    """MySQL connector"""
    
    def connect(self, credentials: Dict[str, Any]):
        """Connect to MySQL"""
        # Ensure default values for MySQL connection options
        conn_params = {
            'host': credentials.get('host'),
            'port': credentials.get('port', 3306),
            'database': credentials.get('database'),
            'user': credentials.get('user'),
            'password': credentials.get('password'),
            'use_pure': credentials.get('use_pure', True),  # Default to True
            'ssl_disabled': credentials.get('ssl_disabled', True)  # Default to True
        }
        
        logger.info(f"Connecting to MySQL: {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}")
        return mysql.connector.connect(**conn_params)
    
    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """Extract schema from MySQL"""
        cursor = connection.cursor(dictionary=True)
        
        try:
            # Get database name
            cursor.execute("SELECT DATABASE() as db_name")
            db_name = cursor.fetchone()['db_name']
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            
            table_infos = []
            for table_name in tables:
                # Get columns
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns_data = cursor.fetchall()
                
                # Get all foreign keys for this table at once (more efficient than per-column queries)
                cursor.execute("""
                    SELECT 
                        kcu.COLUMN_NAME,
                        kcu.REFERENCED_TABLE_NAME,
                        kcu.REFERENCED_COLUMN_NAME,
                        rc.DELETE_RULE,
                        rc.UPDATE_RULE
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                    JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
                    WHERE kcu.TABLE_SCHEMA = DATABASE()
                    AND kcu.TABLE_NAME = %s
                    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                """, (table_name,))
                
                # Build a map of column_name -> foreign key info
                fk_map = {}
                for fk_row in cursor.fetchall():
                    col_name = fk_row[0]
                    fk_map[col_name] = {
                        'table': fk_row[1],
                        'column': fk_row[2],
                        'on_delete': fk_row[3] if fk_row[3] else 'RESTRICT',
                        'on_update': fk_row[4] if fk_row[4] else 'RESTRICT'
                    }
                
                columns = []
                for col in columns_data:
                    # Parse data type
                    data_type = col['Type']
                    is_nullable = col['Null'] == 'YES'
                    default_value = col['Default']
                    is_pk = col['Key'] == 'PRI'
                    
                    # Extract length/precision from type string
                    char_length = None
                    numeric_precision = None
                    numeric_scale = None
                    
                    if '(' in data_type:
                        # Keep the full type string for ENUM/SET, otherwise MariaDB/MySQL
                        # will generate invalid SQL like `status ENUM DEFAULT 'active'`.
                        if data_type.upper().startswith('ENUM(') or data_type.upper().startswith('SET('):
                            base_type_upper = data_type.split('(', 1)[0].upper().strip()
                            # For ENUM/SET we intentionally do NOT parse params/length.
                            # Leave `data_type` unchanged so it stays like ENUM('a','b').
                            data_type = data_type  # explicit no-op for readability
                        else:
                            type_parts = data_type.split('(', 1)
                            base_type = type_parts[0]
                            params = type_parts[1].rstrip(')')
                            base_type_upper = base_type.upper().strip()

                            # Only parse numeric precision/scale for numeric types.
                            if base_type_upper in ['DECIMAL', 'NUMERIC', 'FIXED', 'DOUBLE', 'FLOAT', 'REAL']:
                                if ',' in params:
                                    parts = [p.strip() for p in params.split(',')]
                                    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                                        numeric_precision, numeric_scale = int(parts[0]), int(parts[1])
                                elif params.strip().isdigit():
                                    numeric_precision = int(params.strip())
                            # Parse character length only for char/varchar-like types.
                            elif base_type_upper in ['CHAR', 'VARCHAR', 'BINARY', 'VARBINARY'] and params.strip().isdigit():
                                char_length = int(params.strip())

                            # For other types (e.g., types without length/precision we don't understand),
                            # keep only the base type to avoid breaking SQL generation.
                            data_type = base_type
                    
                    # Get foreign key information from map
                    col_name = col['Field']
                    fk_info = fk_map.get(col_name)
                    
                    column_info = ColumnInfo(
                        name=col_name,
                        data_type=data_type,
                        is_nullable=is_nullable,
                        default_value=str(default_value) if default_value is not None else None,
                        character_maximum_length=char_length,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        is_primary_key=is_pk,
                        is_foreign_key=fk_info is not None,
                        foreign_key_table=fk_info['table'] if fk_info else None,
                        foreign_key_column=fk_info['column'] if fk_info else None,
                        foreign_key_on_delete=fk_info['on_delete'] if fk_info else None,
                        foreign_key_on_update=fk_info['on_update'] if fk_info else None
                    )
                    columns.append(column_info)
                
                # Get indexes
                cursor.execute(f"SHOW INDEXES FROM `{table_name}`")
                indexes_data = cursor.fetchall()
                indexes = []
                index_names = set()
                for idx in indexes_data:
                    if idx['Key_name'] not in index_names and idx['Key_name'] != 'PRIMARY':
                        indexes.append({
                            'name': idx['Key_name'],
                            'definition': f"CREATE INDEX {idx['Key_name']} ON `{table_name}` ({idx['Column_name']})"
                        })
                        index_names.add(idx['Key_name'])
                
                table_infos.append(TableInfo(
                    name=table_name,
                    columns=columns,
                    indexes=indexes
                ))
            
            return SchemaInfo(
                database_type='mysql',
                database_name=db_name,
                tables=table_infos
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
    
    def _get_reverse_dependency_order(self, cursor, table_names):
        """Get drop order: tables that reference others (child tables) first, then parent tables"""
        # Build reverse dependency map: which tables are referenced by which tables
        referenced_by = {}  # parent_table -> set of child tables that reference it
        
        for table_name in table_names:
            # Query existing foreign keys in the database
            cursor.execute("""
                SELECT 
                    TABLE_NAME,
                    REFERENCED_TABLE_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = %s
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (table_name,))
            
            for row in cursor.fetchall():
                child_table = row[0]
                parent_table = row[1]
                if parent_table in table_names:  # Only consider tables we're managing
                    if parent_table not in referenced_by:
                        referenced_by[parent_table] = set()
                    referenced_by[parent_table].add(child_table)
        
        # Topological sort for dropping: child tables first
        # Tables that are NOT referenced by any other table can be dropped first
        drop_order = []
        remaining = set(table_names)
        
        while remaining:
            # Find tables that are not referenced by any remaining table
            ready = [name for name in remaining 
                    if name not in referenced_by or 
                    not (referenced_by[name] & remaining)]
            
            if not ready:
                # Circular dependency - just drop remaining tables
                drop_order.extend(remaining)
                break
            
            # Add ready tables to drop order
            drop_order.extend(ready)
            remaining -= set(ready)
        
        return drop_order
    
    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """Apply schema to MySQL database"""
        cursor = connection.cursor()
        
        try:
            # Sort tables by foreign key dependencies (parent tables first for creation)
            sorted_tables = self._sort_tables_by_dependencies(schema.tables)
            
            # Drop all existing tables first in correct order (child tables first, then parent tables)
            # Query the database to find actual foreign key relationships
            tables_to_drop = []
            for table_info in sorted_tables:
                cursor.execute(f"SHOW TABLES LIKE '{table_info.name}'")
                if cursor.fetchone() is not None:
                    tables_to_drop.append(table_info.name)
            
            if tables_to_drop:
                # Get correct drop order based on actual foreign keys in the database
                drop_order = self._get_reverse_dependency_order(cursor, tables_to_drop)
                for table_name in drop_order:
                    logger.info(f"  Dropping existing table '{table_name}'...")
                    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            
            # Now create tables in correct order (parent tables first)
            for table_info in sorted_tables:
                
                # Build CREATE TABLE statement
                column_defs = []

                def _normalize_default_value(raw_default: str) -> str:
                    """
                    Normalize cross-db default expressions to MySQL/MariaDB-friendly values.
                    Examples:
                      ((1)) -> 1
                      (('active')) / (''active'') -> active
                      (getdate()) -> CURRENT_TIMESTAMP
                    """
                    if raw_default is None:
                        return raw_default

                    val = str(raw_default).strip()

                    # Remove PostgreSQL style casts: 'active'::text
                    if "::" in val:
                        val = re.sub(r"::.*$", "", val).strip()

                    # Unwrap repeated outer parentheses: ((1)) -> 1, (getdate()) -> getdate()
                    while val.startswith("(") and val.endswith(")"):
                        inner = val[1:-1].strip()
                        # Stop if removing would empty out
                        if not inner:
                            break
                        val = inner

                    # Normalize doubled single-quotes wrappers from SQL Server/Postgres exports.
                    # ''active'' -> active
                    if val.startswith("''") and val.endswith("''") and len(val) >= 4:
                        val = val[2:-2]

                    # Remove single/double quote wrappers once.
                    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
                        val = val[1:-1]

                    # Normalize common datetime functions.
                    low = val.lower().strip()
                    if low in {"getdate()", "getdate", "now()", "now", "current_timestamp()", "localtimestamp"}:
                        return "CURRENT_TIMESTAMP"
                    if low in {"current_date()", "current_date"}:
                        return "CURRENT_DATE"
                    if low in {"current_time()", "current_time"}:
                        return "CURRENT_TIME"

                    return val

                for col in table_info.columns:
                    # MySQL doesn't allow TEXT/BLOB as primary keys - convert to VARCHAR
                    data_type = col.data_type
                    
                    # MySQL doesn't support UUID type - convert to CHAR(36)
                    if data_type.upper() == 'UUID':
                        data_type = 'CHAR(36)'
                        logger.debug(f"  Converting UUID type to CHAR(36) for column '{col.name}' in table '{table_info.name}'")
                    
                    # MySQL doesn't support JSONB type - convert to JSON (MySQL 5.7+) or TEXT
                    if data_type.upper() == 'JSONB':
                        data_type = 'JSON'  # MySQL 5.7+ supports JSON, fallback to TEXT for older versions
                        logger.debug(f"  Converting JSONB type to JSON for column '{col.name}' in table '{table_info.name}'")
                    
                    # MySQL doesn't support PostgreSQL timestamp with time zone syntax
                    data_type_upper = data_type.upper()
                    if 'TIMESTAMP WITH TIME ZONE' in data_type_upper or 'TIMESTAMP WITHOUT TIME ZONE' in data_type_upper:
                        data_type = 'DATETIME'
                        data_type_upper = 'DATETIME'
                        logger.debug(f"  Converting PostgreSQL timestamp type to DATETIME for column '{col.name}' in table '{table_info.name}'")
                    
                    # Check if column is part of a key (primary key or foreign key)
                    is_key_column = col.is_primary_key or (col.is_foreign_key and col.foreign_key_table)
                    
                    if col.is_primary_key:
                        # Check if it's a TEXT/BLOB type that can't be used as primary key
                        type_upper = data_type.upper()
                        if type_upper in ['TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB']:
                            # Convert to VARCHAR(255) for primary keys
                            # Use 191 for better compatibility with utf8mb4 (255 * 4 bytes = 1020 > 767 max key length)
                            data_type = 'VARCHAR(191)'
                            data_type_upper = 'VARCHAR(191)'
                            logger.debug(f"  Converting TEXT/BLOB primary key '{col.name}' to VARCHAR(191) for MySQL compatibility")
                    
                    # For key columns, ensure VARCHAR/CHAR length doesn't exceed 191 for utf8mb4 compatibility
                    # MySQL/MariaDB max key length is 3072 bytes, but with utf8mb4 (4 bytes per char), 
                    # 191 chars = 764 bytes, which is safe for the 767-byte limit per column
                    # Update data_type_upper to reflect current data_type
                    data_type_upper = data_type.upper()
                    if is_key_column:
                        # Extract base type (VARCHAR, CHAR, etc.)
                        base_type_match = re.match(r'^(\w+)', data_type, re.IGNORECASE)
                        if base_type_match:
                            base_type = base_type_match.group(1).upper()
                            if base_type in ['VARCHAR', 'CHAR']:
                                # Check if data_type already has a length (e.g., VARCHAR(5000))
                                length_match = re.match(r'^(\w+)\((\d+)\)', data_type, re.IGNORECASE)
                                if length_match:
                                    base_type_name, existing_length = length_match.groups()
                                    existing_length = int(existing_length)
                                    if existing_length > 191:
                                        data_type = f'{base_type_name}(191)'
                                        data_type_upper = data_type.upper()
                                        logger.debug(f"  Truncating key column '{col.name}' length from {existing_length} to 191 for MySQL key length limit")
                                # Also check character_maximum_length if data_type doesn't have length yet
                                elif col.character_maximum_length and col.character_maximum_length > 191:
                                    # Will be handled later when building col_def, but update data_type now
                                    data_type = f'{base_type}(191)'
                                    data_type_upper = data_type.upper()
                                    logger.debug(f"  Setting key column '{col.name}' length to 191 (from {col.character_maximum_length}) for MySQL key length limit")
                    
                    # Build column definition
                    # Ensure we always have the correct uppercase base type for the CURRENT column.
                    # (Some earlier logic updates data_type_upper only for key columns.)
                    data_type_upper = re.match(r'^(\w+)', data_type, re.IGNORECASE).group(1).upper() if data_type else ''
                    col_def = f'`{col.name}` {data_type}'
                    
                    # Add length/precision only if data_type doesn't already have it
                    if '(' not in data_type.upper():
                        # Extract base type to check if it's VARCHAR/CHAR (including NVARCHAR, NCHAR, etc.)
                        base_type_match = re.match(r'^(\w+)', data_type, re.IGNORECASE)
                        base_type_for_check = base_type_match.group(1).upper() if base_type_match else data_type.upper()
                        
                        if col.character_maximum_length and base_type_for_check in ['VARCHAR', 'CHAR', 'NVARCHAR', 'NCHAR']:
                            # For key columns, limit length to 191
                            max_length = col.character_maximum_length
                            if is_key_column and max_length > 191:
                                max_length = 191
                                logger.debug(f"  Truncating key column '{col.name}' length from {col.character_maximum_length} to 191 for MySQL key length limit")
                            col_def += f'({max_length})'
                        elif col.numeric_precision and col.numeric_scale and data_type.upper() in ['DECIMAL', 'NUMERIC']:
                            col_def += f'({col.numeric_precision},{col.numeric_scale})'
                        elif col.numeric_precision and data_type.upper() in ['DECIMAL', 'NUMERIC']:
                            col_def += f'({col.numeric_precision},0)'
                    
                    if not col.is_nullable:
                        col_def += ' NOT NULL'
                    
                    # Handle default values - convert PostgreSQL sequences to MySQL AUTO_INCREMENT
                    # MySQL/MariaDB does not support DEFAULT on TEXT/BLOB types.
                    # MySQL/MariaDB does not support DEFAULT on TEXT/BLOB types.
                    # Be defensive: match by substring on the full data type.
                    full_type_upper = (str(data_type) or "").upper()
                    is_text_or_blob = ("TEXT" in full_type_upper) or ("BLOB" in full_type_upper)

                    def _extract_enum_set_values(type_str: str) -> List[str]:
                        """Extract allowed values from ENUM('a','b') / SET('a','b')."""
                        if not type_str:
                            return []
                        type_str_upper = type_str.upper()
                        if not re.match(r'^(ENUM|SET)\s*\(', type_str_upper):
                            return []
                        # Extract quoted values inside (...)
                        inside = type_str[type_str.find("(") + 1 : type_str.rfind(")")]
                        raw_vals = re.findall(r"'((?:''|[^'])*)'", inside)
                        return [v.replace("''", "'") for v in raw_vals]

                    skip_default = False
                    default_str_unquoted = None
                    if col.default_value:
                        # MySQL/MariaDB does not support DEFAULT on TEXT/BLOB types.
                        if is_text_or_blob:
                            skip_default = True
                        # MariaDB rejects invalid DEFAULT values for ENUM/SET.
                        elif re.match(r'^(ENUM|SET)\s*\(', full_type_upper):
                            default_str = str(col.default_value).strip()
                            if (default_str.startswith("'") and default_str.endswith("'")) or (
                                default_str.startswith('"') and default_str.endswith('"')
                            ):
                                default_str_unquoted = default_str[1:-1]
                            else:
                                default_str_unquoted = default_str
                            allowed = _extract_enum_set_values(str(data_type))
                            if allowed and default_str_unquoted not in allowed:
                                skip_default = True

                    if col.default_value and not skip_default:
                        default_str = _normalize_default_value(col.default_value)
                        # Remove surrounding quotes if present
                        if (default_str.startswith("'") and default_str.endswith("'")) or \
                           (default_str.startswith('"') and default_str.endswith('"')):
                            default_str = default_str[1:-1]

                        # Normalize boolean defaults coming from Postgres like:
                        # false, true, 'false', 'true', '''false''::boolean'
                        # MariaDB generally expects 0/1 for boolean-like columns.
                        default_str_lower = default_str.lower().strip()
                        is_bool_like_type = (
                            data_type_upper in {"BOOLEAN", "BOOL", "BIT", "TINYINT", "INTEGER", "INT", "SMALLINT", "BIGINT"}
                            or "TINYINT(1)" in full_type_upper
                            or "BOOLEAN" in full_type_upper
                            or "BOOL" in full_type_upper
                        )
                        if is_bool_like_type and default_str_lower in {"false", "0", "b'0'"}:
                            default_str = "0"
                        elif is_bool_like_type and default_str_lower in {"true", "1", "b'1'"}:
                            default_str = "1"
                        
                        # Check if it's a PostgreSQL sequence (nextval)
                        if 'nextval' in default_str.lower():
                            # Convert to MySQL AUTO_INCREMENT (only for primary key integer columns)
                            if col.is_primary_key and data_type.upper() in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']:
                                col_def += ' AUTO_INCREMENT'
                            # Don't add DEFAULT for sequences - AUTO_INCREMENT handles it
                        # Handle timestamp defaults - MySQL needs CURRENT_TIMESTAMP for timestamps
                        # Use converted data_type, not original col.data_type
                        elif data_type_upper in ['TIMESTAMP', 'DATETIME']:
                            # Normalize PostgreSQL timestamp defaults to MySQL CURRENT_TIMESTAMP
                            default_lower = default_str.lower()
                            if any(x in default_lower for x in ['current_timestamp', 'now()', 'now(', 'localtimestamp']):
                                col_def += ' DEFAULT CURRENT_TIMESTAMP'
                            elif default_str.upper() in ['CURRENT_TIMESTAMP', 'NOW()', 'LOCALTIMESTAMP']:
                                col_def += ' DEFAULT CURRENT_TIMESTAMP'
                            elif default_str.upper() == 'CURRENT_DATE':
                                col_def += ' DEFAULT CURRENT_DATE'
                            elif default_str.upper() == 'CURRENT_TIME':
                                col_def += ' DEFAULT CURRENT_TIME'
                            elif default_str:
                                # Other timestamp defaults - quote if it's a literal value
                                escaped = default_str.replace("'", "''")
                                col_def += f" DEFAULT '{escaped}'"
                        elif isinstance(col.default_value, str) and not default_str.replace('.', '').replace('-', '').isdigit():
                            # String defaults - escape single quotes
                            escaped = default_str.replace("'", "''")
                            col_def += f" DEFAULT '{escaped}'"
                        else:
                            # Numeric or other literal defaults
                            col_def += f' DEFAULT {default_str}'
                    
                    column_defs.append(col_def)
                
                # Add primary key constraint
                pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                if pk_cols:
                    column_defs.append(f'PRIMARY KEY (`{"`, `".join(pk_cols)}`)')
                
                # Add foreign key constraints
                fk_constraints = []
                for col in table_info.columns:
                    if col.is_foreign_key and col.foreign_key_table and col.foreign_key_column:
                        fk_name = f'fk_{table_info.name}_{col.name}'
                        on_delete = col.foreign_key_on_delete or 'RESTRICT'
                        on_update = col.foreign_key_on_update or 'RESTRICT'
                        fk_constraints.append(
                            f'CONSTRAINT `{fk_name}` FOREIGN KEY (`{col.name}`) '
                            f'REFERENCES `{col.foreign_key_table}` (`{col.foreign_key_column}`) '
                            f'ON DELETE {on_delete} ON UPDATE {on_update}'
                        )
                
                if fk_constraints:
                    column_defs.extend(fk_constraints)
                
                create_table_sql = f"""
                    CREATE TABLE `{table_info.name}` (
                        {', '.join(column_defs)}
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
                try:
                    cursor.execute(create_table_sql)
                except Exception as e:
                    # Log the problematic SQL for debugging
                    logger.error(f"  Failed to create table '{table_info.name}': {e}")
                    # Emit SQL at error level so it's visible even when DEBUG isn't enabled.
                    logger.error(f"  SQL: {create_table_sql}")
                    # Log primary key columns for debugging
                    pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                    if pk_cols:
                        logger.debug(f"  Primary key columns: {pk_cols}")
                        for pk_col in pk_cols:
                            col_info = next((c for c in table_info.columns if c.name == pk_col), None)
                            if col_info:
                                logger.debug(f"    - {pk_col}: {col_info.data_type}, length: {col_info.character_maximum_length}")
                    raise
                logger.debug(f"  Created table '{table_info.name}' with {len(table_info.columns)} columns")
                
                # Create indexes
                for index in table_info.indexes:
                    try:
                        # Convert PostgreSQL index syntax to MySQL
                        index_sql = index['definition']
                        # Remove PostgreSQL-specific syntax: USING btree
                        index_sql = index_sql.replace(' USING btree', '').replace(' USING BTREE', '')
                        # Remove PostgreSQL schema references (public.)
                        index_sql = re.sub(r'public\.', '', index_sql, flags=re.IGNORECASE)
                        # Replace PostgreSQL double quotes with MySQL backticks
                        index_sql = re.sub(r'"([^"]+)"', r'`\1`', index_sql)
                        cursor.execute(index_sql)
                    except Exception as idx_error:
                        logger.warning(f"  Could not create index for '{table_info.name}': {idx_error}")
            
            connection.commit()
            logger.info(f"Schema applied successfully to {schema.database_name}")
        
        except Exception as e:
            connection.rollback()
            logger.error(f"Error applying schema: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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