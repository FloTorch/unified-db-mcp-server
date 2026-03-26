"""
Type Converter System for Database Migrations.
Supports bidirectional conversion between: Supabase, MySQL, PostgreSQL, MongoDB, SQL Server, SQLite, Cassandra.
"""
from typing import Optional


class TypeConverter:
    """Centralized type conversion system for all database pairs"""
    
    @staticmethod
    def convert_type(
        source_db: str,
        target_db: str,
        data_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """
        Convert data type from source database to target database.
        
        Args:
            source_db: Source database type ('supabase', 'mysql', 'postgresql')
            target_db: Target database type ('supabase', 'mysql', 'postgresql')
            data_type: Source data type
            char_length: Character length (for VARCHAR/CHAR)
            precision: Numeric precision (for DECIMAL)
            scale: Numeric scale (for DECIMAL)
        
        Returns:
            Target database data type string
        """
        # Normalize database names
        source = source_db.lower().strip()
        target = target_db.lower().strip()

        # Normalize MariaDB <-> MySQL (treat MariaDB as MySQL for type conversion)
        if source == "mariadb":
            source = "mysql"
        if target == "mariadb":
            target = "mysql"
        
        # If same database type, return as-is
        if source == target:
            return data_type
        
        # Route to appropriate converter
        if source == 'supabase' or source == 'postgresql':
            if target == 'mysql':
                return TypeConverter._postgres_to_mysql(data_type, char_length, precision, scale)
            elif target == 'postgresql' or target == 'supabase':
                return data_type  # Same type system
            elif target == 'mongodb':
                return data_type  # MongoDB connector already uses PostgreSQL-compatible types
            elif target == 'sqlserver':
                # SQL Server is broadly similar to PostgreSQL for most core types
                return data_type
            elif target == 'sqlite':
                return TypeConverter._postgres_to_sqlite(data_type, char_length, precision, scale)
        
        elif source == 'mysql':
            if target == 'supabase' or target == 'postgresql':
                return TypeConverter._mysql_to_postgres(data_type, char_length, precision, scale)
            elif target == 'mongodb':
                # MongoDB uses PostgreSQL-compatible types, so convert MySQL -> PostgreSQL
                return TypeConverter._mysql_to_postgres(data_type, char_length, precision, scale)
            elif target == 'sqlserver':
                # Convert MySQL -> PostgreSQL, then rely on similarity
                return TypeConverter._mysql_to_postgres(data_type, char_length, precision, scale)
            elif target == 'sqlite':
                # Convert MySQL -> PostgreSQL -> SQLite
                pg_type = TypeConverter._mysql_to_postgres(data_type, char_length, precision, scale)
                return TypeConverter._postgres_to_sqlite(pg_type, char_length, precision, scale)
        
        elif source == 'mongodb':
            # MongoDB connector already infers PostgreSQL-compatible types
            if target == 'supabase' or target == 'postgresql':
                return data_type  # Already compatible
            elif target == 'mysql':
                return TypeConverter._postgres_to_mysql(data_type, char_length, precision, scale)
            elif target == 'sqlite':
                # Convert MongoDB (PostgreSQL-compatible) -> SQLite
                return TypeConverter._postgres_to_sqlite(data_type, char_length, precision, scale)
            elif target == 'sqlserver':
                return data_type

        elif source == 'sqlserver':
            # SQL Server to other databases
            if target == 'mysql':
                return TypeConverter._sqlserver_to_mysql(data_type, char_length, precision, scale)
            elif target == 'sqlite':
                # Convert SQL Server -> PostgreSQL -> SQLite
                # We don't have a dedicated SQL Server -> PostgreSQL mapper yet, but most types
                # coming from SQLServerConnector are already fairly PostgreSQL-like (e.g. INT, VARCHAR, DATETIME2).
                # Route through the existing PostgreSQL -> SQLite mapping for stability.
                return TypeConverter._postgres_to_sqlite(data_type, char_length, precision, scale)
            elif target in ['supabase', 'postgresql', 'mongodb']:
                return data_type
        
        elif source == 'sqlite':
            # SQLite to other databases
            if target in ['supabase', 'postgresql']:
                return TypeConverter._sqlite_to_postgres(data_type, char_length, precision, scale)
            elif target == 'mysql':
                # Convert SQLite -> PostgreSQL -> MySQL
                pg_type = TypeConverter._sqlite_to_postgres(data_type, char_length, precision, scale)
                return TypeConverter._postgres_to_mysql(pg_type, char_length, precision, scale)
            elif target == 'mongodb':
                # MongoDB uses PostgreSQL-compatible types
                return TypeConverter._sqlite_to_postgres(data_type, char_length, precision, scale)
            elif target == 'sqlserver':
                # SQL Server is similar to PostgreSQL for most types
                return TypeConverter._sqlite_to_postgres(data_type, char_length, precision, scale)

        elif source == 'cassandra':
            # Cassandra types are mapped as PostgreSQL-compatible intermediates
            if target in ['supabase', 'postgresql', 'mongodb', 'sqlserver']:
                return TypeConverter._cassandra_to_postgres_like(data_type)
            elif target == 'mysql':
                pg_type = TypeConverter._cassandra_to_postgres_like(data_type)
                return TypeConverter._postgres_to_mysql(pg_type, char_length, precision, scale)
            elif target == 'sqlite':
                pg_type = TypeConverter._cassandra_to_postgres_like(data_type)
                return TypeConverter._postgres_to_sqlite(pg_type, char_length, precision, scale)
        
        if target == 'cassandra':
            # Route into Cassandra-compatible primitive types
            if source in ['supabase', 'postgresql', 'mongodb', 'sqlserver']:
                return TypeConverter._postgres_like_to_cassandra(data_type)
            elif source == 'mysql':
                pg_type = TypeConverter._mysql_to_postgres(data_type, char_length, precision, scale)
                return TypeConverter._postgres_like_to_cassandra(pg_type)
            elif source == 'sqlite':
                pg_type = TypeConverter._sqlite_to_postgres(data_type, char_length, precision, scale)
                return TypeConverter._postgres_like_to_cassandra(pg_type)
        
        # Default: return as-is
        return data_type
    
    @staticmethod
    def _postgres_to_mysql(
        pg_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """Convert PostgreSQL data type to MySQL"""
        pg_type_lower = pg_type.lower().strip()
        
        # Remove existing parameters
        if '(' in pg_type_lower:
            pg_type_lower = pg_type_lower.split('(')[0].strip()
        
        # Type mapping
        type_map = {
            # Integer types
            'integer': 'INT', 'int': 'INT', 'int4': 'INT',
            'bigint': 'BIGINT', 'int8': 'BIGINT',
            'smallint': 'SMALLINT', 'int2': 'SMALLINT',
            'serial': 'INT', 'bigserial': 'BIGINT',
            
            # Text types
            'text': 'TEXT',
            'character varying': 'VARCHAR', 'varchar': 'VARCHAR',
            'character': 'CHAR', 'char': 'CHAR',
            
            # Numeric types
            'numeric': 'DECIMAL', 'decimal': 'DECIMAL',
            'real': 'FLOAT', 'float4': 'FLOAT',
            'double precision': 'DOUBLE', 'float8': 'DOUBLE',
            
            # Boolean
            'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
            
            # Date/Time
            'date': 'DATE',
            'time': 'TIME',
            'timestamp': 'DATETIME',
            'timestamp without time zone': 'DATETIME',
            'timestamp with time zone': 'DATETIME',
            
            # UUID
            'uuid': 'CHAR(36)',
            
            # JSON
            'json': 'JSON',
            'jsonb': 'JSON',
            
            # Binary
            'bytea': 'BLOB',
        }
        
        mysql_type = type_map.get(pg_type_lower, 'TEXT')
        
        # Handle special cases with parameters
        if mysql_type == 'VARCHAR' and char_length:
            return f'VARCHAR({char_length})'
        elif mysql_type == 'CHAR' and char_length:
            # MySQL doesn't have UUID type - use CHAR(36) instead
            return f'CHAR({char_length})'
        elif pg_type in ['DECIMAL', 'NUMERIC']:
            if precision and scale:
                return f'{pg_type}({precision},{scale})'
            elif precision:
                return f'{pg_type}({precision},0)'
        
        return pg_type
    
    @staticmethod
    def _mysql_to_postgres(
        mysql_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """Convert MySQL data type to PostgreSQL"""
        mysql_type_lower = mysql_type.lower().strip()
        
        # Remove existing parameters
        if '(' in mysql_type_lower:
            mysql_type_lower = mysql_type_lower.split('(')[0].strip()
        
        # Remove AUTO_INCREMENT if present
        mysql_type_lower = mysql_type_lower.replace(' auto_increment', '').strip()
        
        # Type mapping
        type_map = {
            # Integer types
            'int': 'INTEGER', 'integer': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'mediumint': 'INTEGER',
            
            # Text types
            'text': 'TEXT',
            'longtext': 'TEXT',
            'mediumtext': 'TEXT',
            'tinytext': 'TEXT',
            'varchar': 'VARCHAR', 'character varying': 'VARCHAR',
            'char': 'CHAR', 'character': 'CHAR',
            
            # Numeric types
            'decimal': 'DECIMAL', 'numeric': 'NUMERIC',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            
            # Boolean
            'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
            'tinyint(1)': 'BOOLEAN',
            
            # Date/Time
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            'year': 'INTEGER',
            
            # UUID
            'char(36)': 'UUID',
            
            # JSON
            'json': 'JSONB',
            
            # Binary
            'blob': 'BYTEA',
            'longblob': 'BYTEA',
            'mediumblob': 'BYTEA',
            'tinyblob': 'BYTEA',
            'binary': 'BYTEA',
            'varbinary': 'BYTEA',
        }
        
        pg_type = type_map.get(mysql_type_lower, 'TEXT')
        
        # Handle special cases with parameters
        if pg_type == 'VARCHAR' and char_length:
            return f'VARCHAR({char_length})'
        elif pg_type == 'CHAR' and char_length:
            if char_length == 36:
                return 'UUID'
            return f'CHAR({char_length})'
        elif pg_type in ['DECIMAL', 'NUMERIC']:
            if precision and scale:
                return f'{pg_type}({precision},{scale})'
            elif precision:
                return f'{pg_type}({precision},0)'
        elif mysql_type_lower == 'char' and char_length == 36:
            return 'UUID'
        
        return pg_type
    
    @staticmethod
    @staticmethod
    def _sqlserver_to_mysql(
        sqlserver_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """Convert SQL Server data type to MySQL"""
        sqlserver_type_lower = sqlserver_type.lower().strip()
        
        # Remove existing parameters
        if '(' in sqlserver_type_lower:
            sqlserver_type_lower = sqlserver_type_lower.split('(')[0].strip()
        
        # Type mapping
        type_map = {
            # Integer types
            'int': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'TINYINT',
            'bit': 'TINYINT(1)',  # SQL Server BIT -> MySQL TINYINT(1)
            
            # Text types
            'text': 'TEXT',
            'ntext': 'TEXT',  # Unicode text
            'varchar': 'VARCHAR',
            'nvarchar': 'VARCHAR',  # Unicode varchar
            'char': 'CHAR',
            'nchar': 'CHAR',  # Unicode char
            'varchar(max)': 'TEXT',
            'nvarchar(max)': 'TEXT',
            
            # Numeric types
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'float': 'DOUBLE',
            'real': 'FLOAT',
            'money': 'DECIMAL(19,4)',
            'smallmoney': 'DECIMAL(10,4)',
            
            # Date/Time
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'DATETIME',
            'datetime2': 'DATETIME',
            'smalldatetime': 'DATETIME',
            'datetimeoffset': 'DATETIME',
            'timestamp': 'TIMESTAMP',
            
            # Binary
            'binary': 'BINARY',
            'varbinary': 'VARBINARY',
            'varbinary(max)': 'BLOB',
            'image': 'BLOB',
            
            # Other
            'uniqueidentifier': 'CHAR(36)',  # UUID/GUID
            'xml': 'TEXT',
            'sql_variant': 'TEXT',
        }
        
        mysql_type = type_map.get(sqlserver_type_lower, 'TEXT')

        # SQL Server uses -1 for MAX types (e.g., NVARCHAR(MAX)); treat these as large TEXT/BLOB
        if char_length is not None and char_length <= 0:
            char_length = None
            if mysql_type in ['VARCHAR', 'CHAR', 'VARBINARY', 'BINARY']:
                # Promote to large types since length is unbounded
                if mysql_type in ['VARCHAR', 'CHAR']:
                    mysql_type = 'TEXT'
                elif mysql_type in ['VARBINARY', 'BINARY']:
                    mysql_type = 'BLOB'
        
        # Handle special cases with parameters
        if mysql_type == 'VARCHAR' and char_length:
            # For NVARCHAR, MySQL VARCHAR counts bytes, but we'll keep the same length
            return f'VARCHAR({char_length})'
        elif mysql_type == 'CHAR' and char_length:
            return f'CHAR({char_length})'
        elif mysql_type == 'DECIMAL' and precision and scale is not None:
            return f'DECIMAL({precision},{scale})'
        elif mysql_type == 'DECIMAL' and precision:
            return f'DECIMAL({precision},0)'
        
        return mysql_type

    @staticmethod
    def _sqlite_to_postgres(
        sqlite_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """Convert SQLite data type to PostgreSQL"""
        sqlite_type_lower = sqlite_type.lower().strip()
        
        # Remove existing parameters
        if '(' in sqlite_type_lower:
            sqlite_type_lower = sqlite_type_lower.split('(')[0].strip()
        
        # SQLite type affinity mapping
        # SQLite has dynamic typing but recognizes these type names
        type_map = {
            # Integer types
            'integer': 'INTEGER', 'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            
            # Text types
            'text': 'TEXT',
            'varchar': 'VARCHAR',
            'char': 'CHAR',
            'character': 'CHAR',
            'clob': 'TEXT',
            
            # Numeric types
            'real': 'REAL',
            'float': 'DOUBLE PRECISION',
            'double': 'DOUBLE PRECISION',
            'numeric': 'NUMERIC',
            'decimal': 'DECIMAL',
            
            # Boolean (SQLite stores as INTEGER 0/1)
            'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
            
            # Date/Time (SQLite stores as TEXT, INTEGER, or REAL)
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP',
            
            # Binary
            'blob': 'BYTEA',
            'binary': 'BYTEA',
        }
        
        pg_type = type_map.get(sqlite_type_lower, 'TEXT')
        
        # Handle special cases with parameters
        if pg_type == 'VARCHAR' and char_length:
            return f'VARCHAR({char_length})'
        elif pg_type == 'CHAR' and char_length:
            if char_length == 36:
                return 'UUID'
            return f'CHAR({char_length})'
        elif pg_type in ['DECIMAL', 'NUMERIC']:
            if precision is not None and scale is not None:
                return f'{pg_type}({precision},{scale})'
            elif precision is not None:
                return f'{pg_type}({precision},0)'
        
        return pg_type
    
    @staticmethod
    def _postgres_to_sqlite(
        pg_type: str,
        char_length: Optional[int] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None
    ) -> str:
        """Convert PostgreSQL/Supabase data type to SQLite"""
        pg_type_lower = pg_type.lower().strip()
        
        # Remove existing parameters
        if '(' in pg_type_lower:
            pg_type_lower = pg_type_lower.split('(')[0].strip()
        
        # PostgreSQL to SQLite type mapping
        # SQLite uses dynamic typing but recognizes these type names
        type_map = {
            # Integer types
            'integer': 'INTEGER', 'int': 'INTEGER', 'int4': 'INTEGER',
            'bigint': 'INTEGER', 'int8': 'INTEGER',
            'smallint': 'INTEGER', 'int2': 'INTEGER',
            'serial': 'INTEGER', 'bigserial': 'INTEGER',
            
            # Text types
            'text': 'TEXT',
            'varchar': 'TEXT',
            'character varying': 'TEXT',
            'char': 'TEXT',
            'character': 'TEXT',
            
            # Numeric types
            'real': 'REAL', 'float4': 'REAL',
            'double precision': 'REAL', 'float8': 'REAL',
            'numeric': 'NUMERIC',
            'decimal': 'NUMERIC',
            
            # Boolean (SQLite stores as INTEGER 0/1)
            'boolean': 'INTEGER', 'bool': 'INTEGER',
            
            # Date/Time (SQLite stores as TEXT)
            'date': 'TEXT',
            'time': 'TEXT',
            'timestamp': 'TEXT',
            'timestamp without time zone': 'TEXT',
            'timestamp with time zone': 'TEXT',
            
            # UUID (SQLite stores as TEXT)
            'uuid': 'TEXT',
            
            # JSON (SQLite stores as TEXT)
            'json': 'TEXT',
            'jsonb': 'TEXT',
            
            # Binary
            'bytea': 'BLOB',
        }
        
        sqlite_type = type_map.get(pg_type_lower, 'TEXT')
        
        # SQLite doesn't use length constraints for VARCHAR/CHAR in the same way
        # But we can preserve them for documentation purposes
        if sqlite_type == 'TEXT' and pg_type_lower in ['varchar', 'character varying', 'char', 'character']:
            if char_length:
                # SQLite will ignore the length, but we can include it for clarity
                return f'TEXT'  # SQLite doesn't enforce VARCHAR(n) lengths
            return 'TEXT'
        elif sqlite_type == 'NUMERIC' and precision is not None:
            if scale is not None:
                # SQLite supports NUMERIC(precision, scale) syntax
                return f'NUMERIC({precision},{scale})'
            else:
                return f'NUMERIC({precision})'
        
        return sqlite_type

    @staticmethod
    def _postgres_like_to_cassandra(data_type: str) -> str:
        """Convert PostgreSQL-like SQL types to Cassandra CQL types."""
        t = data_type.lower().strip()
        if '(' in t:
            t = t.split('(')[0].strip()
        mapping = {
            'integer': 'int',
            'int': 'int',
            'bigint': 'bigint',
            'smallint': 'smallint',
            'serial': 'int',
            'bigserial': 'bigint',
            'boolean': 'boolean',
            'bool': 'boolean',
            'text': 'text',
            'varchar': 'text',
            'character varying': 'text',
            'char': 'text',
            'uuid': 'uuid',
            'date': 'date',
            'time': 'time',
            'timestamp': 'timestamp',
            'timestamp with time zone': 'timestamp',
            'timestamp without time zone': 'timestamp',
            'real': 'float',
            'float': 'float',
            'double precision': 'double',
            'numeric': 'decimal',
            'decimal': 'decimal',
            'json': 'text',
            'jsonb': 'text',
            'bytea': 'blob',
        }
        return mapping.get(t, 'text')

    @staticmethod
    def _cassandra_to_postgres_like(data_type: str) -> str:
        """Convert Cassandra CQL types to PostgreSQL-like SQL types."""
        t = data_type.lower().strip()
        if '<' in t:
            # map/list/set/udt/frozen -> JSON-like text fallback
            return 'TEXT'
        mapping = {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'boolean': 'BOOLEAN',
            'text': 'TEXT',
            'ascii': 'TEXT',
            'uuid': 'UUID',
            'timeuuid': 'UUID',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'float': 'REAL',
            'double': 'DOUBLE PRECISION',
            'decimal': 'DECIMAL',
            'blob': 'BYTEA',
        }
        return mapping.get(t, 'TEXT')