"""
Schema Migration Tool: Supabase to MySQL
=========================================

A clean, organized tool to migrate database schemas from Supabase (PostgreSQL) to MySQL.

Features:
- Dynamic schema extraction from Supabase
- Optional table filtering (migrate specific tables or all)
- Type conversion (PostgreSQL -> MySQL)
- Dry-run mode for safe testing
- Comprehensive error handling and logging

Usage:
    python schema_migrate.py [--tables TABLE1,TABLE2] [--dry-run] [--require-confirmation]
"""

import os
import sys
import logging
import argparse
import getpass
from typing import List, Optional, Dict, Any
from pathlib import Path
from unified_db_mcp .database_connectors.supabase_connector import SupabaseConnector
from unified_db_mcp.database_connectors.mysql_connector import MySQLConnector
from unified_db_mcp.database_connectors.postgresql_connector import PostgreSQLConnector
from unified_db_mcp.database_connectors.mongodb_connector import MongoDBConnector
from unified_db_mcp.database_connectors.sqlserver_connector import SQLServerConnector
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector
from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.helpers.type_converter import TypeConverter

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, continue without it

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Use DEBUG for detailed troubleshooting
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration class - credentials can be overridden via environment variables"""
    
    # Supabase Configuration
    SUPABASE_API_KEY = os.getenv('SUPABASE_API_KEY', '')
    SUPABASE_PROJECT = os.getenv('SUPABASE_PROJECT', '')  # Default project name
    SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD', '')  # Database password for direct PostgreSQL connection
    
    # MySQL Configuration
    MYSQL_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'testdb')
    MYSQL_USER = os.getenv('MYSQL_USER', 'root')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')

    # MariaDB Configuration (treated as MySQL-compatible)
    MARIADB_HOST = os.getenv('MARIADB_HOST', '127.0.0.1')
    MARIADB_PORT = int(os.getenv('MARIADB_PORT', '3306'))
    MARIADB_DATABASE = os.getenv('MARIADB_DATABASE', 'testdb')
    MARIADB_USER = os.getenv('MARIADB_USER', 'root')
    MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD', '')
    
    # PostgreSQL Configuration
    POSTGRESQL_HOST = os.getenv('POSTGRESQL_HOST', '127.0.0.1')
    POSTGRESQL_PORT = int(os.getenv('POSTGRESQL_PORT', '5432'))
    POSTGRESQL_DATABASE = os.getenv('POSTGRESQL_DATABASE', 'postgres')
    POSTGRESQL_USER = os.getenv('POSTGRESQL_USER', 'postgres')
    POSTGRESQL_PASSWORD = os.getenv('POSTGRESQL_PASSWORD', '')
    
    # MongoDB Configuration
    MONGODB_HOST = os.getenv('MONGODB_HOST', '127.0.0.1')
    MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
    MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'testdb')
    MONGODB_USER = os.getenv('MONGODB_USER', '')
    MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD', '')
    MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING', '')
    
    # SQL Server Configuration
    SQLSERVER_HOST = os.getenv('SQLSERVER_HOST', 'localhost')
    SQLSERVER_PORT = int(os.getenv('SQLSERVER_PORT', '1433'))
    SQLSERVER_DATABASE = os.getenv('SQLSERVER_DATABASE', 'testdb')
    SQLSERVER_INSTANCE = os.getenv('SQLSERVER_INSTANCE', '')
    SQLSERVER_USER = os.getenv('SQLSERVER_USER', 'sa')
    SQLSERVER_PASSWORD = os.getenv('SQLSERVER_PASSWORD', '')
    SQLSERVER_CONNECTION_STRING = os.getenv('SQLSERVER_CONNECTION_STRING', '')
    
    # SQLite Configuration
    SQLITE_DATABASE_PATH = os.getenv('SQLITE_DATABASE_PATH', 'test.db')

    # Cassandra Configuration
    CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', '127.0.0.1')
    CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', '9042'))
    CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'testdb')
    CASSANDRA_USER = os.getenv('CASSANDRA_USER', '')
    CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD', '')
    CASSANDRA_DATACENTER = os.getenv('CASSANDRA_DATACENTER', 'datacenter1')
    
    @classmethod
    def get_supabase_credentials(cls, prompt_for_password: bool = False) -> Dict[str, Any]:
        """
        Get Supabase credentials with optional secure password prompt.
        
        Args:
            prompt_for_password: If True and password not set, prompt user securely
        
        Returns:
            Dictionary with Supabase credentials
        """
        creds = {
            'api_key': cls.SUPABASE_API_KEY,
        }
        if cls.SUPABASE_PROJECT:
            creds['project_name'] = cls.SUPABASE_PROJECT
            logger.debug(f"Using project name from config: '{cls.SUPABASE_PROJECT}'")
        
        # Check for connection string first (easiest solution - bypasses DNS issues)
        connection_string = os.getenv('SUPABASE_CONNECTION_STRING', '')
        if connection_string:
            creds['connection_string'] = connection_string
            logger.info("Using SUPABASE_CONNECTION_STRING for database connection")
            return creds
        
        # Handle database password
        db_password = cls.SUPABASE_DB_PASSWORD
        if not db_password and prompt_for_password:
            logger.info("")
            logger.info("=" * 70)
            logger.info("SUPABASE DATABASE PASSWORD REQUIRED")
            logger.info("=" * 70)
            logger.info("To apply schema to Supabase, we need the database password.")
            logger.info("You can find it in:")
            logger.info("  1. Supabase Dashboard -> Project Settings -> Database")
            logger.info("  2. Look for 'Connection string' or 'Database password'")
            logger.info("  3. Format: postgresql://postgres:[PASSWORD]@db.xxx.supabase.co")
            logger.info("")
            logger.info("Tip: Set SUPABASE_DB_PASSWORD environment variable to avoid prompts")
            logger.info("")
            try:
                db_password = getpass.getpass("Enter Supabase database password: ")
                if not db_password:
                    raise ValueError("Password cannot be empty")
            except (KeyboardInterrupt, EOFError):
                logger.info("\nPassword input cancelled.")
                raise ValueError("Password input was cancelled")
        
        if db_password:
            creds['db_password'] = db_password
        elif prompt_for_password:
            raise ValueError("Database password is required but was not provided")
        
        return creds
    
    @classmethod
    def get_mysql_credentials(cls) -> Dict[str, Any]:
        """Get MySQL credentials"""
        return {
            'host': cls.MYSQL_HOST,
            'port': cls.MYSQL_PORT,
            'database': cls.MYSQL_DATABASE,
            'user': cls.MYSQL_USER,
            'password': cls.MYSQL_PASSWORD,
            'use_pure': True,
            'ssl_disabled': True
        }

    @classmethod
    def get_mariadb_credentials(cls) -> Dict[str, Any]:
        """Get MariaDB credentials (MySQL-compatible)"""
        return {
            'host': cls.MARIADB_HOST,
            'port': cls.MARIADB_PORT,
            'database': cls.MARIADB_DATABASE,
            'user': cls.MARIADB_USER,
            'password': cls.MARIADB_PASSWORD,
            'use_pure': True,
            'ssl_disabled': True
        }
    
    @classmethod
    def get_postgresql_credentials(cls) -> Dict[str, Any]:
        """Get PostgreSQL credentials"""
        return {
            'host': cls.POSTGRESQL_HOST,
            'port': cls.POSTGRESQL_PORT,
            'database': cls.POSTGRESQL_DATABASE,
            'user': cls.POSTGRESQL_USER,
            'password': cls.POSTGRESQL_PASSWORD
        }
    
    @classmethod
    def get_mongodb_credentials(cls) -> Dict[str, Any]:
        """Get MongoDB credentials"""
        creds = {
            'host': cls.MONGODB_HOST,
            'port': cls.MONGODB_PORT,
            'database': cls.MONGODB_DATABASE
        }
        
        # Use connection string if provided
        if cls.MONGODB_CONNECTION_STRING:
            creds['connection_string'] = cls.MONGODB_CONNECTION_STRING
        elif cls.MONGODB_USER and cls.MONGODB_PASSWORD:
            creds['user'] = cls.MONGODB_USER
            creds['username'] = cls.MONGODB_USER
            creds['password'] = cls.MONGODB_PASSWORD
        
        return creds
    
    @classmethod
    def get_sqlserver_credentials(cls) -> Dict[str, Any]:
        """Get SQL Server credentials"""
        creds: Dict[str, Any] = {}

        # Use connection string if provided
        if cls.SQLSERVER_CONNECTION_STRING:
            creds["connection_string"] = cls.SQLSERVER_CONNECTION_STRING
            return creds

        creds["host"] = cls.SQLSERVER_HOST
        creds["port"] = cls.SQLSERVER_PORT
        creds["database"] = cls.SQLSERVER_DATABASE
        creds["instance"] = cls.SQLSERVER_INSTANCE
        creds["user"] = cls.SQLSERVER_USER
        creds["password"] = cls.SQLSERVER_PASSWORD
        return creds
    
    @classmethod
    def get_sqlite_credentials(cls) -> Dict[str, Any]:
        """Get SQLite credentials"""
        return {
            'database_path': cls.SQLITE_DATABASE_PATH
        }

    @classmethod
    def get_cassandra_credentials(cls) -> Dict[str, Any]:
        """Get Cassandra credentials"""
        creds: Dict[str, Any] = {
            'host': cls.CASSANDRA_HOST,
            'port': cls.CASSANDRA_PORT,
            'keyspace': cls.CASSANDRA_KEYSPACE,
            'datacenter': cls.CASSANDRA_DATACENTER,
        }
        if cls.CASSANDRA_USER and cls.CASSANDRA_PASSWORD:
            creds['user'] = cls.CASSANDRA_USER
            creds['password'] = cls.CASSANDRA_PASSWORD
        return creds
    
    @classmethod
    def get_credentials(cls, db_type: str) -> Dict[str, Any]:
        """Get credentials for any database type"""
        db_type_lower = db_type.lower()
        if db_type_lower in ['supabase', 'supabase']:
            return cls.get_supabase_credentials()
        elif db_type_lower == 'mysql':
            return cls.get_mysql_credentials()
        elif db_type_lower == 'mariadb':
            return cls.get_mariadb_credentials()
        elif db_type_lower == 'postgresql':
            return cls.get_postgresql_credentials()
        elif db_type_lower == 'mongodb':
            return cls.get_mongodb_credentials()
        elif db_type_lower == 'sqlserver':
            return cls.get_sqlserver_credentials()
        elif db_type_lower == 'sqlite':
            return cls.get_sqlite_credentials()
        elif db_type_lower == 'cassandra':
            return cls.get_cassandra_credentials()
        else:
            raise ValueError(f"Unknown database type: {db_type}")


# ============================================================================
# STEP 1: EXTRACT SCHEMA FROM SUPABASE
# ============================================================================

def extract_schema_from_supabase(table_names: Optional[List[str]] = None) -> SchemaInfo:
    """
    Extract schema from Supabase database.
    
    Args:
        table_names: Optional list of specific table names to extract.
                    If None, extracts all tables.
    
    Returns:
        SchemaInfo object containing extracted schema
    """
    logger.info("=" * 70)
    logger.info("STEP 1: Extracting Schema from Supabase")
    logger.info("=" * 70)
    
    try:
        # Initialize Supabase connector
        supabase_connector = SupabaseConnector()
        supabase_credentials = Config.get_supabase_credentials()
        
        # Add table filter if provided
        if table_names:
            supabase_credentials['table_names'] = ','.join(table_names)
            logger.info(f"Filtering tables: {', '.join(table_names)}")
        else:
            logger.info("Auto-discovering all tables from Supabase...")
        
        # Connect and extract schema
        logger.info("Connecting to Supabase...")
        connection = supabase_connector.connect(supabase_credentials)
        
        logger.info("Extracting schema...")
        schema = supabase_connector.extract_schema(connection, supabase_credentials)
        
        # Display results
        logger.info("Schema extraction complete.")
        logger.info(f"   Database: {schema.database_name}")
        logger.info(f"   Tables found: {len(schema.tables)}")
        
        if schema.tables:
            for table in schema.tables:
                logger.info(f"      - {table.name} ({len(table.columns)} columns)")
        else:
            logger.warning("No tables found in Supabase.")
            if table_names:
                logger.warning(f"   Check if these tables exist: {', '.join(table_names)}")
        
        return schema
        
    except Exception as e:
        logger.error(f"Error extracting schema from Supabase: {e}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# STEP 2: CONVERT POSTGRESQL SCHEMA TO MYSQL
# ============================================================================

def convert_postgres_to_mysql_type(pg_type: str, char_length: int = None, 
                                   precision: int = None, scale: int = None) -> str:
    """
    Convert PostgreSQL data type to MySQL data type.
    
    Args:
        pg_type: PostgreSQL data type
        char_length: Character length (for VARCHAR/CHAR)
        precision: Numeric precision (for DECIMAL)
        scale: Numeric scale (for DECIMAL)
    
    Returns:
        MySQL data type string
    """
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
        'serial': 'INT AUTO_INCREMENT', 'bigserial': 'BIGINT AUTO_INCREMENT',
        
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
        'timestamp': 'DATETIME', 'timestamp without time zone': 'DATETIME',
        'timestamp with time zone': 'DATETIME',
        
        # UUID
        'uuid': 'CHAR(36)',
        
        # JSON
        'json': 'JSON', 'jsonb': 'JSON',
        
        # Binary
        'bytea': 'BLOB',
    }
    
    mysql_type = type_map.get(pg_type_lower, 'TEXT')  # Default to TEXT for unknown types
    
    if mysql_type == 'CHAR(36)' and pg_type_lower == 'uuid':
        return 'CHAR(36)'
    
    return mysql_type


def convert_mysql_to_postgres_type(mysql_type: str, char_length: int = None, 
                                   precision: int = None, scale: int = None) -> str:
    """
    Convert MySQL data type to PostgreSQL data type.
    
    Args:
        mysql_type: MySQL data type
        char_length: Character length (for VARCHAR/CHAR)
        precision: Numeric precision (for DECIMAL)
        scale: Numeric scale (for DECIMAL)
    
    Returns:
        PostgreSQL data type string
    """
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
        'tinyint(1)': 'BOOLEAN',  # MySQL often uses TINYINT(1) for boolean
        
        # Date/Time
        'date': 'DATE',
        'time': 'TIME',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'year': 'INTEGER',  # PostgreSQL doesn't have YEAR type
        
        # UUID (MySQL uses CHAR(36) or VARCHAR(36))
        'char(36)': 'UUID',
        
        # JSON
        'json': 'JSONB',  # PostgreSQL prefers JSONB
        
        # Binary
        'blob': 'BYTEA',
        'longblob': 'BYTEA',
        'mediumblob': 'BYTEA',
        'tinyblob': 'BYTEA',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
    }
    
    pg_type = type_map.get(mysql_type_lower, 'TEXT')  # Default to TEXT for unknown types
    
    # Handle special cases with parameters
    if pg_type == 'VARCHAR' and char_length:
        return f'VARCHAR({char_length})'
    elif pg_type == 'CHAR' and char_length:
        return f'CHAR({char_length})'
    elif pg_type in ['DECIMAL', 'NUMERIC']:
        if precision and scale:
            return f'{pg_type}({precision},{scale})'
        elif precision:
            return f'{pg_type}({precision},0)'
        else:
            return pg_type
    elif mysql_type_lower == 'char' and char_length == 36:
        return 'UUID'
    
    return pg_type


def convert_schema_to_mysql(supabase_schema: SchemaInfo) -> SchemaInfo:
    """
    Convert PostgreSQL schema to MySQL-compatible schema.
    
    Args:
        supabase_schema: SchemaInfo from Supabase (PostgreSQL)
    
    Returns:
        SchemaInfo compatible with MySQL
    """
    logger.info("=" * 70)
    logger.info("STEP 2: Converting PostgreSQL Schema to MySQL")
    logger.info("=" * 70)
    
    mysql_tables = []
    
    for table in supabase_schema.tables:
        logger.info(f"Converting table: {table.name}")
        mysql_columns = []
        
        for col in table.columns:
            # Convert data type
            mysql_type = convert_postgres_to_mysql_type(
                col.data_type,
                col.character_maximum_length,
                col.numeric_precision,
                col.numeric_scale
            )
            
            # Handle UUID special case
            char_length = col.character_maximum_length
            if col.data_type.lower() == 'uuid':
                char_length = 36
            
            # Create MySQL column
            mysql_col = ColumnInfo(
                name=col.name,
                data_type=mysql_type,
                is_nullable=col.is_nullable,
                default_value=col.default_value,
                character_maximum_length=char_length,
                numeric_precision=col.numeric_precision,
                numeric_scale=col.numeric_scale,
                is_primary_key=col.is_primary_key,
                is_foreign_key=col.is_foreign_key
            )
            mysql_columns.append(mysql_col)
        
        mysql_tables.append(TableInfo(
            name=table.name,
            columns=mysql_columns,
            indexes=table.indexes
        ))
        
        logger.info(f"   Converted {len(mysql_columns)} columns")
    
    mysql_schema = SchemaInfo(
        database_type='mysql',
        database_name=supabase_schema.database_name,
        tables=mysql_tables
    )
    
    logger.info(f"Schema conversion complete: {len(mysql_tables)} tables")
    return mysql_schema


def convert_schema_to_supabase(mysql_schema: SchemaInfo) -> SchemaInfo:
    """
    Convert MySQL schema to Supabase/PostgreSQL-compatible schema.
    
    Args:
        mysql_schema: SchemaInfo from MySQL
    
    Returns:
        SchemaInfo compatible with Supabase/PostgreSQL
    """
    logger.info("=" * 70)
    logger.info("STEP 2: Converting MySQL Schema to PostgreSQL")
    logger.info("=" * 70)
    
    postgres_tables = []
    
    for table in mysql_schema.tables:
        logger.info(f"Converting table: {table.name}")
        postgres_columns = []
        
        for col in table.columns:
            # Convert data type
            pg_type = convert_mysql_to_postgres_type(
                col.data_type,
                col.character_maximum_length,
                col.numeric_precision,
                col.numeric_scale
            )
            
            # Handle UUID special case
            char_length = col.character_maximum_length
            if col.data_type.upper() in ['CHAR', 'VARCHAR'] and char_length == 36:
                pg_type = 'UUID'
                char_length = None
            
            # Handle AUTO_INCREMENT - convert to SERIAL or use DEFAULT
            default_value = col.default_value
            if 'AUTO_INCREMENT' in str(col.data_type).upper():
                # For PostgreSQL, use SERIAL or BIGSERIAL
                if pg_type == 'INTEGER':
                    pg_type = 'SERIAL'
                elif pg_type == 'BIGINT':
                    pg_type = 'BIGSERIAL'
                default_value = None
            
            # Create PostgreSQL column
            postgres_col = ColumnInfo(
                name=col.name,
                data_type=pg_type,
                is_nullable=col.is_nullable,
                default_value=default_value,
                character_maximum_length=char_length,
                numeric_precision=col.numeric_precision,
                numeric_scale=col.numeric_scale,
                is_primary_key=col.is_primary_key,
                is_foreign_key=col.is_foreign_key
            )
            postgres_columns.append(postgres_col)
        
        postgres_tables.append(TableInfo(
            name=table.name,
            columns=postgres_columns,
            indexes=table.indexes
        ))
        
        logger.info(f"   Converted {len(postgres_columns)} columns")
    
    postgres_schema = SchemaInfo(
        database_type='postgresql',
        database_name=mysql_schema.database_name,
        tables=postgres_tables
    )
    
    logger.info(f"Schema conversion complete: {len(postgres_tables)} tables")
    return postgres_schema


# ============================================================================
# STEP 1 (REVERSE): EXTRACT SCHEMA FROM MYSQL
# ============================================================================

def extract_schema_from_mysql(table_names: Optional[List[str]] = None) -> SchemaInfo:
    """
    Extract schema from MySQL database.
    
    Args:
        table_names: Optional list of specific table names to extract.
                    If None, extracts all tables.
    
    Returns:
        SchemaInfo object containing extracted schema
    """
    logger.info("=" * 70)
    logger.info("STEP 1: Extracting Schema from MySQL")
    logger.info("=" * 70)
    
    try:
        # Initialize MySQL connector
        mysql_connector = MySQLConnector()
        mysql_credentials = Config.get_mysql_credentials()
        
        # Connect and extract schema
        logger.info("Connecting to MySQL...")
        connection = mysql_connector.connect(mysql_credentials)
        
        logger.info("Extracting schema...")
        schema = mysql_connector.extract_schema(connection, mysql_credentials)
        connection.close()
        
        # Filter tables if specified
        if table_names:
            original_tables = schema.tables
            schema.tables = [t for t in original_tables if t.name in table_names]
            logger.info(f"Filtered to {len(schema.tables)} table(s): {', '.join([t.name for t in schema.tables])}")
            missing = set(table_names) - set([t.name for t in schema.tables])
            if missing:
                logger.warning(f"Tables not found: {', '.join(missing)}")
        
        # Display results
        logger.info("Schema extraction complete.")
        logger.info(f"   Database: {schema.database_name}")
        logger.info(f"   Tables found: {len(schema.tables)}")
        
        if schema.tables:
            for table in schema.tables:
                logger.info(f"      - {table.name} ({len(table.columns)} columns)")
        else:
            logger.warning("No tables found in MySQL.")
            if table_names:
                logger.warning(f"   Check if these tables exist: {', '.join(table_names)}")
        
        return schema
        
    except Exception as e:
        logger.error(f"Error extracting schema from MySQL: {e}")
        import traceback
        traceback.print_exc()
        raise


# ============================================================================
# STEP 3: APPLY SCHEMA TO MYSQL
# ============================================================================

def apply_schema_to_mysql(mysql_schema: SchemaInfo, dry_run: bool = False) -> bool:
    """
    Apply schema to MySQL database.
    
    Args:
        mysql_schema: MySQL-compatible SchemaInfo
        dry_run: If True, only show what would be done without applying
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info(f"STEP 3: Applying Schema to MySQL {'(DRY RUN)' if dry_run else ''}")
    logger.info("=" * 70)
    
    try:
        # Initialize MySQL connector
        mysql_connector = MySQLConnector()
        mysql_credentials = Config.get_mysql_credentials()
        
        logger.info("Connecting to MySQL...")
        logger.info(f"   Host: {mysql_credentials['host']}:{mysql_credentials['port']}")
        logger.info(f"   Database: {mysql_credentials['database']}")
        logger.info(f"   User: {mysql_credentials['user']}")
        
        connection = mysql_connector.connect(mysql_credentials)
        
        if dry_run:
            logger.info("DRY RUN MODE: Would create the following tables:")
            for table in mysql_schema.tables:
                logger.info(f"   - {table.name} ({len(table.columns)} columns)")
            connection.close()
            return True
        
        # Apply schema
        logger.info("Applying schema to MySQL...")
        mysql_connector.apply_schema(connection, mysql_schema)
        connection.close()
        
        logger.info("Schema applied successfully.")
        logger.info(f"   Tables created: {len(mysql_schema.tables)}")
        for table in mysql_schema.tables:
            logger.info(f"      - {table.name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error applying schema to MySQL: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# STEP 3 (REVERSE): APPLY SCHEMA TO SUPABASE
# ============================================================================

def apply_schema_to_supabase(postgres_schema: SchemaInfo, dry_run: bool = False, credentials: Dict[str, Any] = None) -> bool:
    """
    Apply schema to Supabase database.
    
    Args:
        postgres_schema: PostgreSQL-compatible SchemaInfo
        dry_run: If True, only show what would be done without applying
        credentials: Optional credentials dict (if None, uses Config)
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info(f"STEP 3: Applying Schema to Supabase {'(DRY RUN)' if dry_run else ''}")
    logger.info("=" * 70)
    
    try:
        # Initialize Supabase connector
        supabase_connector = SupabaseConnector()
        if credentials is None:
            supabase_credentials = Config.get_supabase_credentials()
        else:
            supabase_credentials = credentials
        
        # Add database password if available
        db_password = os.getenv('SUPABASE_DB_PASSWORD')
        if db_password:
            supabase_credentials['db_password'] = db_password
        
        logger.info("Connecting to Supabase...")
        logger.info(f"   Project: {supabase_credentials.get('project_name', 'default')}")
        
        connection = supabase_connector.connect(supabase_credentials)
        
        if dry_run:
            logger.info("DRY RUN MODE: Would create the following tables:")
            for table in postgres_schema.tables:
                logger.info(f"   - {table.name} ({len(table.columns)} columns)")
                for col in table.columns:
                    logger.info(f"      - {col.name}: {col.data_type}")
            return True
        
        # Apply schema
        logger.info("Applying schema to Supabase...")
        supabase_connector.apply_schema(connection, postgres_schema, supabase_credentials)
        
        logger.info("Schema applied successfully.")
        logger.info(f"   Tables created: {len(postgres_schema.tables)}")
        for table in postgres_schema.tables:
            logger.info(f"      - {table.name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error applying schema to Supabase: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# STEP 4: VERIFY MIGRATION
# ============================================================================

def verify_migration() -> bool:
    """
    Verify that migration was successful by comparing schemas.
    
    Returns:
        True if verification passes, False otherwise
    """
    logger.info("=" * 70)
    logger.info("STEP 4: Verifying Migration")
    logger.info("=" * 70)
    
    try:
        mysql_connector = MySQLConnector()
        mysql_credentials = Config.get_mysql_credentials()
        
        logger.info("Extracting schema from MySQL to verify...")
        connection = mysql_connector.connect(mysql_credentials)
        mysql_schema = mysql_connector.extract_schema(connection, mysql_credentials)
        connection.close()
        
        logger.info("Verification complete.")
        logger.info(f"   MySQL database has {len(mysql_schema.tables)} tables")
        for table in mysql_schema.tables:
            logger.info(f"      - {table.name} ({len(table.columns)} columns)")
        
        return True
        
    except Exception as e:
        logger.warning(f"Verification failed: {e}")
        return False


# ============================================================================
# GENERIC BIDIRECTIONAL MIGRATION FUNCTION
# ============================================================================

def convert_schema_between_databases(
    source_schema: SchemaInfo,
    source_db_type: str,
    target_db_type: str
) -> SchemaInfo:
    """
    Convert schema from source database type to target database type.
    
    Args:
        source_schema: SchemaInfo from source database
        source_db_type: Source database type ('supabase', 'mysql', 'postgresql')
        target_db_type: Target database type ('supabase', 'mysql', 'postgresql')
    
    Returns:
        Converted SchemaInfo for target database
    """
    logger.info("=" * 70)
    logger.info(f"Converting Schema: {source_db_type.upper()} -> {target_db_type.upper()}")
    logger.info("=" * 70)
    
    target_tables = []
    
    for table in source_schema.tables:
        logger.info(f"Converting table: {table.name}")
        target_columns = []
        
        for col in table.columns:
            # Convert data type using TypeConverter
            target_type = TypeConverter.convert_type(
                source_db=source_db_type,
                target_db=target_db_type,
                data_type=col.data_type,
                char_length=col.character_maximum_length,
                precision=col.numeric_precision,
                scale=col.numeric_scale
            )
            
            # Handle special cases
            char_length = col.character_maximum_length
            default_value = col.default_value
            
            # Handle AUTO_INCREMENT for MySQL -> PostgreSQL
            if source_db_type == 'mysql' and target_db_type in ['supabase', 'postgresql']:
                if 'AUTO_INCREMENT' in str(col.data_type).upper():
                    if target_type == 'INTEGER':
                        target_type = 'SERIAL'
                    elif target_type == 'BIGINT':
                        target_type = 'BIGSERIAL'
                    default_value = None
            
            # Handle UUID special case
            if col.data_type.upper() in ['CHAR', 'VARCHAR'] and char_length == 36:
                if target_db_type in ['supabase', 'postgresql']:
                    target_type = 'UUID'
                    char_length = None
            
            # Create target column
            target_col = ColumnInfo(
                name=col.name,
                data_type=target_type,
                is_nullable=col.is_nullable,
                default_value=default_value,
                character_maximum_length=char_length,
                numeric_precision=col.numeric_precision,
                numeric_scale=col.numeric_scale,
                is_primary_key=col.is_primary_key,
                is_foreign_key=col.is_foreign_key
            )
            target_columns.append(target_col)
        
        target_tables.append(TableInfo(
            name=table.name,
            columns=target_columns,
            indexes=table.indexes
        ))
        
        logger.info(f"   Converted {len(target_columns)} columns")
    
    target_schema = SchemaInfo(
        database_type=target_db_type,
        database_name=source_schema.database_name,
        tables=target_tables
    )
    
    logger.info(f"Schema conversion complete: {len(target_tables)} tables")
    return target_schema


def migrate_between_databases(
    source_db: str,
    target_db: str,
    table_names: Optional[List[str]] = None,
    dry_run: bool = False,
    require_confirmation: bool = False,
    sqlite_path: Optional[str] = None
) -> bool:
    """
    Generic bidirectional migration function between any two databases.
    
    Args:
        source_db: Source database type ('supabase', 'mysql', 'postgresql', 'mongodb')
        target_db: Target database type ('supabase', 'mysql', 'postgresql', 'mongodb')
        table_names: Optional list of specific tables to migrate
        dry_run: If True, only show what would be done
        require_confirmation: If True, require user confirmation before applying
    
    Returns:
        True if migration successful, False otherwise
    """
    try:
        logger.info("=" * 70)
        logger.info(f"MIGRATION: {source_db.upper()} -> {target_db.upper()}")
        logger.info("=" * 70)
        
        # Get connectors
        source_connector = DatabaseConnector.get_connector(source_db)
        target_connector = DatabaseConnector.get_connector(target_db)
        
        # Get credentials
        source_credentials = Config.get_credentials(source_db)
        
        # Override SQLite path if provided via command line
        if source_db.lower() == 'sqlite' and sqlite_path:
            source_credentials['database_path'] = sqlite_path
            logger.info(f"Using SQLite database path from command line: {sqlite_path}")
        
        # For Supabase target, prompt for password if needed
        if target_db.lower() == 'supabase':
            target_credentials = Config.get_supabase_credentials(prompt_for_password=True)
        else:
            target_credentials = Config.get_credentials(target_db)
        
        # Override SQLite path if provided via command line for target
        if target_db.lower() == 'sqlite' and sqlite_path:
            target_credentials['database_path'] = sqlite_path
            logger.info(f"Using SQLite database path from command line: {sqlite_path}")
        
        # Step 1: Extract schema from source
        logger.info(f"Extracting schema from {source_db}...")
        source_connection = source_connector.connect(source_credentials)
        source_schema = source_connector.extract_schema(source_connection, source_credentials)
        
        # Filter tables if specified
        if table_names:
            original_tables = source_schema.tables
            source_schema.tables = [t for t in original_tables if t.name in table_names]
            logger.info(f"Filtered to {len(source_schema.tables)} table(s): {', '.join([t.name for t in source_schema.tables])}")
            missing = set(table_names) - set([t.name for t in source_schema.tables])
            if missing:
                logger.warning(f"Tables not found: {', '.join(missing)}")
        
        if not source_schema.tables:
            logger.error("No tables to migrate. Exiting.")
            return False
        
        # Step 2: Convert schema
        target_schema = convert_schema_between_databases(
            source_schema=source_schema,
            source_db_type=source_db,
            target_db_type=target_db
        )
        
        # Confirmation if required
        if require_confirmation and not dry_run:
            logger.info("\n" + "=" * 70)
            logger.info("CONFIRMATION REQUIRED")
            logger.info("=" * 70)
            logger.info(f"This will create {len(target_schema.tables)} table(s) in {target_db}:")
            for table in target_schema.tables:
                logger.info(f"   - {table.name}")
            logger.info("\nType 'APPLY' to proceed, or anything else to cancel:")
            confirmation = input("> ").strip()
            if confirmation != 'APPLY':
                logger.info("Migration cancelled by user.")
                return False
        
        # Step 3: Apply schema to target
        logger.info(f"Applying schema to {target_db}...")
        target_connection = target_connector.connect(target_credentials)
        
        if dry_run:
            logger.info("DRY RUN MODE: Would create the following tables:")
            for table in target_schema.tables:
                logger.info(f"   - {table.name} ({len(table.columns)} columns)")
                for col in table.columns:
                    logger.info(f"      - {col.name}: {col.data_type}")
            return True
        
        target_connector.apply_schema(target_connection, target_schema, target_credentials)
        
        logger.info("\n" + "=" * 70)
        logger.info("MIGRATION COMPLETE.")
        logger.info("=" * 70)
        logger.info(f"   Migrated {len(target_schema.tables)} table(s) from {source_db} to {target_db}")
        for table in target_schema.tables:
            logger.info(f"      - {table.name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# MAIN MIGRATION FUNCTIONS (Legacy - for backward compatibility)
# ============================================================================

def migrate_schema(
    table_names: Optional[List[str]] = None,
    dry_run: bool = False,
    require_confirmation: bool = False
) -> bool:
    """
    Main migration function: Supabase -> MySQL
    
    Args:
        table_names: Optional list of specific tables to migrate
        dry_run: If True, only show what would be done
        require_confirmation: If True, require user confirmation before applying
    
    Returns:
        True if migration successful, False otherwise
    """
    try:
        # Step 1: Extract schema from Supabase
        supabase_schema = extract_schema_from_supabase(table_names)
        
        if not supabase_schema.tables:
            logger.error("No tables to migrate. Exiting.")
            return False
        
        # Step 2: Convert to MySQL format
        mysql_schema = convert_schema_to_mysql(supabase_schema)
        
        # Confirmation if required
        if require_confirmation and not dry_run:
            logger.info("\n" + "=" * 70)
            logger.info("CONFIRMATION REQUIRED")
            logger.info("=" * 70)
            logger.info(f"This will create {len(mysql_schema.tables)} table(s) in MySQL:")
            for table in mysql_schema.tables:
                logger.info(f"   - {table.name}")
            logger.info("\nType 'APPLY' to proceed, or anything else to cancel:")
            confirmation = input("> ").strip()
            if confirmation != 'APPLY':
                logger.info("Migration cancelled by user.")
                return False
        
        # Step 3: Apply to MySQL
        success = apply_schema_to_mysql(mysql_schema, dry_run=dry_run)
        
        if not success:
            return False
        
        # Step 4: Verify (only if not dry run)
        if not dry_run:
            verify_migration()
        
        logger.info("\n" + "=" * 70)
        logger.info("MIGRATION COMPLETE.")
        logger.info("=" * 70)
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Bidirectional database schema migration tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported Databases: supabase, mysql, mariadb, postgresql, mongodb, sqlserver, sqlite, cassandra

Examples:
  # Migrate Supabase -> MySQL (all tables)
  python schema_migrate.py --from supabase --to mysql
  
  # Migrate SQLite -> Supabase (all tables)
  python schema_migrate.py --from sqlite --to supabase
  
  # Migrate SQLite -> PostgreSQL (all tables)
  python schema_migrate.py --from sqlite --to postgresql --sqlite-path /path/to/database.db
  
  # Migrate SQLite -> MongoDB (all tables)
  python schema_migrate.py --from sqlite --to mongodb --sqlite-path /path/to/database.db
  
  # Migrate MongoDB -> SQLite (all collections)
  python schema_migrate.py --from mongodb --to sqlite --sqlite-path output.db
  
  # Migrate PostgreSQL -> SQLite (all tables)
  python schema_migrate.py --from postgresql --to sqlite --sqlite-path output.db
  
  # Migrate SQLite -> MySQL (all tables)
  python schema_migrate.py --from sqlite --to mysql --sqlite-path /path/to/database.db
  
  # Migrate MySQL -> SQLite (all tables)
  python schema_migrate.py --from mysql --to sqlite --sqlite-path output.db
  
  # Migrate SQLite -> SQL Server (all tables)
  python schema_migrate.py --from sqlite --to sqlserver --sqlite-path /path/to/database.db

  # Migrate SQL Server -> SQLite (all tables)
  python schema_migrate.py --from sqlserver --to sqlite --sqlite-path output.db

  # Migrate MariaDB -> SQLite (all tables)
  python schema_migrate.py --from mariadb --to sqlite --sqlite-path output.db

  # Migrate SQLite -> MariaDB (all tables)
  python schema_migrate.py --from sqlite --to mariadb --sqlite-path /path/to/database.db
  
  # Migrate MariaDB -> Supabase (all tables)
  python schema_migrate.py --from mariadb --to supabase
  
  # Migrate Supabase -> MariaDB (all tables)
  python schema_migrate.py --from supabase --to mariadb
  
  # Migrate MariaDB -> MySQL (all tables)
  python schema_migrate.py --from mariadb --to mysql
  
  # Migrate MariaDB -> PostgreSQL (all tables)
  python schema_migrate.py --from mariadb --to postgresql
  
  # Migrate PostgreSQL -> MariaDB (all tables)
  python schema_migrate.py --from postgresql --to mariadb
  
  # Migrate MySQL -> Supabase (specific tables)
  python schema_migrate.py --from mysql --to supabase --tables users,orders
  
  # Migrate MongoDB -> Supabase (all collections)
  python schema_migrate.py --from mongodb --to supabase
  
  # Migrate MongoDB -> Supabase (specific collections)
  python schema_migrate.py --from mongodb --to supabase --tables users,products
  
  # Migrate PostgreSQL -> MySQL (dry run)
  python schema_migrate.py --from postgresql --to mysql --dry-run

  # Migrate PostgreSQL -> Cassandra (all tables)
  python schema_migrate.py --from postgresql --to cassandra
  
  # Migrate with confirmation
  python schema_migrate.py --from supabase --to mysql --require-confirmation
  
  # Legacy: Supabase -> MySQL (backward compatibility)
  python schema_migrate.py --tables users,orders
        """
    )
    
    parser.add_argument(
        '--from',
        dest='source_db',
        type=str,
        choices=['supabase', 'mysql', 'mariadb', 'postgresql', 'mongodb', 'sqlserver', 'sqlite', 'cassandra'],
        help='Source database type (supabase, mysql, mariadb, postgresql, mongodb, sqlserver, sqlite, cassandra)'
    )
    
    parser.add_argument(
        '--to',
        dest='target_db',
        type=str,
        choices=['supabase', 'mysql', 'mariadb', 'postgresql', 'mongodb', 'sqlserver', 'sqlite', 'cassandra'],
        help='Target database type (supabase, mysql, mariadb, postgresql, mongodb, sqlserver, sqlite, cassandra)'
    )
    
    parser.add_argument(
        '--tables',
        type=str,
        help='Comma-separated list of specific tables to migrate (e.g., users,orders). If not provided, migrates all tables.'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be migrated without actually applying changes'
    )
    
    parser.add_argument(
        '--require-confirmation',
        action='store_true',
        help='Require typing "APPLY" before applying schema changes'
    )
    
    parser.add_argument(
        '--sqlite-path',
        type=str,
        help='Path to SQLite database file (overrides SQLITE_DATABASE_PATH env var)'
    )
    
    args = parser.parse_args()
    
    # Parse table names
    table_names = None
    if args.tables:
        table_names = [t.strip() for t in args.tables.split(',') if t.strip()]
        logger.info(f"Table filter: {', '.join(table_names)}")
    
    # Use new bidirectional migration if --from and --to are specified
    if args.source_db and args.target_db:
        if args.source_db == args.target_db:
            logger.error("Source and target databases cannot be the same.")
            sys.exit(1)
        
        success = migrate_between_databases(
            source_db=args.source_db,
            target_db=args.target_db,
            table_names=table_names,
            dry_run=args.dry_run,
            require_confirmation=args.require_confirmation,
            sqlite_path=args.sqlite_path if hasattr(args, 'sqlite_path') else None
        )
    elif args.source_db or args.target_db:
        logger.error("Both --from and --to must be specified for bidirectional migration.")
        logger.info("   Example: python schema_migrate.py --from mysql --to supabase")
        sys.exit(1)
    else:
        # Legacy mode: Supabase -> MySQL (backward compatibility)
        logger.info("Using legacy mode: Supabase -> MySQL")
        logger.info("   For other directions, use: --from <source> --to <target>")
        success = migrate_schema(
            table_names=table_names,
            dry_run=args.dry_run,
            require_confirmation=args.require_confirmation
        )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
