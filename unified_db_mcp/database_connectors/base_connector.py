"""Base connector class for database connections"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from unified_db_mcp.helpers.schema_utils import SchemaInfo


class DatabaseConnector(ABC):
    """Base class for database connectors"""
    
    @staticmethod
    def get_connector(db_type: str):
        """Factory method to get appropriate connector"""
        from unified_db_mcp.database_connectors.supabase_connector import SupabaseConnector
        from unified_db_mcp.database_connectors.postgresql_connector import PostgreSQLConnector
        from unified_db_mcp.database_connectors.mysql_connector import MySQLConnector
        from unified_db_mcp.database_connectors.mariadb_connector import MariaDBConnector
        from unified_db_mcp.database_connectors.mongodb_connector import MongoDBConnector
        from unified_db_mcp.database_connectors.sqlserver_connector import SQLServerConnector
        from unified_db_mcp.database_connectors.sqlite_connector import SQLiteConnector
        from unified_db_mcp.database_connectors.cassandra_connector import CassandraConnector
        
        db_type_lower = db_type.lower()
        if db_type_lower == 'supabase':
            return SupabaseConnector()
        elif db_type_lower == 'postgresql':
            return PostgreSQLConnector()
        elif db_type_lower == 'mysql':
            return MySQLConnector()
        elif db_type_lower == 'mariadb':
            return MariaDBConnector()
        elif db_type_lower == 'mongodb':
            return MongoDBConnector()
        elif db_type_lower == 'sqlserver':
            return SQLServerConnector()
        elif db_type_lower == 'sqlite':
            return SQLiteConnector()
        elif db_type_lower == 'cassandra':
            return CassandraConnector()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @abstractmethod
    def connect(self, credentials: Dict[str, Any]):
        """Establish database connection"""
        pass
    
    @abstractmethod
    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """Extract complete schema from database"""
        pass
    
    @abstractmethod
    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """Apply schema to database"""
        pass
