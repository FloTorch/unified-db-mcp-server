"""Credential handling utilities"""
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# LOCAL TEST CONFIGURATION
# ============================================================================
ENABLE_TEST_CREDENTIALS = True  # Set to True for local testing

# SOURCE DATABASE - Supabase (Only API Key needed)
TEST_SOURCE_SUPABASE_API_KEY = "sbp_7f2c27eca978bdd9070fbb00d2f773879fe54317"

# TARGET DATABASE - MySQL
TEST_TARGET_MYSQL_HOST = "127.0.0.1"
TEST_TARGET_MYSQL_DATABASE = "testdb"
TEST_TARGET_MYSQL_USER = "Madhu"
TEST_TARGET_MYSQL_PASSWORD = "Madhu@db07"
TEST_TARGET_MYSQL_PORT = 3306


def get_credentials_from_header(ctx: Optional[Any] = None, db_type: Optional[str] = None, prefix: str = "") -> Dict[str, Any]:
    """
    Extract database credentials from HTTP headers.
    
    For Supabase:
    - Only API key is required: x-supabase-api-key (or x-source-supabase-api-key / x-target-supabase-api-key)
    - System will fetch all accessible projects using the API key
    
    For MySQL:
    - x-mysql-host (or x-source-mysql-host / x-target-mysql-host)
    - x-mysql-database
    - x-mysql-user
    - x-mysql-password
    - x-mysql-port (optional, defaults to 3306)
    - use_pure and ssl_disabled are handled automatically (defaults to True)
    
    For PostgreSQL:
    - x-postgresql-host
    - x-postgresql-database
    - x-postgresql-user
    - x-postgresql-password
    - x-postgresql-port (optional, defaults to 5432)
    
    For local testing: Set ENABLE_TEST_CREDENTIALS = True and configure TEST_* variables above
    """
    # LOCAL TESTING MODE: Return test credentials if enabled
    if ENABLE_TEST_CREDENTIALS:
        if prefix == "source" and (db_type == "supabase" or db_type is None):
            return {
                "api_key": TEST_SOURCE_SUPABASE_API_KEY,
                "key": TEST_SOURCE_SUPABASE_API_KEY,
                "supabase_key": TEST_SOURCE_SUPABASE_API_KEY
            }
        elif prefix == "target" and (db_type == "mysql" or db_type is None):
            return {
                "host": TEST_TARGET_MYSQL_HOST,
                "database": TEST_TARGET_MYSQL_DATABASE,
                "user": TEST_TARGET_MYSQL_USER,
                "password": TEST_TARGET_MYSQL_PASSWORD,
                "port": TEST_TARGET_MYSQL_PORT,
                "use_pure": True,
                "ssl_disabled": True
            }
        else:
            logger.warning(f"Test credentials not configured for db_type: {db_type}, prefix: {prefix}")
            if not ctx:
                raise ValueError(
                    f"Context is required to extract credentials from headers. "
                    f"Test credentials not configured for db_type: {db_type}, prefix: {prefix}"
                )
    
    if not ctx:
        raise ValueError("Context is required to extract credentials from headers")
    
    try:
        request_context = ctx.request_context
        if not hasattr(request_context, 'request') or not request_context.request:
            raise ValueError("Request context not available")
        
        request = request_context.request
        header_dict = {}
        for name in request.headers.keys():
            header_dict[name.lower()] = request.headers[name]
        
        credentials = {}
        
        # Build header prefix for source/target (e.g., 'x-source-' or 'x-target-')
        header_prefix = f'x-{prefix}-' if prefix else 'x-'
        
        # Check for Supabase API key (only API key needed)
        supabase_key = (
            header_dict.get(f'{header_prefix}supabase-api-key') or
            header_dict.get('x-supabase-api-key')
        )
        if supabase_key:
            # Validate Supabase API key format
            if not (supabase_key.startswith('sb_') or supabase_key.startswith('sbp_') or supabase_key.startswith('eyJ')):
                raise ValueError(
                    "Invalid Supabase API key format. "
                    "Supabase API keys should start with 'sb_', 'sbp_', or be a JWT token starting with 'eyJ'"
                )
            
            credentials['api_key'] = supabase_key
            credentials['key'] = supabase_key
            credentials['supabase_key'] = supabase_key
            return credentials
        
        # Check for MySQL headers
        mysql_host = (
            header_dict.get(f'{header_prefix}mysql-host') or
            header_dict.get('x-mysql-host')
        )
        if mysql_host:
            credentials['host'] = mysql_host
            credentials['database'] = (
                header_dict.get(f'{header_prefix}mysql-database') or
                header_dict.get('x-mysql-database')
            )
            credentials['user'] = (
                header_dict.get(f'{header_prefix}mysql-user') or
                header_dict.get('x-mysql-user')
            )
            credentials['password'] = (
                header_dict.get(f'{header_prefix}mysql-password') or
                header_dict.get('x-mysql-password')
            )
            credentials['port'] = int(
                header_dict.get(f'{header_prefix}mysql-port') or
                header_dict.get('x-mysql-port', 3306)
            )
            # Default MySQL connection options
            credentials['use_pure'] = True
            credentials['ssl_disabled'] = True
            
            if not credentials.get('database') or not credentials.get('user') or not credentials.get('password'):
                raise ValueError(
                    "MySQL credentials incomplete. Required headers: "
                    "X-Mysql-Host, X-Mysql-Database, X-Mysql-User, X-Mysql-Password"
                )
            
            return credentials
        
        # Check for PostgreSQL headers
        pg_host = (
            header_dict.get(f'{header_prefix}postgresql-host') or
            header_dict.get('x-postgresql-host')
        )
        if pg_host:
            credentials['host'] = pg_host
            credentials['database'] = (
                header_dict.get(f'{header_prefix}postgresql-database') or
                header_dict.get('x-postgresql-database')
            )
            credentials['user'] = (
                header_dict.get(f'{header_prefix}postgresql-user') or
                header_dict.get('x-postgresql-user')
            )
            credentials['password'] = (
                header_dict.get(f'{header_prefix}postgresql-password') or
                header_dict.get('x-postgresql-password')
            )
            credentials['port'] = int(
                header_dict.get(f'{header_prefix}postgresql-port') or
                header_dict.get('x-postgresql-port', 5432)
            )
            
            if not credentials.get('database') or not credentials.get('user') or not credentials.get('password'):
                raise ValueError(
                    "PostgreSQL credentials incomplete. Required headers: "
                    "X-Postgresql-Host, X-Postgresql-Database, X-Postgresql-User, X-Postgresql-Password"
                )
            
            return credentials
        
        # Method 2: Fallback to JSON format in header
        creds_header = (
            header_dict.get(f'{header_prefix}database-credentials') or
            header_dict.get('x-database-credentials') or
            header_dict.get('database-credentials') or
            header_dict.get('authorization')
        )
        
        if creds_header:
            # Handle Bearer token format (for Supabase)
            if creds_header.startswith('Bearer '):
                token = creds_header.replace('Bearer ', '').strip()
                if token.startswith('sb_') or token.startswith('sbp_') or token.startswith('eyJ'):
                    return {"api_key": token, "key": token, "supabase_key": token}
                return {"access_token": token}
            
            # Parse JSON credentials
            try:
                parsed_creds = json.loads(creds_header)
                # Ensure MySQL defaults
                if 'host' in parsed_creds and 'database' in parsed_creds:
                    if 'use_pure' not in parsed_creds:
                        parsed_creds['use_pure'] = True
                    if 'ssl_disabled' not in parsed_creds:
                        parsed_creds['ssl_disabled'] = True
                return parsed_creds
            except json.JSONDecodeError:
                raise ValueError("Invalid credentials format. Must be valid JSON.")
        
        # No credentials found
        raise ValueError(
            "Database credentials not found in HTTP headers. "
            "Provide credentials using one of these methods:\n"
            "1. Supabase: X-Supabase-Api-Key (only API key needed)\n"
            "2. MySQL: X-Mysql-Host, X-Mysql-Database, X-Mysql-User, X-Mysql-Password\n"
            "3. PostgreSQL: X-Postgresql-Host, X-Postgresql-Database, X-Postgresql-User, X-Postgresql-Password\n"
            "4. JSON format: X-Database-Credentials (JSON string)"
        )
    
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Error extracting credentials from headers: {e}")
        raise ValueError(f"Failed to extract credentials from headers: {str(e)}")
