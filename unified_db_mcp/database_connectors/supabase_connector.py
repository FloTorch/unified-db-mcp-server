"""Supabase database connector using REST API"""
import logging
import requests
import urllib3
from typing import Dict, Any, List, Optional
from unified_db_mcp.helpers.schema_utils import SchemaInfo, TableInfo, ColumnInfo
from unified_db_mcp.helpers.supabase_api import get_all_supabase_projects, get_supabase_project_api_keys
from unified_db_mcp.database_connectors.base_connector import DatabaseConnector

logger = logging.getLogger(__name__)

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 not available. Direct PostgreSQL connection for apply_schema will not work.")

# Suppress SSL warnings when verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class SupabaseConnector(DatabaseConnector):
    """Supabase connector - uses REST API to fetch schema"""
    
    def connect(self, credentials: Dict[str, Any]):
        """Connect to Supabase"""
        api_key = credentials.get('api_key') or credentials.get('key')
        if not api_key:
            raise ValueError("Supabase API key is required")
        return {"api_key": api_key}
    
    def extract_schema(self, connection, credentials: Dict[str, Any] = None) -> SchemaInfo:
        """Extract schema from Supabase using REST API"""
        api_key = connection.get('api_key') if isinstance(connection, dict) else None
        if not api_key and credentials:
            api_key = credentials.get('api_key') or credentials.get('key')
        if not api_key:
            raise ValueError("Supabase API key is required")
        
        project_name = credentials.get('project_name') if credentials else None
        table_names = credentials.get('table_names') if credentials else None
        if table_names and isinstance(table_names, str):
            table_names = [t.strip() for t in table_names.split(',') if t.strip()]
        
        # Try to extract project_ref from connection string first (most reliable)
        connection_string = credentials.get('connection_string') if credentials else None
        project_ref_from_conn = None
        if connection_string and 'postgres.' in connection_string:
            import re
            match = re.search(r'postgres\.([^:]+)@', connection_string)
            if match:
                project_ref_from_conn = match.group(1)
                logger.info(f"Extracted project_ref '{project_ref_from_conn}' from connection string")
        
        # Get projects
        projects = get_all_supabase_projects(api_key)
        if not projects:
            # API call failed (401, 502, 503, etc.) - try fallback methods
            if project_ref_from_conn:
                # Use project ref from connection string (most reliable)
                project_ref = project_ref_from_conn
                project_name_display = project_name or project_ref
                logger.info(f"Using project_ref '{project_ref}' from connection string")
            elif project_name:
                logger.warning("Supabase API call failed. Using project name as project ref.")
                project_ref = project_name
                project_name_display = project_name
                logger.info(f"Using project name as ref: {project_ref}")
            else:
                raise ValueError(
                    "No Supabase projects found. Supabase API may be unavailable or API key invalid.\n"
                    "Options:\n"
                    "  1. Check your SUPABASE_API_KEY in .env\n"
                    "  2. Provide SUPABASE_PROJECT=your-project-ref\n"
                    "  3. Use SUPABASE_CONNECTION_STRING=postgresql://..."
                )
        else:
            # API succeeded - map project name to project ref
            if project_name:
                projects = [p for p in projects if p.get('name', '').lower() == project_name.lower() or 
                           p.get('ref', '').lower() == project_name.lower()]
                if not projects:
                    raise ValueError(f"Project '{project_name}' not found")
            
            # Extract schema from first project
            project = projects[0]
            project_ref = project.get('ref') or project.get('id')
            project_name_display = project.get('name', 'Unknown')
            logger.info(f"Found project: '{project_name_display}' (ref: {project_ref})")
        project_url = f"https://{project_ref}.supabase.co"
        
        # Get project API keys
        project_keys = get_supabase_project_api_keys(api_key, project_ref)
        project_api_key = (project_keys.get('service_role_key') or 
                          project_keys.get('anon_key') if project_keys else api_key)
        
        # Extract schema via REST API
        schema = self._extract_schema_via_rest(project_api_key, project_url, project_ref, table_names)
        
        # If REST API failed to discover tables and we have connection info, fallback to PostgreSQL
        if not schema.tables and (credentials.get('connection_string') or credentials.get('db_password')):
            logger.info("REST API failed to discover tables. Falling back to direct PostgreSQL connection...")
            # Try to extract project_ref from connection string if available
            connection_string = credentials.get('connection_string')
            if connection_string and 'postgres.' in connection_string:
                # Extract project_ref from connection string: postgres.{project_ref}@...
                import re
                match = re.search(r'postgres\.([^:]+)@', connection_string)
                if match:
                    project_ref_from_conn = match.group(1)
                    logger.info(f"Extracted project_ref '{project_ref_from_conn}' from connection string")
                    project_ref = project_ref_from_conn
            schema = self._extract_schema_via_postgres(credentials, project_ref, table_names)
        
        return schema
    
    def _extract_schema_via_rest(self, api_key: str, project_url: str, project_ref: str, 
                                 table_names: Optional[List[str]] = None) -> SchemaInfo:
        """Extract schema using REST API"""
        base_url = project_url.rstrip('/')
        headers = {
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # First, try to get OpenAPI schema with definitions (contains column info)
        openapi_schema = None
        try:
            schema_url = f"{base_url}/rest/v1/"
            schema_response = requests.get(
                schema_url,
                headers={**headers, "Accept": "application/json"},
                timeout=10,
                verify=False
            )
            if schema_response.status_code == 200:
                schema_data = schema_response.json()
                if isinstance(schema_data, dict) and 'definitions' in schema_data:
                    openapi_schema = schema_data
                    logger.info("Retrieved OpenAPI schema with definitions (contains column info)")
        except Exception as e:
            logger.debug(f"Could not get OpenAPI schema: {e}")
        
        # Discover tables if not provided
        if not table_names:
            table_names = self._discover_tables(base_url, headers)
            if not table_names:
                logger.warning("Could not discover tables. Provide table_names explicitly.")
                return SchemaInfo(database_type='supabase', database_name=project_ref, tables=[])
        
        # Verify and get schema for each table - ONLY migrate tables that actually exist
        logger.info(f"Verifying {len(table_names)} table(s) exist in Supabase...")
        table_infos = []
        verified_count = 0
        skipped_count = 0
        
        for table_name in table_names:
            logger.info(f"Checking if table '{table_name}' exists in Supabase...")
            if self._verify_table_exists(base_url, headers, table_name):
                logger.info(f"  Table '{table_name}' exists - will migrate")
                
                # Try to get columns from OpenAPI schema first (fastest, works for empty tables)
                columns = None
                if openapi_schema:
                    columns = self._get_columns_from_openapi(openapi_schema, table_name)
                
                # Fallback to other methods if OpenAPI didn't work
                if not columns:
                    columns = self._get_table_columns(base_url, headers, table_name)
                
                if columns:
                    table_infos.append(TableInfo(name=table_name, columns=columns, indexes=[]))
                    verified_count += 1
                else:
                    logger.warning(f"  Table '{table_name}' exists but could not get schema")
            else:
                logger.warning(f"  Table '{table_name}' does not exist in Supabase - skipping")
                skipped_count += 1
        
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} table(s) that do not exist in Supabase")
        
        return SchemaInfo(database_type='supabase', database_name=project_ref, tables=table_infos)
    
    def _extract_schema_via_postgres(self, credentials: Dict[str, Any], project_ref: str, 
                                     table_names: Optional[List[str]] = None) -> SchemaInfo:
        """Extract schema using direct PostgreSQL connection"""
        from psycopg2.extras import RealDictCursor
        import re
        
        # Build connection string
        connection_string = credentials.get('connection_string')
        if not connection_string:
            db_password = credentials.get('db_password')
            if db_password:
                from urllib.parse import quote_plus
                encoded_password = quote_plus(db_password)
                project_region = credentials.get('region', 'ap-southeast-1')
                # Try to extract actual project_ref from connection string if available
                conn_str_check = credentials.get('connection_string')
                if conn_str_check and 'postgres.' in conn_str_check:
                    match = re.search(r'postgres\.([^:]+)@', conn_str_check)
                    if match:
                        project_ref = match.group(1)
                        logger.info(f"Using project_ref '{project_ref}' from connection string")
                connection_string = f"postgresql://postgres.{project_ref}:{encoded_password}@aws-1-{project_region}.pooler.supabase.com:5432/postgres"
        else:
            # Extract project_ref from connection string for logging
            if 'postgres.' in connection_string:
                match = re.search(r'postgres\.([^:]+)@', connection_string)
                if match:
                    extracted_ref = match.group(1)
                    logger.info(f"Using project_ref '{extracted_ref}' from connection string")
        
        if not connection_string:
            raise ValueError("No connection string or password available for PostgreSQL fallback")
        
        logger.info("Connecting to Supabase via PostgreSQL...")
        pg_conn = psycopg2.connect(connection_string, connect_timeout=10)
        
        try:
            cursor = pg_conn.cursor(cursor_factory=RealDictCursor)
            
            # Get all tables
            if table_names:
                # Filter to specific tables
                placeholders = ','.join(['%s'] * len(table_names))
                cursor.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    AND table_name IN ({placeholders})
                    ORDER BY table_name
                """, table_names)
            else:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
            tables = [row['table_name'] for row in cursor.fetchall()]
            
            if not tables:
                logger.warning("No tables found in Supabase database")
                return SchemaInfo(database_type='supabase', database_name=project_ref, tables=[])
            
            logger.info(f"Found {len(tables)} table(s) via PostgreSQL connection")
            
            table_infos = []
            for table_name in tables:
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
                    
                    column_info = ColumnInfo(
                        name=col['column_name'],
                        data_type=col['data_type'],
                        is_nullable=col['is_nullable'] == 'YES',
                        default_value=col['column_default'],
                        character_maximum_length=col['character_maximum_length'],
                        numeric_precision=col['numeric_precision'],
                        numeric_scale=col['numeric_scale'],
                        is_primary_key=is_pk
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
            
            return SchemaInfo(database_type='supabase', database_name=project_ref, tables=table_infos)
        finally:
            pg_conn.close()
        if verified_count > 0:
            logger.info(f"Verified {verified_count} table(s) exist and will be migrated")
        
        return SchemaInfo(database_type='supabase', database_name=project_ref, tables=table_infos)
    
    def _discover_tables(self, base_url: str, headers: Dict[str, str]) -> List[str]:
        """Discover tables by querying Supabase OpenAPI schema"""
        logger.info("Querying Supabase to discover tables...")
        
        # Try multiple endpoints
        endpoints = [
            f"{base_url}/rest/v1/",
            f"{base_url}/rest/v1",
        ]
        
        for endpoint in endpoints:
            try:
                # Try with SSL verification first
                response = requests.get(endpoint, headers=headers, timeout=10, verify=True)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"OpenAPI response type: {type(data)}")
                    if isinstance(data, dict):
                        paths = data.get('paths', {})
                        logger.debug(f"Found {len(paths)} paths in OpenAPI schema")
                        tables = []
                        
                        for path in paths.keys():
                            if path.startswith('/rest/v1/') and path != '/rest/v1/':
                                # Extract table name more carefully
                                path_clean = path.replace('/rest/v1/', '').strip('/')
                                # Handle paths like /rest/v1/table_name or /rest/v1/table_name/{id}
                                table_name = path_clean.split('/')[0]
                                table_name = table_name.split('?')[0].split('{')[0].split('}')[0].strip()
                                
                                excluded = ['rpc', 'auth', 'storage', 'realtime', 'rest', '', 'information_schema', 'pg_']
                                # Exclude system schemas and special endpoints
                                if (table_name and 
                                    table_name not in excluded and 
                                    not table_name.startswith('pg_') and
                                    not table_name.startswith('_') and
                                    table_name not in tables):
                                    tables.append(table_name)
                        
                        if tables:
                            logger.info(f"Discovered {len(tables)} tables: {', '.join(tables)}")
                            return tables
                        else:
                            logger.debug("OpenAPI schema found but no table paths detected")
                    else:
                        logger.debug(f"OpenAPI response is not a dict: {type(data)}")
                
            except requests.exceptions.SSLError:
                # Try without SSL verification as fallback
                try:
                    logger.warning("SSL error, trying without verification...")
                    response = requests.get(endpoint, headers=headers, timeout=10, verify=False)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            logger.debug(f"OpenAPI response (no SSL): {type(data)}")
                            if isinstance(data, dict):
                                paths = data.get('paths', {})
                                logger.debug(f"Found {len(paths)} paths in OpenAPI schema")
                                if paths:
                                    logger.debug(f"Sample paths: {list(paths.keys())[:5]}")
                                tables = []
                                for path in paths.keys():
                                    if path.startswith('/rest/v1/') and path != '/rest/v1/':
                                        # Extract table name more carefully
                                        path_clean = path.replace('/rest/v1/', '').strip('/')
                                        table_name = path_clean.split('/')[0]
                                        table_name = table_name.split('?')[0].split('{')[0].split('}')[0].strip()
                                        
                                        excluded = ['rpc', 'auth', 'storage', 'realtime', 'rest', '', 'information_schema', 'pg_']
                                        # Exclude system schemas and special endpoints
                                        if (table_name and 
                                            table_name not in excluded and 
                                            not table_name.startswith('pg_') and
                                            not table_name.startswith('_') and
                                            table_name not in tables):
                                            tables.append(table_name)
                                if tables:
                                    logger.info(f"Discovered {len(tables)} tables: {', '.join(tables)}")
                                    return tables
                                else:
                                    logger.warning("OpenAPI schema found but no table paths detected")
                                    logger.debug(f"All paths: {list(paths.keys())[:20]}")
                            else:
                                logger.warning(f"OpenAPI response is not a dict, got: {type(data)}")
                                logger.debug(f"Response content (first 500 chars): {str(data)[:500]}")
                        except Exception as json_error:
                            logger.warning(f"Failed to parse JSON response: {json_error}")
                            logger.debug(f"Response text (first 500 chars): {response.text[:500]}")
                    elif response.status_code == 404:
                        logger.warning(f"Endpoint {endpoint} returned 404 - endpoint not found")
                    else:
                        logger.warning(f"Endpoint {endpoint} returned status {response.status_code}")
                        logger.debug(f"Response: {response.text[:200]}")
                except Exception as e:
                    logger.debug(f"Error with endpoint {endpoint} (no SSL): {e}")
                    continue
            except Exception as e:
                logger.debug(f"Error with endpoint {endpoint}: {e}")
                continue
        
        # Alternative: Try PostgREST schema endpoint
        logger.info("Trying PostgREST schema endpoint...")
        try:
            schema_response = requests.get(
                f"{base_url}/rest/v1/",
                headers={**headers, "Accept": "application/vnd.pgjson.object+json"},
                timeout=10,
                verify=False
            )
            if schema_response.status_code == 200:
                logger.debug("PostgREST schema endpoint responded")
        except Exception as e:
            logger.debug(f"PostgREST schema endpoint failed: {e}")
        
        # Method 2: Try querying information_schema via RPC
        logger.info("Trying information_schema query via RPC...")
        try:
            # Try common RPC function names for getting tables
            rpc_functions = [
                'get_tables',
                'list_tables',
                'get_table_names',
                'pg_tables',
            ]
            
            for rpc_func in rpc_functions:
                try:
                    rpc_url = f"{base_url}/rest/v1/rpc/{rpc_func}"
                    rpc_response = requests.post(
                        rpc_url,
                        headers={**headers, "Content-Type": "application/json"},
                        json={},
                        timeout=10,
                        verify=False
                    )
                    if rpc_response.status_code == 200:
                        tables_data = rpc_response.json()
                        if isinstance(tables_data, list) and len(tables_data) > 0:
                            # Extract table names from response
                            tables = []
                            for item in tables_data:
                                if isinstance(item, dict):
                                    table_name = item.get('table_name') or item.get('tablename') or item.get('name')
                                    if table_name:
                                        tables.append(table_name)
                                elif isinstance(item, str):
                                    tables.append(item)
                            if tables:
                                logger.info(f"Discovered {len(tables)} tables via RPC: {', '.join(tables)}")
                                return tables
                except Exception as rpc_error:
                    logger.debug(f"RPC function {rpc_func} failed: {rpc_error}")
                    continue
        except Exception as e:
            logger.debug(f"RPC query failed: {e}")
        
        # Method 3: Try querying information_schema.tables directly via REST
        logger.info("Trying information_schema.tables query via REST...")
        try:
            # Try to query information_schema.tables as if it's a regular table
            info_schema_url = f"{base_url}/rest/v1/information_schema.tables"
            info_headers = {
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            info_params = {
                "table_schema": "eq.public",
                "table_type": "eq.BASE TABLE",
                "select": "table_name"
            }
            info_response = requests.get(
                info_schema_url,
                headers=info_headers,
                params=info_params,
                timeout=10,
                verify=False
            )
            if info_response.status_code == 200:
                info_data = info_response.json()
                if isinstance(info_data, list):
                    tables = [item.get('table_name') for item in info_data if isinstance(item, dict) and item.get('table_name')]
                    if tables:
                        logger.info(f"Discovered {len(tables)} tables via information_schema: {', '.join(tables)}")
                        return tables
        except Exception as e:
            logger.debug(f"information_schema.tables query failed: {e}")
        
        # Method 3b: Try querying pg_tables directly via REST
        logger.info("Trying pg_tables query via REST...")
        try:
            # Try to access pg_tables view (if exposed)
            pg_tables_url = f"{base_url}/rest/v1/pg_tables"
            pg_response = requests.get(
                pg_tables_url,
                headers={**headers, "Accept": "application/json"},
                params={"schemaname": "eq.public", "select": "tablename"},
                timeout=10,
                verify=False
            )
            if pg_response.status_code == 200:
                pg_data = pg_response.json()
                if isinstance(pg_data, list):
                    tables = [item.get('tablename') for item in pg_data if isinstance(item, dict) and item.get('tablename')]
                    if tables:
                        logger.info(f"Discovered {len(tables)} tables via pg_tables: {', '.join(tables)}")
                        return tables
        except Exception as e:
            logger.debug(f"pg_tables query failed: {e}")
        
        # Method 4: Try probing common table names and build list from successful probes
        logger.info("Trying table name probing method...")
        try:
            # This is a fallback: try common table name patterns
            # But first, let's try to get schema info from PostgREST
            schema_url = f"{base_url}/rest/v1/"
            schema_headers = {
                **headers,
                "Accept": "application/json",
                "Prefer": "return=representation"
            }
            schema_response = requests.get(schema_url, headers=schema_headers, timeout=10, verify=False)
            
            if schema_response.status_code == 200:
                # Try to parse the response - sometimes PostgREST returns schema info
                try:
                    schema_data = schema_response.json()
                    # Look for any hints about available tables
                    if isinstance(schema_data, dict):
                        # Check if it's an OpenAPI-like structure
                        if 'definitions' in schema_data:
                            tables = list(schema_data.get('definitions', {}).keys())
                            # Filter out non-table definitions
                            tables = [t for t in tables if not t.startswith('_') and t not in ['rpc', 'auth']]
                            if tables:
                                logger.info(f"Discovered {len(tables)} tables from schema definitions: {', '.join(tables)}")
                                return tables
                except:
                    pass
        except Exception as e:
            logger.debug(f"Schema probing failed: {e}")
        
        # Method 5: Try HEAD requests on potential table endpoints (probing method)
        logger.info("Trying table endpoint probing...")
        try:
            # This is a last resort: try common table name patterns
            # But this is inefficient, so we'll skip it for now
            # Instead, let's try one more OpenAPI endpoint variation
            openapi_endpoints = [
                f"{base_url}/rest/v1/",
                f"{base_url}/rest/v1",
                f"{base_url}/rest/v1/?format=openapi",
                f"{base_url}/rest/v1/?openapi",
            ]
            
            for openapi_endpoint in openapi_endpoints:
                try:
                    openapi_response = requests.get(
                        openapi_endpoint,
                        headers={**headers, "Accept": "application/openapi+json,application/json"},
                        timeout=10,
                        verify=False
                    )
                    if openapi_response.status_code == 200:
                        openapi_data = openapi_response.json()
                        # Try to extract from OpenAPI 3.0 format
                        if isinstance(openapi_data, dict):
                            # Check for OpenAPI 3.0 structure
                            if 'paths' in openapi_data:
                                paths = openapi_data['paths']
                                tables = []
                                for path_key in paths.keys():
                                    # Extract table name from path
                                    if path_key.startswith('/rest/v1/') and path_key != '/rest/v1/':
                                        parts = path_key.replace('/rest/v1/', '').split('/')
                                        table_name = parts[0].split('?')[0].split('{')[0].strip()
                                        excluded = ['rpc', 'auth', 'storage', 'realtime', 'rest', '', 'information_schema']
                                        if table_name and table_name not in excluded and table_name not in tables:
                                            # Verify it's actually a table by checking if GET is available
                                            path_info = paths.get(path_key, {})
                                            if 'get' in path_info or 'post' in path_info:
                                                tables.append(table_name)
                                if tables:
                                    logger.info(f"Discovered {len(tables)} tables from OpenAPI paths: {', '.join(tables)}")
                                    return tables
                except Exception as openapi_error:
                    logger.debug(f"OpenAPI endpoint {openapi_endpoint} failed: {openapi_error}")
                    continue
        except Exception as e:
            logger.debug(f"Table probing failed: {e}")
        
        logger.error("Could not discover tables automatically from Supabase OpenAPI.")
        logger.info("")
        logger.info("   To migrate specific tables, use one of these methods:")
        logger.info("")
        logger.info("   Method 1: Command line argument")
        logger.info("      python schema_migrate.py --tables table1,table2,table3")
        logger.info("")
        logger.info("   Method 2: Get table names from Supabase Dashboard")
        logger.info("      -> Go to: Supabase Dashboard -> Table Editor")
        logger.info("      -> Copy the table names you want to migrate")
        logger.info("")
        logger.info("   Method 3: Query Supabase directly")
        logger.info("      -> Use Supabase SQL Editor to run:")
        logger.info("        SELECT table_name FROM information_schema.tables")
        logger.info("        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';")
        logger.info("")
        logger.info("   Tables will be verified to exist before migrating")
        return []
    
    def _verify_table_exists(self, base_url: str, headers: Dict[str, str], table_name: str) -> bool:
        """Verify table exists by actually checking Supabase REST API"""
        try:
            # Try with SSL verification first
            response = requests.get(
                f"{base_url}/rest/v1/{table_name}",
                headers=headers,
                params={"limit": "1"},
                timeout=10,
                verify=True
            )
            
            status = response.status_code
            if status == 404:
                logger.info(f"  Table '{table_name}' does not exist in Supabase (404)")
                return False
            elif status in [200, 401, 403]:
                logger.info(f"  Table '{table_name}' exists in Supabase (status: {status})")
                return True
            else:
                logger.warning(f"  ? Table '{table_name}' returned status: {status}")
                return False
                
        except requests.exceptions.SSLError:
            # Fallback: try without SSL verification
            try:
                logger.warning(f"  SSL error checking '{table_name}', trying without verification...")
                response = requests.get(
                    f"{base_url}/rest/v1/{table_name}",
                    headers=headers,
                    params={"limit": "1"},
                    timeout=10,
                    verify=False
                )
                status = response.status_code
                if status == 404:
                    logger.info(f"  Table '{table_name}' does not exist in Supabase (404)")
                    return False
                elif status in [200, 401, 403]:
                    logger.info(f"  Table '{table_name}' exists in Supabase (status: {status})")
                    return True
                return False
            except Exception as e:
                logger.warning(f"  Error checking table '{table_name}': {e}")
                return False
        except Exception as e:
            logger.warning(f"  Error checking table '{table_name}': {e}")
            return False
    
    def _get_columns_from_openapi(self, openapi_schema: Dict, table_name: str) -> Optional[List[ColumnInfo]]:
        """Extract column schema from OpenAPI definitions"""
        try:
            # Try different OpenAPI schema structures
            # Structure 1: definitions.table_name.properties
            definitions = openapi_schema.get('definitions', {})
            table_def = definitions.get(table_name)
            
            # Structure 2: components.schemas.table_name (OpenAPI 3.0)
            if not table_def:
                components = openapi_schema.get('components', {})
                schemas = components.get('schemas', {}) if components else {}
                table_def = schemas.get(table_name)
            
            # Structure 3: Check paths for table schema
            if not table_def:
                paths = openapi_schema.get('paths', {})
                table_path = f"/rest/v1/{table_name}"
                path_info = paths.get(table_path, {})
                # Look for schema in GET response
                get_op = path_info.get('get', {})
                responses = get_op.get('responses', {})
                success_response = responses.get('200', {})
                content = success_response.get('content', {})
                json_content = content.get('application/json', {})
                schema_ref = json_content.get('schema', {})
                
                # If it's a reference, resolve it
                if '$ref' in schema_ref:
                    ref_path = schema_ref['$ref'].replace('#/definitions/', '').replace('#/components/schemas/', '')
                    table_def = definitions.get(ref_path) or schemas.get(ref_path)
                elif 'items' in schema_ref:
                    # Array response - get schema from items
                    items_schema = schema_ref.get('items', {})
                    if '$ref' in items_schema:
                        ref_path = items_schema['$ref'].replace('#/definitions/', '').replace('#/components/schemas/', '')
                        table_def = definitions.get(ref_path) or schemas.get(ref_path)
                    else:
                        table_def = items_schema
            
            if not table_def:
                logger.debug(f"  Table '{table_name}' not found in OpenAPI definitions")
                return None
            
            # Extract properties (columns)
            properties = table_def.get('properties', {})
            required = table_def.get('required', [])
            
            if not properties:
                logger.debug(f"  No properties found for table '{table_name}' in OpenAPI")
                return None
            
            columns = []
            for col_name, col_schema in properties.items():
                if not isinstance(col_schema, dict):
                    continue
                
                # Extract data type - handle $ref and nested types
                data_type = col_schema.get('type', '')
                format_type = col_schema.get('format', '')
                
                # Handle $ref (type reference)
                if '$ref' in col_schema:
                    ref_path = col_schema['$ref'].replace('#/definitions/', '').replace('#/components/schemas/', '')
                    ref_def = definitions.get(ref_path) or schemas.get(ref_path) if 'schemas' in locals() else None
                    if ref_def:
                        data_type = ref_def.get('type', 'text')
                        format_type = ref_def.get('format', '')
                
                # Map OpenAPI types to PostgreSQL types
                pg_type = 'text'  # default
                if format_type == 'uuid':
                    pg_type = 'uuid'
                elif format_type == 'date-time':
                    pg_type = 'timestamp'
                elif format_type == 'date':
                    pg_type = 'date'
                elif format_type == 'time':
                    pg_type = 'time'
                elif data_type == 'integer':
                    pg_type = 'integer'
                elif data_type == 'number':
                    pg_type = 'numeric'
                elif data_type == 'boolean':
                    pg_type = 'boolean'
                elif data_type == 'string':
                    # Check if it's varchar with length
                    max_length = col_schema.get('maxLength')
                    if max_length:
                        pg_type = 'varchar'
                    else:
                        pg_type = 'text'
                
                # Get constraints
                is_nullable = col_name not in required
                char_length = col_schema.get('maxLength')
                numeric_precision = None
                numeric_scale = None
                
                # Handle numeric precision/scale from format
                if data_type == 'number':
                    # Try to infer from example or default
                    pass
                
                # Check if it's likely a primary key
                is_pk = (col_name == 'id' or col_name.endswith('_id'))
                
                columns.append(ColumnInfo(
                    name=col_name,
                    data_type=pg_type,
                    is_nullable=is_nullable,
                    default_value=None,  # OpenAPI doesn't always have defaults
                    character_maximum_length=int(char_length) if char_length else None,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    is_primary_key=is_pk,
                    is_foreign_key=False
                ))
            
            if columns:
                logger.info(f"  Got schema for '{table_name}' ({len(columns)} columns) via OpenAPI definitions")
                return columns
            else:
                logger.debug(f"  No columns extracted from OpenAPI for '{table_name}'")
        except Exception as e:
            logger.debug(f"  Failed to extract columns from OpenAPI for {table_name}: {e}")
            import traceback
            logger.debug(f"  OpenAPI extraction traceback: {traceback.format_exc()}")
        
        return None
    
    def _get_table_columns(self, base_url: str, headers: Dict[str, str], table_name: str) -> List[ColumnInfo]:
        """Get table columns by querying information_schema.columns"""
        logger.info(f"  Extracting schema for table '{table_name}'...")
        
        # Method 1: Try querying information_schema.columns directly via REST
        try:
            logger.debug(f"  Method 1: Trying information_schema.columns for {table_name}...")
            info_schema_url = f"{base_url}/rest/v1/information_schema.columns"
            info_headers = {
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            # Try different parameter formats
            param_variations = [
                {
                    "table_schema": "eq.public",
                    "table_name": f"eq.{table_name}",
                    "select": "column_name,data_type,is_nullable,column_default,character_maximum_length,numeric_precision,numeric_scale"
                },
                {
                    "table_schema": "public",
                    "table_name": table_name,
                    "select": "column_name,data_type,is_nullable,column_default,character_maximum_length,numeric_precision,numeric_scale"
                }
            ]
            
            for info_params in param_variations:
                try:
                    response = requests.get(
                        info_schema_url,
                        headers=info_headers,
                        params=info_params,
                        timeout=10,
                        verify=False
                    )
                    
                    logger.debug(f"  information_schema.columns query status: {response.status_code} for {table_name}")
                    
                    if response.status_code == 200:
                        columns_data = response.json()
                        if isinstance(columns_data, list) and len(columns_data) > 0:
                            columns = []
                            for col_data in columns_data:
                                col_name = col_data.get('column_name')
                                if not col_name:
                                    continue
                                    
                                data_type = col_data.get('data_type', 'text')
                                is_nullable = col_data.get('is_nullable', 'YES') == 'YES'
                                default_value = col_data.get('column_default')
                                char_length = col_data.get('character_maximum_length')
                                numeric_precision = col_data.get('numeric_precision')
                                numeric_scale = col_data.get('numeric_scale')
                                
                                columns.append(ColumnInfo(
                                    name=col_name,
                                    data_type=data_type,
                                    is_nullable=is_nullable,
                                    default_value=str(default_value) if default_value else None,
                                    character_maximum_length=int(char_length) if char_length else None,
                                    numeric_precision=int(numeric_precision) if numeric_precision else None,
                                    numeric_scale=int(numeric_scale) if numeric_scale else None,
                                    is_primary_key=False,  # Will be set below
                                    is_foreign_key=False
                                ))
                            
                            if columns:
                                # Get primary key information
                                pk_columns = self._get_primary_keys(base_url, headers, table_name)
                                for col in columns:
                                    if col.name in pk_columns:
                                        col.is_primary_key = True
                                
                                logger.info(f"  Got schema for '{table_name}' ({len(columns)} columns) via information_schema")
                                return columns
                        elif response.status_code == 404:
                            logger.debug(f"  information_schema.columns endpoint not found (404)")
                            break
                        else:
                            logger.debug(f"  information_schema.columns returned status {response.status_code}: {response.text[:200]}")
                except Exception as param_error:
                    logger.debug(f"  Parameter variation failed: {param_error}")
                    continue
        except Exception as e:
            logger.debug(f"  information_schema.columns query failed for {table_name}: {e}")
        
        # Method 2: Try using RPC function to query information_schema
        logger.debug(f"  Trying RPC function to get schema for {table_name}...")
        try:
            # Try to create/use an RPC function to get table columns
            # First, try if there's a built-in function
            rpc_functions = [
                'get_table_columns',
                'get_columns',
                'table_columns',
            ]
            
            for rpc_func in rpc_functions:
                try:
                    rpc_url = f"{base_url}/rest/v1/rpc/{rpc_func}"
                    rpc_payload = {"table_name": table_name, "schema_name": "public"}
                    rpc_response = requests.post(
                        rpc_url,
                        headers={**headers, "Content-Type": "application/json"},
                        json=rpc_payload,
                        timeout=10,
                        verify=False
                    )
                    if rpc_response.status_code == 200:
                        rpc_data = rpc_response.json()
                        if isinstance(rpc_data, list) and len(rpc_data) > 0:
                            # Parse RPC response
                            columns = []
                            for col_data in rpc_data:
                                if isinstance(col_data, dict):
                                    col_name = col_data.get('column_name') or col_data.get('name')
                                    if col_name:
                                        columns.append(ColumnInfo(
                                            name=col_name,
                                            data_type=col_data.get('data_type', 'text'),
                                            is_nullable=col_data.get('is_nullable', True),
                                            default_value=str(col_data.get('default_value')) if col_data.get('default_value') else None,
                                            character_maximum_length=col_data.get('character_maximum_length'),
                                            numeric_precision=col_data.get('numeric_precision'),
                                            numeric_scale=col_data.get('numeric_scale'),
                                            is_primary_key=col_data.get('is_primary_key', False),
                                            is_foreign_key=False
                                        ))
                            if columns:
                                logger.info(f"  Got schema for '{table_name}' ({len(columns)} columns) via RPC")
                                return columns
                except Exception as rpc_error:
                    logger.debug(f"  RPC function {rpc_func} failed: {rpc_error}")
                    continue
        except Exception as e:
            logger.debug(f"  RPC query failed for {table_name}: {e}")
        
        # Method 2: Try querying pg_attribute via REST (PostgreSQL system catalog)
        logger.info(f"  Method 2: Trying pg_attribute/pg_class query for {table_name}...")
        try:
            # Query pg_attribute which contains column information
            pg_attr_url = f"{base_url}/rest/v1/pg_attribute"
            pg_headers = {
                **headers,
                "Accept": "application/json"
            }
            # We need to join with pg_class to get table oid, but REST API might not support joins
            # Try a simpler approach: query pg_class first to get table oid
            pg_class_url = f"{base_url}/rest/v1/pg_class"
            pg_class_params = {
                "relname": f"eq.{table_name}",
                "relnamespace": "eq.2200",  # public schema oid
                "select": "oid,relname"
            }
            
            class_response = requests.get(pg_class_url, headers=pg_headers, params=pg_class_params, timeout=10, verify=False)
            logger.debug(f"  pg_class query status: {class_response.status_code} for {table_name}")
            
            if class_response.status_code == 200:
                class_data = class_response.json()
                if isinstance(class_data, list) and len(class_data) > 0:
                    table_oid = class_data[0].get('oid')
                    if table_oid:
                        # Now query pg_attribute for this table
                        pg_attr_params = {
                            "attrelid": f"eq.{table_oid}",
                            "attnum": "gt.0",  # Exclude system columns (negative attnum)
                            "select": "attname,atttypid,attnotnull,atthasdef,attlen,attnum"
                        }
                        attr_response = requests.get(pg_attr_url, headers=pg_headers, params=pg_attr_params, timeout=10, verify=False)
                        logger.debug(f"  pg_attribute query status: {attr_response.status_code} for {table_name}, oid: {table_oid}")
                        
                        if attr_response.status_code == 200:
                            attr_data = attr_response.json()
                            if isinstance(attr_data, list) and len(attr_data) > 0:
                                # Map PostgreSQL type OIDs to type names (common OIDs)
                                # This is a simplified mapping - full mapping would require querying pg_type
                                common_type_map = {
                                    16: 'boolean', 20: 'bigint', 21: 'smallint', 23: 'integer',
                                    25: 'text', 700: 'real', 701: 'double precision', 1043: 'varchar',
                                    1082: 'date', 1083: 'time', 1114: 'timestamp', 1184: 'timestamptz',
                                    2950: 'uuid'
                                }
                                columns = []
                                for attr in attr_data:
                                    col_name = attr.get('attname')
                                    type_oid = attr.get('atttypid')
                                    is_not_null = attr.get('attnotnull', False)
                                    has_default = attr.get('atthasdef', False)
                                    
                                    # Get type name from map
                                    data_type = common_type_map.get(type_oid, 'text')
                                    
                                    columns.append(ColumnInfo(
                                        name=col_name,
                                        data_type=data_type,
                                        is_nullable=not is_not_null,
                                        default_value=None,  # Can't get default from pg_attribute easily
                                        character_maximum_length=None,
                                        numeric_precision=None,
                                        numeric_scale=None,
                                        is_primary_key=(col_name == 'id'),  # Will be updated below
                                        is_foreign_key=False
                                    ))
                                
                                if columns:
                                    # Get primary keys
                                    pk_columns = self._get_primary_keys(base_url, headers, table_name)
                                    for col in columns:
                                        if col.name in pk_columns:
                                            col.is_primary_key = True
                                    
                                    logger.info(f"  Got schema for '{table_name}' ({len(columns)} columns) via pg_attribute")
                                    return columns
        except Exception as e:
            logger.warning(f"  pg_attribute query failed for {table_name}: {e}")
            import traceback
            logger.debug(f"  pg_attribute traceback: {traceback.format_exc()}")
        
        # Method 3: Fallback - Try querying sample data (for tables with data)
        try:
            response = requests.get(
                f"{base_url}/rest/v1/{table_name}",
                headers=headers,
                params={"limit": "1", "select": "*"},
                timeout=10,
                verify=False
            )
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    logger.info(f"  Got schema for '{table_name}' ({len(data[0].keys())} columns) via sample data")
                    return self._infer_columns_from_data(data)
                else:
                    # Table exists but is empty - try alternative methods
                    logger.debug(f"  Table '{table_name}' is empty, trying alternative methods...")
                    
                    # Method 3: Try querying with OPTIONS to get column info
                    try:
                        options_response = requests.options(
                            f"{base_url}/rest/v1/{table_name}",
                            headers={**headers, "Accept": "application/json"},
                            timeout=10,
                            verify=False
                        )
                        if options_response.status_code == 200:
                            # OPTIONS might return schema info in headers
                            logger.debug(f"  OPTIONS request successful for {table_name}")
                    except:
                        pass
                    
                    # Method 4: Try to get schema from OpenAPI definitions we discovered earlier
                    # This is handled in the discovery phase, but we can't access it here
                    # So we'll log the issue
                    logger.warning(f"  Table '{table_name}' exists but is empty - trying information_schema query...")
        except Exception as e:
            logger.debug(f"  Sample data query failed for {table_name}: {e}")
        
        return []
    
    def _get_primary_keys(self, base_url: str, headers: Dict[str, str], table_name: str) -> List[str]:
        """Get primary key column names for a table"""
        try:
            # Query constraint_column_usage or key_column_usage
            pk_url = f"{base_url}/rest/v1/information_schema.table_constraints"
            pk_headers = {
                **headers,
                "Accept": "application/json"
            }
            pk_params = {
                "table_schema": "eq.public",
                "table_name": f"eq.{table_name}",
                "constraint_type": "eq.PRIMARY KEY",
                "select": "constraint_name"
            }
            
            response = requests.get(pk_url, headers=pk_headers, params=pk_params, timeout=10, verify=False)
            if response.status_code == 200:
                constraints = response.json()
                if constraints and len(constraints) > 0:
                    constraint_name = constraints[0].get('constraint_name')
                    if constraint_name:
                        # Now get columns for this constraint
                        key_url = f"{base_url}/rest/v1/information_schema.key_column_usage"
                        key_params = {
                            "table_schema": "eq.public",
                            "table_name": f"eq.{table_name}",
                            "constraint_name": f"eq.{constraint_name}",
                            "select": "column_name"
                        }
                        key_response = requests.get(key_url, headers=pk_headers, params=key_params, timeout=10, verify=False)
                        if key_response.status_code == 200:
                            key_data = key_response.json()
                            return [item.get('column_name') for item in key_data if isinstance(item, dict) and item.get('column_name')]
        except Exception as e:
            logger.debug(f"  Could not get primary keys for {table_name}: {e}")
        
        # Fallback: assume 'id' is primary key if it exists
        return []
    
    def _infer_columns_from_data(self, data: List[Dict]) -> List[ColumnInfo]:
        """Infer column schema from sample data"""
        if not data:
            return []
        
        columns = []
        first_row = data[0]
        
        for col_name, col_value in first_row.items():
            # Infer data type
            if col_value is None:
                data_type = "text"
                is_nullable = True
            elif isinstance(col_value, bool):
                data_type = "boolean"
                is_nullable = False
            elif isinstance(col_value, int):
                data_type = "integer" if -2147483648 <= col_value <= 2147483647 else "bigint"
                is_nullable = False
            elif isinstance(col_value, float):
                data_type = "numeric"
                is_nullable = False
            elif isinstance(col_value, str):
                if len(col_value) == 36 and col_value.count('-') == 4:
                    data_type = "uuid"
                else:
                    data_type = "text"
                is_nullable = False
            else:
                data_type = "text"
                is_nullable = True
            
            # Check nullability
            has_nulls = any(row.get(col_name) is None for row in data)
            
            columns.append(ColumnInfo(
                name=col_name,
                data_type=data_type,
                is_nullable=has_nulls or is_nullable,
                default_value=None,
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
                is_primary_key=(col_name == "id"),
                is_foreign_key=False
            ))
        
        return columns
    
    def apply_schema(self, connection, schema: SchemaInfo, credentials: Dict[str, Any] = None):
        """Apply schema to Supabase using direct PostgreSQL connection"""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 is required for applying schema to Supabase. Install it with: pip install psycopg2-binary")
        
        # Get connection details
        api_key = connection.get('api_key') if isinstance(connection, dict) else None
        if not api_key and credentials:
            api_key = credentials.get('api_key') or credentials.get('key')
        if not api_key:
            raise ValueError("Supabase API key is required")
        
        project_name = credentials.get('project_name') if credentials else None
        project_ref = credentials.get('project_ref') if credentials else None  # Allow direct project ref
        
        logger.info(f"Project lookup: name='{project_name}', ref='{project_ref}'")
        
        # If project_ref is provided directly, use it (bypasses API call)
        if project_ref:
            logger.info(f"Using provided project ref: {project_ref}")
            project_name_display = project_name or project_ref
        else:
            # Get projects from API
            try:
                projects = get_all_supabase_projects(api_key)
                if not projects:
                    raise ValueError("No Supabase projects found")
                
                logger.debug(f"Found {len(projects)} project(s) in your account")
                
                # Filter project if specified
                if project_name:
                    logger.info(f"Looking for project with name: '{project_name}'")
                    matching_projects = [p for p in projects if p.get('name', '').lower() == project_name.lower() or 
                                        p.get('ref', '').lower() == project_name.lower()]
                    if not matching_projects:
                        # List available projects for debugging
                        available_names = [p.get('name', 'Unknown') for p in projects]
                        logger.error(f"Project '{project_name}' not found!")
                        logger.error(f"Available projects: {', '.join(available_names)}")
                        raise ValueError(f"Project '{project_name}' not found. Available projects: {', '.join(available_names)}")
                    
                    project = matching_projects[0]
                    project_ref = project.get('ref') or project.get('id')
                    project_name_display = project.get('name', 'Unknown')
                    
                    logger.info(f"Found project: '{project_name_display}' (ref: {project_ref})")
                else:
                    # No project name specified - use first project (with warning)
                    logger.warning("No project name specified - using first available project")
                    project = projects[0]
                    project_ref = project.get('ref') or project.get('id')
                    project_name_display = project.get('name', 'Unknown')
                    logger.info(f"Using first project: '{project_name_display}' (ref: {project_ref})")
            except Exception as api_error:
                error_msg = str(api_error)
                if "502" in error_msg or "Bad Gateway" in error_msg or "503" in error_msg:
                    # API is temporarily down - if we have project_name, try using it as ref
                    if project_name:
                        logger.warning("Supabase API is temporarily unavailable (502/503). Using project name as project ref.")
                        project_ref = project_name
                        project_name_display = project_name
                        logger.info(f"Using project name as ref: {project_ref}")
                    else:
                        raise ValueError(
                            f"Supabase API is temporarily unavailable (502 Bad Gateway).\n"
                            f"Options:\n"
                            f"  1. Wait a few minutes and try again\n"
                            f"  2. Provide project_ref directly: SUPABASE_PROJECT_REF=your-project-ref\n"
                            f"  3. Use connection string: SUPABASE_CONNECTION_STRING=postgresql://..."
                        )
                else:
                    raise
        
        # Check if project has connection info in API response (if available)
        project_db_host = None
        project_region = 'ap-southeast-1'  # Default region (most common)
        try:
            if 'project' in locals() and project:
                project_db_host = project.get('db_host') or project.get('database_host')
                # Get region from API response
                api_region = project.get('region') or project.get('cloud_provider')
                if api_region:
                    # Map common region formats
                    if 'ap-southeast' in api_region.lower() or 'singapore' in api_region.lower():
                        project_region = 'ap-southeast-1'
                    elif 'us-east' in api_region.lower():
                        project_region = 'us-east-1'
                    elif 'us-west' in api_region.lower():
                        project_region = 'us-west-1'
                    elif 'eu-west' in api_region.lower():
                        project_region = 'eu-west-1'
                    elif 'ap-northeast' in api_region.lower():
                        project_region = 'ap-northeast-1'
                    else:
                        project_region = api_region  # Use as-is if format is different
                if project_db_host:
                    logger.debug(f"Found database host from API: {project_db_host}")
        except:
            pass
        
        # Check if full connection string is provided (bypasses all DNS/hostname issues)
        connection_string = credentials.get('connection_string') if credentials else None
        
        # If no connection string but password is provided, build Session Pooler URL automatically
        if not connection_string:
            db_password = credentials.get('db_password') if credentials else None
            if db_password and project_ref:
                # Build Session Pooler connection string automatically
                # Format: postgresql://postgres.{project_ref}:{password}@aws-1-{region}.pooler.supabase.com:5432/postgres
                from urllib.parse import quote_plus
                
                # URL encode password
                encoded_password = quote_plus(db_password)
                
                # Build Session Pooler connection string
                connection_string = f"postgresql://postgres.{project_ref}:{encoded_password}@aws-1-{project_region}.pooler.supabase.com:5432/postgres"
                logger.info(f"Built Session Pooler connection string automatically")
                logger.info(f"  Project: {project_name_display} (ref: {project_ref})")
                logger.info(f"  Region: {project_region}")
                logger.debug(f"  Connection string: postgresql://postgres.{project_ref}:***@aws-1-{project_region}.pooler.supabase.com:5432/postgres")
        
        if connection_string:
            # Use connection string directly - but handle DNS resolution if needed
            logger.info("Using provided connection string")
            logger.debug(f"Connection string: postgresql://postgres:***@{connection_string.split('@')[1] if '@' in connection_string else 'hidden'}")
            try:
                pg_conn = psycopg2.connect(connection_string, connect_timeout=10)
                logger.info("Successfully connected using connection string")
                # Continue to table creation below
            except psycopg2.OperationalError as conn_error:
                error_msg = str(conn_error)
                if "could not translate host name" in error_msg or "Name or service not known" in error_msg:
                    # DNS resolution failed - try to resolve hostname to IP
                    logger.info("DNS resolution failed, attempting to resolve hostname to IP address...")
                    import re
                    import subprocess
                    import socket
                    
                    # Parse connection string to extract hostname
                    # Format: postgresql://user:pass@host:port/db
                    match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', connection_string)
                    if match:
                        conn_user, conn_pass, conn_host, conn_port, conn_db = match.groups()
                        
                        # Try to resolve hostname using nslookup
                        try:
                            result = subprocess.run(
                                ['nslookup', conn_host],
                                capture_output=True,
                                text=True,
                                timeout=5
                            )
                            if result.returncode == 0:
                                output = result.stdout
                                lines = output.split('\n')
                                found_name = False
                                resolved_ip = None
                                
                                for i, line in enumerate(lines):
                                    line = line.strip()
                                    if line.startswith('Name:'):
                                        found_name = True
                                        continue
                                    if found_name and ('Address:' in line or 'Addresses:' in line):
                                        ip_part = line.split('Addresses:')[1].strip() if 'Addresses:' in line else (line.split('Address:')[1].strip() if 'Address:' in line else '')
                                        # Check for IPv6 (contains colons)
                                        if ':' in ip_part:
                                            ipv6_address = ip_part.split()[0] if ' ' in ip_part else ip_part
                                            # Validate IPv6 format
                                            if ipv6_address.count(':') >= 2:
                                                resolved_ip = f"[{ipv6_address}]"
                                                break
                                        # Check for IPv4
                                        elif '.' in ip_part:
                                            ipv4_address = ip_part.split()[0] if ' ' in ip_part else ip_part
                                            if ipv4_address.count('.') == 3:
                                                try:
                                                    socket.inet_aton(ipv4_address)
                                                    if ipv4_address not in ['8.8.8.8', '8.8.4.4', '1.1.1.1', '1.0.0.1']:
                                                        resolved_ip = ipv4_address
                                                        break
                                                except socket.error:
                                                    continue
                                
                                if resolved_ip:
                                    # Rebuild connection string with IP address
                                    new_conn_str = f"postgresql://{conn_user}:{conn_pass}@{resolved_ip}:{conn_port}/{conn_db}"
                                    logger.info(f"Resolved {conn_host} to {resolved_ip}, retrying connection...")
                                    try:
                                        pg_conn = psycopg2.connect(new_conn_str, connect_timeout=10)
                                        logger.info("Successfully connected using resolved IP address")
                                        # Continue to table creation below - skip all other connection methods
                                        connection_string_success = True
                                    except Exception as ip_error:
                                        error_msg_ip = str(ip_error)
                                        if "Network is unreachable" in error_msg_ip or "10051" in error_msg_ip:
                                            logger.error(f"IPv6 network unreachable. Your network doesn't support IPv6.")
                                            logger.error(f"Please use Session Pooler connection string instead:")
                                            logger.error("  -> Supabase Dashboard -> Connection String -> Session Pooler")
                                            logger.error("  -> Format: postgresql://postgres.[PROJECT_REF]:[PASSWORD]@aws-1-[REGION].pooler.supabase.com:5432/postgres")
                                        logger.error(f"Connection with resolved IP also failed: {ip_error}")
                                        raise
                                else:
                                    logger.error(f"Could not resolve hostname {conn_host} to an IP address")
                                    raise conn_error
                            else:
                                logger.error(f"nslookup failed for hostname {conn_host}")
                                raise conn_error
                        except Exception as resolve_error:
                            logger.error(f"Failed to resolve hostname: {resolve_error}")
                            raise conn_error
                    else:
                        logger.error("Could not parse connection string format")
                        raise conn_error
                else:
                    # Other error (not DNS-related)
                    logger.error(f"Connection string failed: {conn_error}")
                    raise
            except Exception as e:
                logger.error(f"Connection string failed: {e}")
                raise
        
        # If we reach here without connection_string, it means password wasn't provided
        if not connection_string:
            db_password = credentials.get('db_password') if credentials else None
            if not db_password:
                error_msg = (
                    "Database password is required for applying schema to Supabase.\n"
                    "Options:\n"
                    "  1. Set environment variable: SUPABASE_DB_PASSWORD=your_password\n"
                    "  2. Create .env file with: SUPABASE_DB_PASSWORD=your_password\n"
                    "  3. Password will be prompted interactively if not set\n"
                    "  4. OR provide full connection_string in credentials\n\n"
                    "Find your password in Supabase Dashboard:\n"
                    "  -> Project Settings -> Database -> Database password (or Connection string)"
                )
                raise ValueError(error_msg)
            
            # Connection successful - continue to table creation
        
        # Table creation
        cursor = pg_conn.cursor()
        
        try:
            for table_info in schema.tables:
                try:
                    # Check if table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table_info.name,))
                    table_exists = cursor.fetchone()[0]
                    
                    if table_exists:
                        logger.info(f"  Table '{table_info.name}' already exists - dropping and recreating...")
                        cursor.execute(f'DROP TABLE IF EXISTS "{table_info.name}" CASCADE')
                    
                    # Extract and create sequences before table creation
                    sequences_to_create = []
                    for col in table_info.columns:
                        if col.default_value:
                            default_str = str(col.default_value)
                            # Check if default uses nextval() - extract sequence name
                            if 'nextval' in default_str.lower():
                                import re
                                # Extract sequence name from nextval('sequence_name'::regclass)
                                seq_match = re.search(r"nextval\s*\(\s*['\"]([^'\"]+)['\"]", default_str, re.IGNORECASE)
                                if seq_match:
                                    seq_name = seq_match.group(1)
                                    sequences_to_create.append(seq_name)
                    
                    # Create sequences if needed
                    for seq_name in sequences_to_create:
                        try:
                            cursor.execute(f'DROP SEQUENCE IF EXISTS "{seq_name}" CASCADE')
                            cursor.execute(f'CREATE SEQUENCE "{seq_name}"')
                            logger.debug(f"  Created sequence '{seq_name}'")
                        except Exception as seq_error:
                            logger.warning(f"  Could not create sequence '{seq_name}': {seq_error}")
                    
                    # Build CREATE TABLE statement
                    column_defs = []
                    for col in table_info.columns:
                        # Build column definition
                        col_def = f'"{col.name}" {col.data_type}'
                        
                        # Add length/precision if not already in data_type
                        if '(' not in col.data_type.upper():
                            if col.character_maximum_length and col.data_type.upper() in ['VARCHAR', 'CHAR', 'CHARACTER VARYING']:
                                col_def += f'({col.character_maximum_length})'
                            elif col.numeric_precision and col.numeric_scale and col.data_type.upper() in ['DECIMAL', 'NUMERIC']:
                                col_def += f'({col.numeric_precision},{col.numeric_scale})'
                            elif col.numeric_precision and col.data_type.upper() in ['DECIMAL', 'NUMERIC']:
                                col_def += f'({col.numeric_precision},0)'
                        
                        if not col.is_nullable:
                            col_def += ' NOT NULL'
                        
                        if col.default_value:
                            # Handle different default value types
                            default_str = str(col.default_value)
                            
                            # Normalize MySQL/MariaDB timestamp functions to PostgreSQL format
                            default_lower = default_str.lower().strip()
                            if default_lower in ['current_timestamp()', 'current_timestamp', 'now()', 'now']:
                                # PostgreSQL uses CURRENT_TIMESTAMP without parentheses
                                default_str = 'CURRENT_TIMESTAMP'
                            elif default_lower == 'current_date()':
                                default_str = 'CURRENT_DATE'
                            
                            # Check if it's a function call (nextval, gen_random_uuid, etc.) - don't quote
                            is_function = any(func in default_str.lower() for func in ['nextval', 'gen_random_uuid', 'currval', 'now()', 'current_timestamp', 'current_date'])
                            
                            if is_function or default_str.upper() in ['CURRENT_TIMESTAMP', 'NOW()', 'CURRENT_DATE']:
                                # Function call - use as-is without quotes
                                col_def += f' DEFAULT {default_str}'
                            elif col.data_type.upper() in ['UUID'] and 'gen_random_uuid()' in default_str.lower():
                                col_def += ' DEFAULT gen_random_uuid()'
                            elif isinstance(col.default_value, str) and not default_str.replace('.', '').replace('-', '').isdigit():
                                # String default - escape single quotes
                                escaped = default_str.replace("'", "''")
                                col_def += f" DEFAULT '{escaped}'"
                            else:
                                # Numeric or other literal - use as-is
                                col_def += f' DEFAULT {default_str}'
                        
                        column_defs.append(col_def)
                    
                    # Add primary key constraint
                    pk_cols = [col.name for col in table_info.columns if col.is_primary_key]
                    if pk_cols:
                        pk_cols_quoted = ', '.join([f'"{col}"' for col in pk_cols])
                        column_defs.append(f'PRIMARY KEY ({pk_cols_quoted})')
                    
                    create_table_sql = f"""
                        CREATE TABLE "{table_info.name}" (
                            {', '.join(column_defs)}
                        )
                    """
                    
                    cursor.execute(create_table_sql)
                    # CRITICAL: Commit table creation immediately to ensure it's saved
                    pg_conn.commit()
                    logger.info(f"  Created table '{table_info.name}' with {len(table_info.columns)} columns")
                    
                    # Create indexes in separate transactions (using savepoints for safety)
                    for idx_num, index in enumerate(table_info.indexes):
                        savepoint_name = f"idx_{table_info.name}_{idx_num}"
                        try:
                            # Create a savepoint before each index creation
                            cursor.execute(f"SAVEPOINT {savepoint_name}")
                            
                            # Convert index definition if needed
                            index_sql = index.get('definition', '')
                            if 'CREATE INDEX' in index_sql.upper():
                                # Convert MySQL backticks (`) to PostgreSQL double quotes (")
                                # MySQL uses backticks, PostgreSQL uses double quotes
                                index_sql = index_sql.replace('`', '"')
                                
                                # Extract index name to check if it already exists
                                # Pattern: CREATE INDEX "index_name" ON "table" ...
                                import re
                                index_name_match = re.search(r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:"?)(\w+)(?:"?)', index_sql, re.IGNORECASE)
                                if index_name_match:
                                    index_name = index_name_match.group(1)
                                    # Check if index already exists
                                    cursor.execute("""
                                        SELECT EXISTS (
                                            SELECT FROM pg_indexes 
                                            WHERE schemaname = 'public' 
                                            AND indexname = %s
                                        )
                                    """, (index_name,))
                                    index_exists = cursor.fetchone()[0]
                                    if index_exists:
                                        logger.debug(f"  Index '{index_name}' already exists, skipping...")
                                        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                        continue
                                
                                cursor.execute(index_sql)
                                pg_conn.commit()
                                logger.debug(f"  Created index '{index_name_match.group(1) if index_name_match else 'unnamed'}' for '{table_info.name}'")
                            else:
                                # Not a CREATE INDEX statement, skip
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        except Exception as idx_error:
                            # Rollback to savepoint on error, but keep the table
                            try:
                                cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                                pg_conn.commit()
                            except:
                                pass
                            logger.warning(f"  Could not create index for '{table_info.name}': {idx_error}")
                    
                except Exception as table_error:
                    # Rollback this table's transaction, but continue with other tables
                    pg_conn.rollback()
                    logger.error(f"  Failed to create table '{table_info.name}': {table_error}")
                    import traceback
                    logger.debug(f"  Traceback: {traceback.format_exc()}")
                    # Continue with next table instead of failing completely
                    continue
            
            logger.info(f"Schema applied successfully to Supabase project '{project_ref}'")
            
        except Exception as e:
            pg_conn.rollback()
            logger.error(f"Error applying schema: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            cursor.close()
            if pg_conn:
                pg_conn.close()
