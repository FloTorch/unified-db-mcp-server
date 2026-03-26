"""Script to verify tables exist in Supabase"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    import psycopg2
    from urllib.parse import quote_plus
    from helpers.supabase_api import get_all_supabase_projects
    
    # Get credentials
    api_key = os.getenv('SUPABASE_API_KEY')
    project_name = os.getenv('SUPABASE_PROJECT')
    db_password = os.getenv('SUPABASE_DB_PASSWORD')
    
    if not api_key or not project_name or not db_password:
        print("[ERROR] Missing credentials. Please set SUPABASE_API_KEY, SUPABASE_PROJECT, and SUPABASE_DB_PASSWORD in .env")
        sys.exit(1)
    
    # Get project ref
    projects = get_all_supabase_projects(api_key)
    project = next((p for p in projects if p.get('name') == project_name), None)
    
    if not project:
        print(f"[ERROR] Project '{project_name}' not found")
        sys.exit(1)
    
    project_ref = project.get('id') or project.get('ref')
    project_region = project.get('region', 'ap-southeast-1')
    
    if 'ap-southeast' in project_region.lower() or 'singapore' in project_region.lower():
        project_region = 'ap-southeast-1'
    elif 'ap-south' in project_region.lower():
        project_region = 'ap-south-1'
    elif 'us-east' in project_region.lower():
        project_region = 'us-east-1'
    elif 'us-west' in project_region.lower():
        project_region = 'us-west-1'
    elif 'eu-west' in project_region.lower():
        project_region = 'eu-west-1'
    
    print(f"Project: {project_name} (ref: {project_ref})")
    print(f"Region: {project_region}")
    
    # Build connection string
    encoded_password = quote_plus(db_password)
    connection_string = f"postgresql://postgres.{project_ref}:{encoded_password}@aws-1-{project_region}.pooler.supabase.com:5432/postgres"
    
    print(f"Connecting to Supabase...")
    conn = psycopg2.connect(connection_string, connect_timeout=10)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    
    print(f"\n[SUCCESS] Found {len(tables)} table(s) in Supabase:\n")
    for (table_name,) in tables:
        # Get row count
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            count = cursor.fetchone()[0]
            print(f"  - {table_name} ({count} rows)")
        except:
            print(f"  - {table_name} (unable to count rows)")
    
    cursor.close()
    conn.close()
    
    print(f"\n[SUCCESS] Verification complete!")
    
except ImportError as e:
    print(f"[ERROR] Missing dependency: {e}")
    print("Install with: pip install psycopg2-binary python-dotenv")
    sys.exit(1)
except Exception as e:
    print(f"[ERROR] Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
