"""Supabase API helper functions"""
import logging
import requests
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


def get_all_supabase_projects(api_key: str) -> List[Dict[str, Any]]:
    """Get all Supabase projects accessible with the given API key"""
    try:
        response = requests.get(
            "https://api.supabase.com/v1/projects",
            headers={
                "Authorization": f"Bearer {api_key}",
                "apikey": api_key,
                "Content-Type": "application/json"
            },
            timeout=30
        )
        response.raise_for_status()
        projects = response.json()
        logger.info(f"Found {len(projects)} Supabase projects")
        return projects
    except Exception as e:
        logger.error(f"Error fetching Supabase projects: {e}")
        return []


def get_supabase_project_api_keys(api_key: str, project_ref: str) -> Optional[Dict[str, str]]:
    """Get project API keys (anon and service_role) from Management API"""
    try:
        response = requests.get(
            f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
            headers={
                "Authorization": f"Bearer {api_key}",
                "apikey": api_key,
                "Content-Type": "application/json"
            },
            timeout=30
        )
        if response.status_code == 200:
            api_keys_data = response.json()
            keys = {}
            for key_info in api_keys_data:
                key_name = key_info.get('name', '').lower()
                key_value = key_info.get('api_key') or key_info.get('key')
                if 'anon' in key_name or 'public' in key_name:
                    keys['anon_key'] = key_value
                elif 'service' in key_name or 'service_role' in key_name:
                    keys['service_role_key'] = key_value
            if keys:
                logger.info(f"Found API keys for project {project_ref}")
                return keys
    except Exception as e:
        logger.debug(f"Error fetching API keys: {e}")
    return None
