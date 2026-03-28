"""
Configuration for Unified DB tools server.
"""

import os

APP_NAME = "Unified DB MCP"

HOST = os.getenv("HOST", "0.0.0.0")

PORT = int(os.getenv("PORT", "7860"))

MCP_PATH = "/unified-db/mcp"

SUPPORTED_DATABASES = [
    "supabase",
    "mysql",
    "mariadb",
    "postgresql",
    "mongodb",
    "sqlserver",
    "sqlite",
    "cassandra",
]