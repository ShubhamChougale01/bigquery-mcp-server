import json
import uuid
import hashlib
import time
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict
from collections import defaultdict

from mcp.server.fastmcp import FastMCP
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.client_info import ClientInfo
from pydantic import BaseModel
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stderr)]
)

logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("bigquery")

class RateLimiter:
    def __init__(self, max_requests=20, window=60):
        self.requests = defaultdict(list)
        self.max_requests = max_requests
        self.window = window

    def allow(self, client_id: str) -> bool:
        now = time.time()
        window_start = now - self.window
        req_times = self.requests[client_id]

        # Remove timestamps older than the window
        self.requests[client_id] = [t for t in req_times if t > window_start]

        # Check if limit reached
        if len(self.requests[client_id]) >= self.max_requests:
            return False

        # Add this request time
        self.requests[client_id].append(now)
        return True
    
# Global state for sessions and rate limiting
class ServerState:
    def __init__(self):
        self.active_sessions: Dict[str, 'ClientSession'] = {}
        self.registered_clients = self._load_clients()
        self.rate_limiter = RateLimiter(max_requests=30, window=60)
        self.session_duration = timedelta(hours=1)
    
    def _load_clients(self):
        default_clients = {
            "demo_client_id_123": "demo_secret_xyz789",
            "analytics_client_456": "secret_abc123def"
        }
        registered = getattr(config, "REGISTERED_CLIENTS", None)
        if isinstance(registered, dict) and registered:
            return registered
        return default_clients

# Global state instance
state = ServerState()


class ClientSession:
    """Represents an authenticated client session"""
    def __init__(self, client_id: str, session_token: str):
        self.client_id = client_id
        self.session_token = session_token
        self.created_at = datetime.now()
        self.expires_at = self.created_at + state.session_duration
        self.bigquery_client: Optional[bigquery.Client] = None


def generate_session_token(client_id: str) -> str:
    """Generate unique session token"""
    unique_string = f"{client_id}:{uuid.uuid4()}:{time.time()}"
    return hashlib.sha256(unique_string.encode()).hexdigest()


def get_bq_client(session: ClientSession) -> bigquery.Client:
    """Get or create BigQuery client for session"""
    if session.bigquery_client is None:
        if config.CREDENTIALS_PATH:
            credentials = service_account.Credentials.from_service_account_file(
                config.CREDENTIALS_PATH
            )
            session.bigquery_client = bigquery.Client(
                project=config.PROJECT_ID,
                credentials=credentials,
                client_info=ClientInfo(user_agent="MCP-Server-1.0")
            )
        else:
            session.bigquery_client = bigquery.Client(project=config.PROJECT_ID)
    
    return session.bigquery_client


def validate_session(session_token: str) -> Optional[ClientSession]:
    """Validate session and return it if valid"""
    if not session_token or session_token not in state.active_sessions:
        return None
    
    session = state.active_sessions[session_token]
    
    # Check expiration
    if datetime.now() > session.expires_at:
        del state.active_sessions[session_token]
        return None
    
    # Check rate limit
    if not state.rate_limiter.allow(session.client_id):
        logger.warning(f"RATE LIMIT: client={session.client_id} exceeded request limit.")
        return None
    
    return session


# MCP Tools with correct naming (bq prefix)
@mcp.tool(name="authenticate")
async def authenticate(client_id: str, client_secret: str) -> str:
    """
    Authenticate a client and get a session token.
    
    Args:
        client_id: The client ID for authentication
        client_secret: The client secret for authentication
    
    Returns:
        JSON string containing session_token and expiration info
    """
    if client_id not in state.registered_clients:
        return json.dumps({"error": "Invalid client credentials"})
    
    if state.registered_clients[client_id] != client_secret:
        return json.dumps({"error": "Invalid client credentials"})
    
    # Generate session token
    session_token = generate_session_token(client_id)
    session = ClientSession(client_id, session_token)
    state.active_sessions[session_token] = session
    
    logger.info(f"Client authenticated: {client_id}")
    
    return json.dumps({
        "session_token": session_token,
        "expires_at": session.expires_at.isoformat(),
        "client_id": client_id
    })


@mcp.tool(name="bq.run_query")
async def run_query(session_token: str, sql: str, max_results: int = 1000, use_legacy_sql: bool = False) -> str:
    """
    Execute a SQL query on BigQuery.
    
    Args:
        session_token: Valid session token from authentication
        sql: SQL query to execute
        max_results: Maximum number of rows to return (default: 1000)
        use_legacy_sql: Whether to use legacy SQL syntax (default: False)
    
    Returns:
        JSON string containing query results with rows and schema
    """
    session = validate_session(session_token)
    if not session:
        return json.dumps({"error": "Invalid or expired session token"})
    
    try:
        client = get_bq_client(session)
        
        job_config = bigquery.QueryJobConfig(use_legacy_sql=use_legacy_sql)
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result(max_results=max_results)
        
        # Convert to list of dicts
        rows = [dict(row) for row in results]
        
        # Get schema info
        schema = [{"name": field.name, "type": field.field_type} 
                 for field in results.schema]
        
        logger.info(f"Query executed successfully by {session.client_id}, returned {len(rows)} rows")
        
        return json.dumps({
            "rows": rows,
            "schema": schema,
            "total_rows": results.total_rows,
            "client_id": session.client_id
        }, default=str)
        
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
        return json.dumps({"error": f"Query failed: {str(e)}"})


@mcp.tool(name="bq.list_tables")
async def list_tables(session_token: str, dataset_id: str, max_results: int = 100) -> str:
    """
    List all tables in a BigQuery dataset.
    
    Args:
        session_token: Valid session token from authentication
        dataset_id: The dataset ID to list tables from
        max_results: Maximum number of tables to return (default: 100)
    
    Returns:
        JSON string containing list of tables with metadata
    """
    session = validate_session(session_token)
    if not session:
        return json.dumps({"error": "Invalid or expired session token"})
    
    try:
        client = get_bq_client(session)
        
        dataset_ref = f"{config.PROJECT_ID}.{dataset_id}"
        tables = client.list_tables(dataset_ref, max_results=max_results)
        
        table_list = []
        for table in tables:
            table_list.append({
                "table_id": table.table_id,
                "full_table_id": f"{table.project}.{table.dataset_id}.{table.table_id}",
                "table_type": table.table_type,
                "created": table.created.isoformat() if table.created else None
            })
        
        logger.info(f"Listed {len(table_list)} tables from {dataset_id}")
        
        return json.dumps({
            "tables": table_list,
            "dataset_id": dataset_id,
            "client_id": session.client_id
        })
        
    except Exception as e:
        logger.error(f"List tables failed: {str(e)}")
        return json.dumps({"error": f"List tables failed: {str(e)}"})


@mcp.tool(name="bq.get_table_profile")
async def get_table_profile(session_token: str, dataset_id: str, table_id: str) -> str:
    """
    Get detailed profile and statistics for a BigQuery table.
    
    Args:
        session_token: Valid session token from authentication
        dataset_id: The dataset ID containing the table
        table_id: The table ID to profile
    
    Returns:
        JSON string containing detailed table information including schema, row count, and sample data
    """
    session = validate_session(session_token)
    if not session:
        return json.dumps({"error": "Invalid or expired session token"})
    
    try:
        client = get_bq_client(session)
        
        table_ref = f"{config.PROJECT_ID}.{dataset_id}.{table_id}"
        table = client.get_table(table_ref)
        
        # Get sample data
        sample_query = f"SELECT * FROM `{table_ref}` LIMIT 10"
        sample_job = client.query(sample_query)
        sample_results = sample_job.result()
        sample_rows = [dict(row) for row in sample_results]
        
        profile = {
            "table_id": table.table_id,
            "full_table_id": table_ref,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created.isoformat() if table.created else None,
            "modified": table.modified.isoformat() if table.modified else None,
            "partitioning": {
                "type": table.time_partitioning.type_ if table.time_partitioning else None,
                "field": table.time_partitioning.field if table.time_partitioning else None
            } if table.time_partitioning else None,
            "clustering_fields": table.clustering_fields if table.clustering_fields else [],
            "schema": [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description
                }
                for field in table.schema
            ],
            "sample_data": sample_rows,
            "client_id": session.client_id
        }
        
        logger.info(f"Table profile generated for {table_ref}")
        
        return json.dumps(profile, default=str)
        
    except Exception as e:
        logger.error(f"Get table profile failed: {str(e)}")
        return json.dumps({"error": f"Get table profile failed: {str(e)}"})


def main():
    """Initialize and run the MCP server"""
    logger.info("Starting BigQuery MCP Server...")
    logger.info(f"Registered clients: {len(state.registered_clients)}")
    
    # Run the server
    mcp.run(transport='stdio')


if __name__ == "__main__":
    main()