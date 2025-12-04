import json
import uuid
import hashlib
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from google.cloud import bigquery
from google.oauth2 import service_account
import asyncio
import config

@dataclass
class ClientSession:
    """Represents an authenticated client session"""
    client_id: str
    session_token: str
    created_at: datetime
    expires_at: datetime
    bigquery_client: Optional[bigquery.Client] = None


class AuthenticationManager:
    """Manages client authentication and session lifecycle"""
    
    def __init__(self):
        # In production, store these securely (e.g., environment variables, secret manager)
        self.registered_clients = {
            "demo_client_id_123": "demo_secret_xyz789",
            "analytics_client_456": "secret_abc123def"
        }
        self.active_sessions: Dict[str, ClientSession] = {}
        self.session_duration = timedelta(hours=1)
    
    def authenticate_client(self, client_id: str, client_secret: str) -> Optional[str]:
        """
        Authenticate client and create session token
        Returns session token if successful, None otherwise
        """
        # Verify client credentials
        if client_id not in self.registered_clients:
            return None
        
        if self.registered_clients[client_id] != client_secret:
            return None
        
        # Generate unique session token
        session_token = self._generate_session_token(client_id)
        
        # Create session
        now = datetime.now()
        session = ClientSession(
            client_id=client_id,
            session_token=session_token,
            created_at=now,
            expires_at=now + self.session_duration
        )
        
        self.active_sessions[session_token] = session
        return session_token
    
    def validate_session(self, session_token: str) -> bool:
        """Validate if session token is active and not expired"""
        if session_token not in self.active_sessions:
            return False
        
        session = self.active_sessions[session_token]
        
        # Check expiration
        if datetime.now() > session.expires_at:
            del self.active_sessions[session_token]
            return False
        
        return True
    
    def get_session(self, session_token: str) -> Optional[ClientSession]:
        """Retrieve session by token"""
        return self.active_sessions.get(session_token)
    
    def revoke_session(self, session_token: str) -> bool:
        """Revoke a session token"""
        if session_token in self.active_sessions:
            del self.active_sessions[session_token]
            return True
        return False
    
    def _generate_session_token(self, client_id: str) -> str:
        """Generate unique session token"""
        unique_string = f"{client_id}:{uuid.uuid4()}:{time.time()}"
        return hashlib.sha256(unique_string.encode()).hexdigest()


class BigQueryMCPServer:
    """MCP Server for BigQuery operations"""
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        self.project_id = project_id
        self.credentials_path = credentials_path
        self.auth_manager = AuthenticationManager()
        
        # Tool definitions following MCP protocol
        self.tools = {
            "bq.run_query": {
                "description": "Execute SQL query on BigQuery",
                "parameters": {
                    "sql": {"type": "string", "description": "SQL query to execute"},
                    "max_results": {"type": "integer", "description": "Maximum rows to return", "default": 1000},
                    "use_legacy_sql": {"type": "boolean", "description": "Use legacy SQL", "default": False}
                }
            },
            "bq.list_tables": {
                "description": "List tables in dataset with metadata",
                "parameters": {
                    "dataset_id": {"type": "string", "description": "Dataset ID to list tables from"},
                    "max_results": {"type": "integer", "description": "Maximum tables to return", "default": 100}
                }
            },
            "bq.get_table_profile": {
                "description": "Get detailed table profile and statistics",
                "parameters": {
                    "dataset_id": {"type": "string", "description": "Dataset ID"},
                    "table_id": {"type": "string", "description": "Table ID"}
                }
            }
        }
    
    def _get_bq_client(self, session: ClientSession) -> bigquery.Client:
        """Get or create BigQuery client for session"""
        if session.bigquery_client is None:
            if self.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path
                )
                session.bigquery_client = bigquery.Client(
                    project=self.project_id,
                    credentials=credentials
                )
            else:
                # Use application default credentials
                session.bigquery_client = bigquery.Client(project=self.project_id)
        
        return session.bigquery_client
    
    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main request handler following MCP protocol
        Request format: {
            "method": "tools/call",
            "params": {
                "name": "tool_name",
                "arguments": {...},
                "session_token": "..."
            }
        }
        """
        try:
            method = request.get("method")
            params = request.get("params", {})
            
            # Handle authentication request
            if method == "auth/authenticate":
                return await self._handle_authentication(params)
            
            # All other methods require authentication
            session_token = params.get("session_token")
            if not session_token or not self.auth_manager.validate_session(session_token):
                return {
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Invalid or expired session token"
                    }
                }
            
            # Get session
            session = self.auth_manager.get_session(session_token)
            
            # Handle tool calls
            if method == "tools/list":
                return {"tools": list(self.tools.values())}
            
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                return await self._handle_tool_call(session, tool_name, arguments)
            
            else:
                return {
                    "error": {
                        "code": "METHOD_NOT_FOUND",
                        "message": f"Unknown method: {method}"
                    }
                }
        
        except Exception as e:
            return {
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": str(e)
                }
            }
    
    async def _handle_authentication(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle client authentication"""
        client_id = params.get("client_id")
        client_secret = params.get("client_secret")
        
        if not client_id or not client_secret:
            return {
                "error": {
                    "code": "INVALID_REQUEST",
                    "message": "client_id and client_secret required"
                }
            }
        
        session_token = self.auth_manager.authenticate_client(client_id, client_secret)
        
        if session_token:
            session = self.auth_manager.get_session(session_token)
            return {
                "result": {
                    "session_token": session_token,
                    "expires_at": session.expires_at.isoformat(),
                    "client_id": client_id
                }
            }
        else:
            return {
                "error": {
                    "code": "AUTHENTICATION_FAILED",
                    "message": "Invalid client credentials"
                }
            }
    
    async def _handle_tool_call(
        self, 
        session: ClientSession, 
        tool_name: str, 
        arguments: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route tool calls to appropriate handlers"""
        
        if tool_name == "bq.run_query":
            return await self._run_query(session, arguments)
        elif tool_name == "bq.list_tables":
            return await self._list_tables(session, arguments)
        elif tool_name == "bq.get_table_profile":
            return await self._get_table_profile(session, arguments)
        else:
            return {
                "error": {
                    "code": "TOOL_NOT_FOUND",
                    "message": f"Unknown tool: {tool_name}"
                }
            }
    
    async def _run_query(self, session: ClientSession, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute BigQuery SQL query"""
        sql = args.get("sql")
        max_results = args.get("max_results", 1000)
        use_legacy_sql = args.get("use_legacy_sql", False)
        
        if not sql:
            return {"error": {"code": "INVALID_ARGS", "message": "sql parameter required"}}
        
        try:
            client = self._get_bq_client(session)
            
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=use_legacy_sql
            )
            
            query_job = client.query(sql, job_config=job_config)
            results = query_job.result(max_results=max_results)
            
            # Convert to list of dicts
            rows = [dict(row) for row in results]
            
            # Get schema info
            schema = [{"name": field.name, "type": field.field_type} 
                     for field in results.schema]
            
            return {
                "result": {
                    "rows": rows,
                    "schema": schema,
                    "total_rows": results.total_rows,
                    "client_id": session.client_id
                }
            }
        
        except Exception as e:
            return {"error": {"code": "QUERY_FAILED", "message": str(e)}}
    
    async def _list_tables(self, session: ClientSession, args: Dict[str, Any]) -> Dict[str, Any]:
        """List tables in a dataset"""
        dataset_id = args.get("dataset_id")
        max_results = args.get("max_results", 100)
        
        if not dataset_id:
            return {"error": {"code": "INVALID_ARGS", "message": "dataset_id required"}}
        
        try:
            client = self._get_bq_client(session)
            
            dataset_ref = f"{self.project_id}.{dataset_id}"
            tables = client.list_tables(dataset_ref, max_results=max_results)
            
            table_list = []
            for table in tables:
                table_list.append({
                    "table_id": table.table_id,
                    "full_table_id": f"{table.project}.{table.dataset_id}.{table.table_id}",
                    "table_type": table.table_type,
                    "created": table.created.isoformat() if table.created else None
                })
            
            return {
                "result": {
                    "tables": table_list,
                    "dataset_id": dataset_id,
                    "client_id": session.client_id
                }
            }
        
        except Exception as e:
            return {"error": {"code": "LIST_FAILED", "message": str(e)}}
    
    async def _get_table_profile(self, session: ClientSession, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get detailed table profile"""
        dataset_id = args.get("dataset_id")
        table_id = args.get("table_id")
        
        if not dataset_id or not table_id:
            return {"error": {"code": "INVALID_ARGS", "message": "dataset_id and table_id required"}}
        
        try:
            client = self._get_bq_client(session)
            
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            table = client.get_table(table_ref)
            
            # Get sample statistics
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
            
            return {"result": profile}
        
        except Exception as e:
            return {"error": {"code": "PROFILE_FAILED", "message": str(e)}}


# Example usage
async def main():
    """Demonstration of MCP server usage"""
    
    # Initialize server
    server = BigQueryMCPServer(
    project_id=config.PROJECT_ID,
    credentials_path=config.CREDENTIALS_PATH
)
    
    print("=== BigQuery MCP Server Demo ===\n")
    
    # Client 1: Authenticate
    print("Client 1: Authenticating...")
    auth_request = {
        "method": "auth/authenticate",
        "params": {
            "client_id": "demo_client_id_123",
            "client_secret": "demo_secret_xyz789"
        }
    }
    auth_response = await server.handle_request(auth_request)
    print(f"Response: {json.dumps(auth_response, indent=2)}\n")
    
    if "result" in auth_response:
        client1_token = auth_response["result"]["session_token"]
        
        # Client 1: List tables
        print("Client 1: Listing tables...")
        list_request = {
            "method": "tools/call",
            "params": {
                "name": "bq.list_tables",
                "arguments": {
                    "dataset_id": "test_db" # replace with actual dataset 
                },
                "session_token": client1_token
            }
        }
        list_response = await server.handle_request(list_request)
        print(f"Response: {json.dumps(list_response, indent=2)}\n")
    
    # Client 2: Authenticate with different credentials
    print("Client 2: Authenticating...")
    auth_request2 = {
        "method": "auth/authenticate",
        "params": {
            "client_id": "analytics_client_456",
            "client_secret": "secret_abc123def"
        }
    }
    auth_response2 = await server.handle_request(auth_request2)
    print(f"Response: {json.dumps(auth_response2, indent=2)}\n")
    
    # Demonstrate unauthorized access
    print("Attempting unauthorized access...")
    unauthorized_request = {
        "method": "tools/call",
        "params": {
            "name": "bq.list_tables",
            "arguments": {"dataset_id": "test_db"}, # replace with actual dataset
            "session_token": "invalid_token_123"
        }
    }
    unauthorized_response = await server.handle_request(unauthorized_request)
    print(f"Response: {json.dumps(unauthorized_response, indent=2)}\n")
    
    print("=== Demo Complete ===")


if __name__ == "__main__":
    asyncio.run(main())