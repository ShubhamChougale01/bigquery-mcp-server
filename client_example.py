import asyncio
import os
import json
from bq_mcp_server import BigQueryMCPServer
import config
from dotenv import load_dotenv
load_dotenv()

async def test_client():
    # Initialize server
    server = BigQueryMCPServer(
        project_id=config.PROJECT_ID,
        credentials_path=config.CREDENTIALS_PATH
    )
    
    # Step 1: Authenticate
    print("🔐 Authenticating client...")
    auth_response = await server.handle_request({
        "method": "auth/authenticate",
        "params": {
            "client_id": "demo_client_id_123",
            "client_secret": "demo_secret_xyz789"
        }
    })
    
    if "error" in auth_response:
        print(f"❌ Auth failed: {auth_response['error']}")
        return
    
    token = auth_response["result"]["session_token"]
    print(f"✅ Authenticated! Token: {token[:20]}...")
    print(f"   Client ID: {auth_response['result']['client_id']}")
    print(f"   Expires: {auth_response['result']['expires_at']}")
    
    # Step 2: List tables (using your actual dataset: test_db)
    print("\n📋 Listing tables in test_db...")
    list_response = await server.handle_request({
        "method": "tools/call",
        "params": {
            "name": "bq.list_tables",
            "arguments": {"dataset_id": "test_db"},  # Changed to test_db
            "session_token": token
        }
    })
    print(json.dumps(list_response, indent=2))
    
    # Step 3: Run a query
    print("\n🔍 Running count query...")
    query_response = await server.handle_request({
        "method": "tools/call",
        "params": {
            "name": "bq.run_query",
            "arguments": {
                "sql": f"SELECT COUNT(*) as total FROM `{config.PROJECT_ID}.test_db.sample_table`",
                "max_results": 10
            },
            "session_token": token
        }
    })
    print(json.dumps(query_response, indent=2))
    
    # Step 4: Run a query to see sample data
    print("\n📊 Getting sample data...")
    sample_query_response = await server.handle_request({
        "method": "tools/call",
        "params": {
            "name": "bq.run_query",
            "arguments": {
                "sql": f"SELECT * FROM `{config.PROJECT_ID}.test_db.sample_table` LIMIT 5",
                "max_results": 5
            },
            "session_token": token
        }
    })
    print(json.dumps(sample_query_response, indent=2, default=str))
    
    # Step 5: Get table profile
    print("\n📈 Getting table profile...")
    profile_response = await server.handle_request({
        "method": "tools/call",
        "params": {
            "name": "bq.get_table_profile",
            "arguments": {
                "dataset_id": "test_db",  # Changed to test_db
                "table_id": "sample_table"
            },
            "session_token": token
        }
    })
    print(json.dumps(profile_response, indent=2, default=str))
    
    # Step 6: Demonstrate client uniqueness - authenticate a second client
    print("\n" + "="*60)
    print("🔐 Authenticating second client (analytics_client_456)...")
    auth_response2 = await server.handle_request({
        "method": "auth/authenticate",
        "params": {
            "client_id": "analytics_client_456",
            "client_secret": "secret_abc123def"
        }
    })
    
    if "result" in auth_response2:
        token2 = auth_response2["result"]["session_token"]
        print(f"✅ Second client authenticated!")
        print(f"   Token: {token2[:20]}...")
        print(f"   Client ID: {auth_response2['result']['client_id']}")
        
        # Show tokens are different
        print(f"\n🔑 Token comparison:")
        print(f"   Client 1 token: {token[:30]}...")
        print(f"   Client 2 token: {token2[:30]}...")
        print(f"   Tokens are unique: {token != token2}")
        
        # Second client runs same query
        print(f"\n🔍 Second client running query...")
        query_response2 = await server.handle_request({
            "method": "tools/call",
            "params": {
                "name": "bq.run_query",
                "arguments": {
                    "sql": f"SELECT COUNT(*) as total FROM `{config.PROJECT_ID}.test_db.sample_table`",
                    "max_results": 10
                },
                "session_token": token2
            }
        })
        
        # Show client_id is tracked per request
        if "result" in query_response2:
            print(f"   ✅ Query successful for client: {query_response2['result']['client_id']}")
    
    # Step 7: Demonstrate unauthorized access
    print("\n" + "="*60)
    print("🚫 Testing unauthorized access...")
    unauthorized_response = await server.handle_request({
        "method": "tools/call",
        "params": {
            "name": "bq.list_tables",
            "arguments": {"dataset_id": "test_db"},
            "session_token": "invalid_fake_token_xyz"
        }
    })
    print(json.dumps(unauthorized_response, indent=2))
    
    print("\n" + "="*60)
    print("✅ All tests completed!")

if __name__ == "__main__":
    asyncio.run(test_client())