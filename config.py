import os
import json
from dotenv import load_dotenv
load_dotenv()

PROJECT_NAME = os.getenv("PROJECT_NAME", "bigquery-mcp-server")
PROJECT_ID = os.getenv("PROJECT_ID")
PROJECT_NUMBER = os.getenv("PROJECT_NUMBER", "")

CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "bigquery-credentials.json")

REGISTERED_CLIENTS = json.loads(os.getenv("CLIENTS_JSON", "{}"))
