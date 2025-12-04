# **README.md — BigQuery MCP Server**

## Overview

The **BigQuery MCP Server** is a custom Model Context Protocol (MCP) server that allows AI agents and clients to safely interact with **Google BigQuery**.

This server implements:

### Part A — BigQuery MCP Tools

1. **`bq.run_query`**
   Execute SQL queries against BigQuery.

2. **`bq.list_tables`**
   List all tables inside a dataset with metadata.

3. **`bq.get_table_profile`**
   Returns table statistics including:

   * row count
   * bytes
   * partition details
   * clustering fields
   * schema
   * sample rows
   * last modified timestamp

---

### Part B — OAuth-style Client Authentication

The server supports an OAuth-like workflow:

* Clients send `client_id` + `client_secret`
* Server validates credentials
* A unique **session token** is generated
* All tool calls require a valid session token
* Rate limits are enforced **per-client**

---

### Additional Production Enhancements

| Feature                   | Status |
| ------------------------- | ------ |
| Logging                   | ✔ Yes  |
| Rate Limiting             | ✔ Yes  |
| Secure credential loading | ✔ Yes  |
| Proper session lifecycle  | ✔ Yes  |

---

# Project Structure

```
bigquery-mcp/
│
├── bq_mcp_server.py     # Main MCP server with auth, tools, rate limiting
├── config.py            # Project configuration & credentials path
├── bigquery-credentials.json  # Service account key (DO NOT COMMIT)(You have to export this (GCP))
├── README.md
└── venv/ (optional)
```

---

# Requirements

* Python 3.10+
* Google Cloud project with BigQuery enabled
* Service Account with roles:
  * BigQuery Data Editor
  * BigQuery Job User
* Service Account key file (`bigquery-credentials.json`)

Install dependencies:
```bash
pip install google-cloud-bigquery google-auth
```

---

# Setup Instructions

## 1️⃣ Create Service Account & Download Key

In Google Cloud Console:
```
IAM & Admin → Service Accounts → Create Service Account
```

Give roles:
* BigQuery Data Editor
* BigQuery Job User

Generate JSON key:
```
Keys → Add Key → JSON
```

Move it into your project directory:
```bash
mv ~/Downloads/bigquery-credentials.json ./bigquery-credentials.json
```

---

## 2️⃣ Configure `config.py`

```python
import os

PROJECT_NAME = "******"
PROJECT_ID = "******"
PROJECT_NUMBER = "2837********"

CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "bigquery-credentials.json")

REGISTERED_CLIENTS = {
    "demo_client_id_123": "demo_secret_xyz789",
    "analytics_client_456": "secret_abc123def"
}
```

---

## 3️⃣ Create Sample Dataset & Table

### Create dataset:

```
test_db
```

### Create a sample table:

```sql
CREATE OR REPLACE TABLE 
`bigquery-mcp-******.test_db.sample_table` AS
SELECT
  'User' || CAST(id AS STRING) AS user_id,
  RAND() * 1000 AS revenue,
  DATE_SUB(CURRENT_DATE(), INTERVAL CAST(RAND() * 365 AS INT64) DAY) AS date
FROM UNNEST(GENERATE_ARRAY(1, 100)) AS id;
```

---

# Running the MCP Server

```bash
python bq_mcp_server.py
```

You should see:
```
Client 1: Authenticating...
Client 1: Listing tables...
```

---

# MCP Tools

## `bq.run_query`

Example request:

```json
{
  "method": "tools/call",
  "params": {
    "name": "bq.run_query",
    "arguments": {
      "sql": "SELECT COUNT(*) FROM `project.dataset.table`"
    },
    "session_token": "<token>"
  }
}
```

---

## `bq.list_tables`

```json
{
  "method": "tools/call",
  "params": {
    "name": "bq.list_tables",
    "arguments": { "dataset_id": "test_db" },
    "session_token": "<token>"
  }
}
```

---

## `bq.get_table_profile`

```json
{
  "method": "tools/call",
  "params": {
    "name": "bq.get_table_profile",
    "arguments": {
      "dataset_id": "test_db",
      "table_id": "sample_table"
    },
    "session_token": "<token>"
  }
}
```

---

# Authentication Workflow

### Step 1 — Authenticate

```json
{
  "method": "auth/authenticate",
  "params": {
    "client_id": "demo_client_id_123",
    "client_secret": "demo_secret_xyz789"
  }
}
```

Response:

```json
{
  "result": {
    "session_token": "abcdef123456...",
    "expires_at": "2025-01-01T12:00:00Z",
    "client_id": "demo_client_id_123"
  }
}
```

### Step 2 — Use tools with session token

All further requests must include:
```
"session_token": "<your_token>"
```

---

# Security Features

### ✔ Rate Limiting (per client)
Prevents excessive BigQuery usage.

### ✔ Logging
Tracks all requests, failures, warnings.

### ✔ Session Expiration
Sessions expire automatically after 1 hour.

---

# Architecture Diagram
Below is the diagram.

```
                ┌────────────────────────────┐
                │        MCP Client           │
                │  (AI Agent, CLI, App)       │
                └──────────────┬─────────────┘
                               │ 1. client_id + secret
                               ▼
                ┌────────────────────────────┐
                │ Authentication Manager      │
                │ - Validates credentials     │
                │ - Issues session tokens     │
                └──────────────┬─────────────┘
                               │ 2. session_token
                               ▼
               ┌──────────────────────────────┐
               │      BigQuery MCP Server      │
               │--------------------------------│
               │ Tools:                         │
               │  • bq.run_query                │
               │  • bq.list_tables              │
               │  • bq.get_table_profile        │
               │--------------------------------│
               │ Security Layers:               │
               │  • Rate Limiter (per client)   │
               │  • Logging                     │
               └──────────────┬─────────────────┘
                               │ 3. BigQuery API calls
                               ▼
               ┌───────────────────────────────┐
               │      Google BigQuery           │
               │  Dataset, tables, metadata     │
               └───────────────────────────────┘
```
---