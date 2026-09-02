#!/usr/bin/env python
# coding: utf-8

# ## nb_run_ingestion
# 
# null

# In[13]:


# PARAMETERS CELL ********************
 
key_vault_uri = "https://allens-ae-fabric-dev-kv.vault.azure.net/"
tenant_id     = "700538d4-594a-49e5-9b7c-1e7d5173c3ba"
sources       = "copilot,legora"          # comma separated filter, blank runs all


# In[14]:


import json, notebookutils
from datetime import datetime, timedelta, timezone
 
NOTEBOOK = "./nb_api_to_bronze"
TIMEOUT = 3600
today = datetime.now(timezone.utc).date()
COCOUNSEL = {
    "destination_schema" : "cocounsel",
    "auth_type": "oauth2_client_credentials",
    "token_url": "https://auth.thomsonreuters.com/oauth/token",
    "key_vault_uri": key_vault_uri,
    "client_id_secret": "CoCounsel-Client-id",
    "client_secret_secret": "CoCounsel-Client-Secret",
    "audience_secret": "CoCounsel-Audience",
    "base_url": "https://admin-au.cocounsel.thomsonreuters.com/api/v1/admin",
    "response_format": "json",
    "records_path": "individuals",
    "date_param_from": "startDate",
    "date_param_to": "endDate",
    "request_delay": 8,          # 10 requests per minute per IP
}
 
COPILOT = {
    "destination_schema" : "copilot",
    "auth_type": "oauth2_client_credentials",
    "token_url": f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
    "key_vault_uri": key_vault_uri,
    "client_id_secret": "copilot-client-id",
    "client_secret_secret": "copilot-client-secret",
    "scope_secret": "copilot-scope",
    "base_url": "https://graph.microsoft.com/v1.0",
    "response_format": "csv",
    "ingestion_type": "full",
}
 
LEGORA = {
    "destination_schema" : "legora",
    "auth_type": "api_key",
    "key_vault_uri": key_vault_uri,
    "token_secret": "legora-api-key",
    "base_url": "https://api.ap.legora.com/api/v1",
    "response_format": "json",
    "pagination": "cursor",
    "cursor_field": "nextCursor",
    "page_size_param": "limit",
    "page_size": 1000,
}
 
DATASETS = [ # 1st aug 2025 
    {**COCOUNSEL, "source": "cocounsel", "dataset": "user_usage",
     "endpoint": "/analytics", "destination_table": "user_usage",
     "ingestion_type": "incremental", "start_date": str(today - timedelta(days=30))},
 
    {**COCOUNSEL, "source": "cocounsel", "dataset": "skill_usage",
     "endpoint": "/analytics", "query_params": json.dumps({"include": "skills"}),
    "destination_table": "skill_usage",
     "ingestion_type": "incremental", "start_date": str(today - timedelta(days=30))},
 
    {**COPILOT, "source": "copilot", "dataset": "user_detail",
     "endpoint": "/copilot/reports/getMicrosoft365CopilotUsageUserDetail(period='D30')",
     "destination_table": "user_detail"},
 
    {**COPILOT, "source": "copilot", "dataset": "user_count_trend",
     "endpoint": "/copilot/reports/getMicrosoft365CopilotUserCountTrend(period='D7')",
     "destination_table": "user_count_trend"},
 
    {**COPILOT, "source": "copilot", "dataset": "user_count_summary",
     "endpoint": "/copilot/reports/getMicrosoft365CopilotUserCountSummary(period='D30')",
     "destination_table": "user_count_summary"},
 
    {**LEGORA, "source": "legora", "dataset": "audit_logs",
     "endpoint": "/audit-logs", "records_path": "data",
     "date_param_from": "from", "date_param_to": "to",
     "destination_table": "audit_logs",
     "ingestion_type": "incremental", "start_date": str(today - timedelta(days=90))},
 
    {**LEGORA, "source": "legora", "dataset": "users",
     "endpoint": "/users", "records_path": "data",
     "destination_table": "users", "ingestion_type": "full"},
]
 
wanted = [s.strip() for s in sources.split(",") if s.strip()]
queue = [d for d in DATASETS if not wanted or d["source"] in wanted]


# In[15]:


results, failures = [], []
 
for cfg in queue:
    label = f"{cfg['source']}.{cfg['dataset']}"
    params = {k: v for k, v in cfg.items() if k not in ("source", "dataset") and v != ""}
    # params["destination_schema"] = cfg["source"]
    try:
        res = json.loads(notebookutils.notebook.run(NOTEBOOK, TIMEOUT, params))
        results.append({"dataset": label, **res})
        print(f"OK   {label}: {res['rows']} rows -> {res['table']}")
    except Exception as exc:
        failures.append({"dataset": label, "error": str(exc)[:300]})
        print(f"FAIL {label}: {str(exc)[:300]}")
 
if failures:
    raise RuntimeError(json.dumps(failures, indent=2))
 
print(f"\n{len(results)} dataset(s) loaded")

