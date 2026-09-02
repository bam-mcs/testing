#!/usr/bin/env python
# coding: utf-8

# ## nb_api_to_bronze
# 
# null

# In[ ]:


# PARAMETERS CELL ********************

# destination
destination_schema = ""                 # source system name, created if missing
destination_table  = ""
ingestion_type     = "incremental"      # incremental | full
start_date         = "2020-01-01"
end_date           = ""
 
# auth
auth_type            = "none"           # none | api_key | oauth2_client_credentials
key_vault_uri        = ""
token_secret         = ""
client_id_secret     = ""
client_secret_secret = ""
audience_secret      = ""
scope_secret         = ""
token_url            = ""
scope                = ""
audience             = ""
 
# source
base_url        = ""
endpoint        = ""
query_params    = "{}"
extra_headers   = "{}"
date_param_from = ""
date_param_to   = ""
 
# response
response_format = "json"                # json | csv
records_path    = ""
pagination      = "none"                # none | next_link | cursor
cursor_param    = "cursor"
cursor_field    = "nextCursor"
page_size_param = ""
page_size       = 1000
max_pages       = 100
request_delay   = 0                     # seconds between requests, for rate limited APIs


# In[ ]:


print(destination_table)


# In[ ]:


import json, re, time, requests, notebookutils
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
 
if not destination_schema or not destination_table:
    raise ValueError("destination_schema and destination_table are required")
 
table = f"{destination_schema}.{destination_table}"
 
 
def secret(name):
    return notebookutils.credentials.getSecret(key_vault_uri, name).strip() if name else ""
 
 
def auth_header():
    if auth_type == "api_key":
        return {"Authorization": f"Bearer {secret(token_secret)}"}
    if auth_type == "oauth2_client_credentials":
        data = {"grant_type": "client_credentials",
                "client_id": secret(client_id_secret),
                "client_secret": secret(client_secret_secret)}
        sc, au = secret(scope_secret) or scope, secret(audience_secret) or audience
        if sc:
            data["scope"] = sc
        if au:
            data["audience"] = au
        r = requests.post(token_url, data=data, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Token failed {r.status_code}: {r.text[:300]}")
        return {"Authorization": f"Bearer {r.json()['access_token']}"}
    return {}
 
 
headers = {"Accept": "application/json" if response_format == "json" else "text/csv"}
headers.update(json.loads(extra_headers or "{}"))
headers.update(auth_header())
 


# In[ ]:


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {destination_schema}")
table_exists = spark.catalog.tableExists(table)
 
if ingestion_type == "incremental" and table_exists:
    wm = spark.sql(f"SELECT max(ingestion_timestamp) FROM {table}").collect()[0][0]
    from_date = wm.date().isoformat() if wm else start_date
else:
    from_date = start_date
 
to_date = end_date or datetime.now(timezone.utc).date().isoformat()
 
params = json.loads(query_params or "{}")
if date_param_from:
    params[date_param_from] = from_date
if date_param_to:
    params[date_param_to] = to_date
if page_size_param:
    params[page_size_param] = page_size
 
print(f"{table} | {ingestion_type} | {from_date} to {to_date} | exists={table_exists}")


# In[ ]:


url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
records, csv_text, cursor, next_url, page = [], "", None, url, 0

while next_url and page < max_pages:
    page += 1
    if page > 1 and request_delay:
        time.sleep(request_delay)

    q = dict(params)
    if cursor:
        q[cursor_param] = cursor
    r = requests.get(next_url, params=q or None, headers=headers, timeout=500, allow_redirects=False)

    # CSV report endpoints redirect to storage, which rejects the auth header
    if r.status_code in (301, 302, 303, 307, 308):
        r = requests.get(r.headers["Location"], timeout=500)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} from {next_url}: {r.text[:400]}")

    if response_format == "csv":
        csv_text = r.text
        break

    payload = r.json()
    batch = payload
    for p in [x for x in records_path.split(".") if x]:
        batch = batch.get(p, []) if isinstance(batch, dict) else []
    records += batch

    if pagination == "next_link":
        next_url, params = payload.get("@odata.nextLink"), {}
    elif pagination == "cursor":
        cursor = payload.get(cursor_field) or (payload.get("meta") or {}).get(cursor_field)
        next_url = url if cursor and batch else None
    else:
        next_url = None

print(f"{page} request(s), {len(records) if response_format == 'json' else 'csv'} record(s)")




# In[ ]:


def clean(c):
    c = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(c).replace("\ufeff", "").strip())
    return re.sub(r"[^0-9a-zA-Z_]+", "_", c).strip("_").lower()


if response_format == "csv":
    path = f"Files/tmp/{destination_schema}_{destination_table}.csv"
    notebookutils.fs.put(path, csv_text.lstrip("\ufeff"), True)
    # no inferSchema, so every column reads as string
    df = (spark.read.option("header", "true").option("encoding", "UTF-8")
          .option("multiLine", "true").csv(path))
else:
    if not records:
        notebookutils.notebook.exit(json.dumps({"table": table, "rows": 0, "status": "no_data"}))
    # explicit all-string schema over the union of keys, so inference cannot vary between batches
    keys = list(dict.fromkeys(k for r in records for k in r))
    schema = StructType([StructField(k, StringType(), True) for k in keys])
    df = spark.read.schema(schema).json(
        spark.sparkContext.parallelize([json.dumps(x, default=str) for x in records]))

df = df.toDF(*[clean(c) for c in df.columns]).withColumn("ingestion_timestamp", F.current_timestamp())

mode = "append" if ingestion_type == "incremental" else "overwrite"
option = "mergeSchema" if mode == "append" else "overwriteSchema"
df.write.format("delta").mode(mode).option(option, "true").saveAsTable(table)

rows = df.count()
print(f"{rows} rows {mode} to {table}")
notebookutils.notebook.exit(json.dumps({"table": table, "rows": rows, "status": "success",
                                        "from": from_date, "to": to_date}))

