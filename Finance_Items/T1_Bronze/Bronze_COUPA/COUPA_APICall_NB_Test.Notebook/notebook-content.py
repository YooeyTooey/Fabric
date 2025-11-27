# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa",
# META       "default_lakehouse_name": "Bronze_COUPA_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Ingest data from Coupa API to Bronze Raw tbl Lakehouse**

# PARAMETERS CELL ********************

CLIENT_ID = "" #parameter
CLIENT_SECRET = "" #parameter
SCOPE = ""  #parameter
table_name =""
end_point = "" # parameter


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
import pandas as pd
from IPython.display import display
from pyspark.sql import functions as F, types as T

# --- Step 1: Config ---
BASE_URL = os.getenv("COUPA_BASE_URL", "https://wvi-stage.coupahost.com").rstrip("/")


#------------------------------------------ FOR DEBUGGING ONLY ------------------------------------#

# print("DEBUG", {"end_point": end_point, "table_name": table_name, "client_secret" : CLIENT_SECRET ,"client_id": CLIENT_ID, "scope": SCOPE})

#------------------------------------------ END OF DEBUGGING ------------------------------------#



# --- Step 2: Get token ---
payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": SCOPE
}

resp = requests.post(
    f"{BASE_URL}/oauth2/token",
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    },
    data=payload,
    timeout=(10, 60),
)
resp.raise_for_status()
token = resp.json().get("access_token")

# --- Step 3: Call Coupa endpoint ---

api_url = f"{BASE_URL}/api/{end_point}"

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

api_resp = requests.get(api_url, headers=headers, timeout=(10, 60))
api_resp.raise_for_status()

# --- Step 4: Convert JSON to table ---
data = api_resp.json()
if isinstance(data, dict):
    data = [data]  # normalize to list

df = pd.json_normalize(data, sep="_")

# --- Step 5: Show table ---
display(df)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Flatten the table to get the array of columns

# CELL ********************



def flatten_schema(df, parent=None):
    """
    Recursively flattens struct fields and explodes arrays of structs.
    Returns a DataFrame with only primitive (non-struct/array) columns.
    """
    def qualified(n): return f"{parent}_{n}" if parent else n

    cols = []
    arrays = []

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType
        qname = f"`{name}`"

        if isinstance(dtype, T.StructType):
            # expand struct.* with prefix
            for sub in dtype.fields:
                cols.append(F.col(f"{qname}.{sub.name}").alias(qualified(f"{name}_{sub.name}")))
        elif isinstance(dtype, T.ArrayType):
            arrays.append((name, dtype))
        else:
            cols.append(F.col(qname).alias(qualified(name)))

    # If there is an array, explode it one-by-one and recurse
    if arrays:
        name, dtype = arrays[0]               # explode arrays sequentially
        exploded = df.select("*", F.posexplode_outer(F.col(f"`{name}`")).alias(f"{name}__pos", f"{name}__val")) \
                      .drop(name)
        # move the exploded value to a clean column name
        exploded = exploded.withColumn(name, F.col(f"{name}__val")).drop(f"{name}__val")

        # If array elements are structs, we’ll expand in the next recursion
        return flatten_schema(exploded, parent=parent)

    # No arrays left, but there may be structs we need to flatten deeper
    out = df.select(*cols)
    # Check again for any remaining structs after first pass
    if any(isinstance(f.dataType, T.StructType) for f in out.schema.fields):
        return flatten_schema(out, parent=parent)

    return out

# Example: convert your API payload to a Spark DF and flatten
import json
raw = api_resp.json()
raw = raw if isinstance(raw, list) else [raw]
spark_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw)]))
flat_spark = flatten_schema(spark_df)

# Write to Lakehouse (Delta)
full_table = f"Bronze_COUPA_Test.bronze_{table_name}_incremental"
flat_spark.write.mode("overwrite").format("delta").saveAsTable(full_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Load the table as a Spark DataFrame
spark_df = spark.table("bronze_COUPA_Test.bronze_invoice_header_incremental")

# Convert to pandas DataFrame
df = spark_df.toPandas()

# Detect duplicate columns
duplicates = [col for col in df.columns if df.columns.tolist().count(col) > 1]
print("Duplicate columns:", duplicates)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
