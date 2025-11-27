# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1bc7e1c8-5772-47d5-9649-4d937d1d7450",
# META       "default_lakehouse_name": "Bronze_PBAS_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "1bc7e1c8-5772-47d5-9649-4d937d1d7450"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
import os
import json
import pandas as pd
import re
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

workspaceID = ""
lakehouseID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


mount_name = "/PBAS_mnt"

base_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{lakehouseID}/"
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()

file_path = base_path + "/Files/Configuration/PBAS_DIMENSIONS.json"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.json(file_path)

display(df)

raw_json = df.toJSON().collect()[0]
data = json.loads(raw_json)


entities = data.get("pbi:mashup", {}).get("entities", [])

if not entities:

    entities = data.get("entities", [])

dimensions = {}

for entity in entities:
    name = entity.get("name", "")
    if name.lower().startswith("dim_"):
        columns = [attr["name"] for attr in entity.get("attributes", [])]
        dimensions[name] = columns

for dim_name, columns in dimensions.items():
    print(f"\n Table: {dim_name}")
    print("Columns:")
    for col in columns:
        print(f" - {col}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_table_name(name):
  
    return re.sub(r'[^0-9a-zA-Z_]', '_', name)
    
def clean_col(col_name):

     return re.sub(r'[^0-9a-zA-Z_]', '', col_name.strip().replace(" ", "_").lower())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)
tables_path = local_path +  "/Tables"

tables = os.listdir(tables_path)

fact = spark.table("Bronze_PBAS_Test")

for dim_name,columns in dimensions.items():

    existing_cols = [col for col in columns if col in fact.columns]

    if not existing_cols:
        print(f"Skipping {dim_name} - no matching columns in fact table.")
        continue
    
    dim_df = fact.select(*existing_cols).dropDuplicates()

    for old_col in dim_df.columns:
        new_col = clean_col(old_col)
        if new_col != old_col:
            dim_df = dim_df.withColumnRenamed(old_col, new_col)

    dim_df = dim_df.withColumn("date_added", current_timestamp())

    display(dim_df.limit(10))
    clean_dim_name = clean_table_name(dim_name)
    table_name = f"bronze_{clean_dim_name}"

    if table_name in tables:
   
        dim_df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        print(f"✅ Appended data to existing table: {table_name}")
    else:
        
        dim_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
        print(f"✅ Created new table: {table_name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
