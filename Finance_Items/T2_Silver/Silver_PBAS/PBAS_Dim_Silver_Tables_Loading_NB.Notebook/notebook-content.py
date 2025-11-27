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
# META           "id": "d8662dd1-f8a5-4ecc-8fc8-b2475b3b498e"
# META         },
# META         {
# META           "id": "1bc7e1c8-5772-47d5-9649-4d937d1d7450"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = ""
silverlakehouseID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_table_name = table_name.replace("bronze_", "silver_")
silver_lakehouse = silverlakehouseID.split(".")[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_drop = ["date_added"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(table_name)

for col in columns_to_drop:
    if col in df.columns:
        df = df.drop(col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").option("mergeSchema", "True").saveAsTable(f"{silver_lakehouse}.dbo.{silver_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
