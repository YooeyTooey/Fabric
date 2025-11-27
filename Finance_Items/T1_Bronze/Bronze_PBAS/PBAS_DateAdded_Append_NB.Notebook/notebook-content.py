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
from pyspark.sql.functions import current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = "Bronze_PBAS_Test"
df = spark.read.table(Table_Name)
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_date = df.withColumn("date_added", current_timestamp())
df_with_date.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(Table_Name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
