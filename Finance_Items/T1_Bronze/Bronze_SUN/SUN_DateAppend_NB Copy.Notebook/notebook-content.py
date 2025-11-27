# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c39d0feb-b253-47c3-afd9-29b725d43c5d",
# META       "default_lakehouse_name": "Bronze_SUN_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "c39d0feb-b253-47c3-afd9-29b725d43c5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Library Imports
# 
# - current_timestamp() function used in

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp
import os
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

workspaceID = ""
lakehouseID = ""
table_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_date = df.withColumn("date_added", current_timestamp())
df_with_date.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
