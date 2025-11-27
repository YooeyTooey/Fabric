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

# PARAMETERS CELL ********************

table_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

full_table = f"Bronze_COUPA_Test.bronze_{table_name}_incremental"
df = spark.read.table(full_table)
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_date = df.withColumn("time_stamp", current_timestamp())
df_with_date.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(full_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
