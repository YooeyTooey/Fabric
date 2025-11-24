# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6726287a-4815-4a83-8397-99b3ffbf7d29",
# META       "default_lakehouse_name": "Bronze_Horizon_Test",
# META       "default_lakehouse_workspace_id": "4bd5b086-3b44-4645-8b35-d81df1d61106",
# META       "known_lakehouses": [
# META         {
# META           "id": "cb63792a-b5e9-4730-86bb-ea5d49b778a8"
# META         },
# META         {
# META           "id": "6726287a-4815-4a83-8397-99b3ffbf7d29"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import col
import com.microsoft.spark.fabric
import os
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

bronze_table_name = "bronze_grantprofile"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df = spark.read.table(bronze_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




possible_names = ["grantcode", "gpcode"]
target_name = "grant_code"

existing_name = next((c for c in possible_names if c in df.columns), None)

if existing_name:
    df = df.withColumnRenamed(existing_name, target_name)
    
    for old_col in possible_names:
        if old_col.strip() in df.columns and old_col != target_name:
            df = df.drop(old_col)

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)
        
    mssparkutils.notebook.exit(f"✅ Renamed '{existing_name}' → '{target_name}' and dropped old columns.")
    
elif bronze_table_name == "bronze_grantprofile":
        silver_table_name = "silver_fact_" + bronze_table_name.replace("bronze_", "")
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Silver_Horizon_Test.dbo.{silver_table_name}")
else:
    mssparkutils.notebook.exit("⚠️ No grantcode/gpcode column found in this table.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
