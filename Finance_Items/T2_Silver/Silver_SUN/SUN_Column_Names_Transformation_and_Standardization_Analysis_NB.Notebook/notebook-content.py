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
# META           "id": "092b1c77-3c55-489e-8dd5-e59480f92bba"
# META         },
# META         {
# META           "id": "c39d0feb-b253-47c3-afd9-29b725d43c5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp
import json
import re



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

silverlakehouseID = "Silver_SUN_Test.lakehouse"
print(silverlakehouseID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "Bronze_SUN_Analysis"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(table_name)
silver_lakehouse = silverlakehouseID.split(".")[0]
silver_table_name = table_name.replace("Bronze_", "silver_")

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

def get_mount_path(lakehouse_name):
    mnt_point = f'/mnt/mnt_{lakehouse_name}'
    mssparkutils.fs.mount(lakehouse_name, mnt_point)
    return f'file:{mssparkutils.fs.getMountPath(mnt_point)}'
    
def clean_col(col_name):

     return re.sub(r'[^0-9a-zA-Z_]', '', col_name.strip().replace(" ", "_").lower())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_mnt = get_mount_path(silver_lakehouse)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for col in df.columns:
    if col.lower() == "office_code":
        df = df.withColumnRenamed(col, "bu_code")
    else: 
        df = df.withColumnRenamed(col,clean_col(col))
df_cleaned = (
    df
    .withColumn("date_added", current_timestamp())
)

display(df_cleaned.head(10))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{silver_lakehouse}.dbo.{silver_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cleaned.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
