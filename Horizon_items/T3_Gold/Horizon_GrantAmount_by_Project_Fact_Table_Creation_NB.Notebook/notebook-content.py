# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cb63792a-b5e9-4730-86bb-ea5d49b778a8",
# META       "default_lakehouse_name": "Silver_Horizon_Test",
# META       "default_lakehouse_workspace_id": "4bd5b086-3b44-4645-8b35-d81df1d61106",
# META       "known_lakehouses": [
# META         {
# META           "id": "cb63792a-b5e9-4730-86bb-ea5d49b778a8"
# META         },
# META         {
# META           "id": "90df7a21-4ac4-4e5c-b979-a6c1734e54e6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Library Imports

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import functions as F
from pyspark.sql import Window
import json
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pipeline Parameters

# CELL ********************

goldworkspaceID = "z_WV_ITD_DA"
goldlakehouseID = "Gold_Horizon_Test.lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Table to Dataframe
# - silver lakehouse attached on the left side for easy relative access

# CELL ********************

df = spark.read.table("silver_fact_grantprofile")
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Gold Transformations
# 
# - trimmed certain columns to achieve desired value per row in the column
# - made aggregations like sum

# CELL ********************

df_result = (
    df.withColumn("office_code", F.substring(F.col("grant_code"), 1, 4))
      .withColumn("project_code", F.substring(F.col("funded_projects"), 1, 6))
      .groupBy("office_code", "project_code")
      .agg(F.sum("totalgrantamountinusd").alias("grant_amount"))
)

display(df_result.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sample User Defined Function
# 
# - A concrete example of how a frequently used function will be modularized and defined across fabric

# CELL ********************

def get_mount_path(lakehouse_name):
    mnt_point = f'/mnt/mnt_{lakehouse_name}'
    mssparkutils.fs.mount(lakehouse_name, mnt_point)
    return f'file:{mssparkutils.fs.getMountPath(mnt_point)}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Sample UDF usage within a notebook

# CELL ********************

gold_lakehouse =  goldlakehouseID.split(".")[0]

gold_path = f"abfss://{goldworkspaceID}@onelake.dfs.fabric.microsoft.com/{goldlakehouseID}/Tables/dbo/"
gold_mount = get_mount_path(gold_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Writing Dataframe to Lakehouse using Mount path + table name

# CELL ********************



gold_table_name = f"gold_fact_grantamount_by_project"
df_result.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(gold_path + gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Displaying table for validation

# CELL ********************

gold_table_name = f"gold_fact_grantamount_by_project"
display(spark.read.parquet(gold_path + gold_table_name).head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
