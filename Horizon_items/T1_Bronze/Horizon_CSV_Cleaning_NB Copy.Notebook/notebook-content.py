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
# META           "id": "6726287a-4815-4a83-8397-99b3ffbf7d29"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Library Imports

# CELL ********************

from notebookutils import mssparkutils
import os
import pandas as pd
import re
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pipeline Parameters
# 
# - variable file_name declared to accept individual file name from Horizon_Lakehouse_Mounting_NB file list.

# PARAMETERS CELL ********************

file_name = ""
last_modified = ""
workspaceID = ""
lakehouseID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Mount Path
# 
# - Due to frequency of this exact mechanism (retrieving an accessible mount/path for a certain lakehouse), code block can be transformed as a modular, on-demand, user-defined-function.

# CELL ********************


mount_name = "/Horizon_mnt"

base_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{lakehouseID}/"
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()

file_path = base_path + "/Files/Input/"+ file_name



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # CSV to Dataframe
# 
# - utilizing spark Dataframes, csv files passed to this notebook is accessed and read to be converted to a spark Dataframe, available for further table specific operations and modifications.

# CELL ********************

csv_path = "/Files/Archive/GrantDocument.csv"
df = spark.read.option("header", True).option("inferSchema", True).option("multiline", True).option("quote", "\"") .option("escape", "\"").csv(file_path)

display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transformation specific user defined function
# - The function below is an example of a frequently used operation which is to clean column names by replacing whitespaces with underscores and using lower case as a standard format. (Format may be arbitrarily improved upon)

# CELL ********************

def clean_col(col_name):

     return re.sub(r'[^0-9a-zA-Z_]', '', col_name.strip().replace(" ", "_").lower())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dataframe Cleansing and Writing
# 
# - A sample code block for utilizing previously defined functions to clean/prep data for the minimum requirements of fabric not yet including business specific rules.
# - Shows the utilization of mount paths to absolutely access/write on lakehouses although tasks requiring only one lakehouse may operate with just assigning a default lakehouse environment beside the notebook in the Explorer section.
# - Also shows a sample exit output to inform process/pipeline loaders the current status of the notebook and its output.

# CELL ********************

cleaned_cols = [clean_col(c) for c in df.columns]
df_cleaned = df.toDF(*cleaned_cols)

base_table_name = f"bronze_{file_name.split('.')[0].lower()}"

local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)
tables_path = local_path +  "/Tables"

tables = os.listdir(tables_path)

df_cleaned = (
    df_cleaned
    .withColumn("last_modified", F.lit(last_modified))
    .withColumn("date_added", current_timestamp())
)

if base_table_name in tables:
   
    df_cleaned.write.mode("append").saveAsTable(f"{base_table_name}")
    mssparkutils.notebook.exit(f"✅ Appended data to existing table: {base_table_name}")
else:
    
    df_cleaned.write.mode("overwrite").saveAsTable(f"{base_table_name}")
    mssparkutils.notebook.exit(f"✅ Created new table: {base_table_name}")


#display(df_cleaned.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
