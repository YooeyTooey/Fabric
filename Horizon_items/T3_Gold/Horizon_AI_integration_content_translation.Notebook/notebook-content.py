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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from synapse.ml.spark.aifunc.DataFrameExtensions import AIFunctions
from pyspark.sql.functions import col


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("silver_dim_grantresourcetypemap")
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

content = df.groupBy(df.resourcetypename).count().sort("resourcetypename")
display(content)
column_df = spark.createDataFrame([(c,) for c in content.columns],["text"])
display(column_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

language = "filipino"

translated_df = content.ai.translate(
    to_lang=language,
    input_col="resourcetypename",
    output_col="translation"
)

translated_columns = column_df.ai.translate(
    to_lang=language,
    input_col="text",
    output_col="translation"
)

column_map = {row["text"]: row["translation"] for row in translated_columns.collect()}

final_df = translated_df.select(
    col("translation").alias(column_map["resourcetypename"]), 
    col("count").alias(column_map["count"])                    
)

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
