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
# META         },
# META         {
# META           "id": "cb63792a-b5e9-4730-86bb-ea5d49b778a8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, collect_list, concat_ws, lower
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = "bronze_grantdocument"
silver_lakehouse = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(table_name)

if table_name == "bronze_grantprofile":
    mssparkutils.notebook.exit(f"Skipping {table_name}")

cols_with_name = [c for c in df.columns if "name" in c.lower()]
selected_cols = ["grant_code"] + cols_with_name
df_selected = df.select([col(c) for c in selected_cols])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicates = (
    df.groupBy("grant_code")
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

agg_exprs = [
    concat_ws(", ", collect_list(col(c))).alias(c)
    for c in cols_with_name
]

df_combined = (
    df_selected.groupBy("grant_code")
    .agg(*agg_exprs)
    .orderBy("grant_code")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_table_name = "silver_dim_" + table_name.replace("bronze_", "")
print(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined.write.mode("overwrite").format("delta").saveAsTable(f"Silver_Horizon_Test.dbo.{silver_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
