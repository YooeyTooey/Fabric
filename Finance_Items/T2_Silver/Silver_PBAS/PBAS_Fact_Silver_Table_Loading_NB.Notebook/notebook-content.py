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
# META         },
# META         {
# META           "id": "d8662dd1-f8a5-4ecc-8fc8-b2475b3b498e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
import ast

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_name = "Bronze_PBAS_Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************


silverlakehouseID = ""
tables = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = ast.literal_eval(tables)
silver_lakehouse = silverlakehouseID.split(".")[0]
silver_table_name = table_name.replace("Bronze_", "silver_")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_col(col_name):

     return re.sub(r'[^0-9a-zA-Z_]', '', col_name.strip().replace(" ", "_").lower())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_new = spark.read.table(table_name)

cleaned_cols = [clean_col(c) for c in df_new.columns]
df_cleaned = df_new.toDF(*cleaned_cols)
columns_to_keep = ["mypbas_doc_number", "date_added", "pbas_as_of_date", "office_code", "projectnumber"]

for table in tables:
    
    t = table.lower()
    if t == table_name.lower():
        continue
    
    try:
        
        dim_df = spark.read.table(t)
        dim_cols = [clean_col(c) for c in dim_df.columns]

        common_cols = set(dim_cols).intersection(set(df_cleaned.columns))

        cols_to_drop = [c for c in common_cols if c not in columns_to_keep]

        if cols_to_drop:
            print(f"Dropping {len(cols_to_drop)} columns from {t}: {cols_to_drop}")
            df_cleaned = df_cleaned.drop(*cols_to_drop)
        else:
            print(f"No droppable columns found for {t} — skipping.")

    except Exception as e:
        print(f"Error reading {t}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_exists = spark.catalog.tableExists(f"{silver_lakehouse}.dbo.{silver_table_name}")

if table_exists:
    df_existing = spark.read.table(f"{silver_lakehouse}.dbo.{silver_table_name}")
    df_combined = df_existing.unionByName(df_cleaned, allowMissingColumns=True)
else:
    df_combined = df_cleaned

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

windowSpec = Window.partitionBy("mypbas_doc_number")
df_combined = (
    df_combined.withColumn(
        "iscurrent",
        F.when(
            F.col("pbas_as_of_date") == F.max("pbas_as_of_date").over(windowSpec),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "isendofmonth",
        F.when(
            F.last_day("pbas_as_of_date") == F.col("pbas_as_of_date"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_combined.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_lakehouse}.dbo.{silver_table_name}")
mssparkutils.notebook.exit(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_combined.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
