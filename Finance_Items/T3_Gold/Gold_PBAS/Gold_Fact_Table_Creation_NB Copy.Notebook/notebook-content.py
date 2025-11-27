# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eb337e93-2630-42a8-b860-ff9834be952d",
# META       "default_lakehouse_name": "Gold_Finance_LH",
# META       "default_lakehouse_workspace_id": "5a5a7ce9-c78d-4181-9489-87935e86fb7d",
# META       "known_lakehouses": [
# META         {
# META           "id": "eb337e93-2630-42a8-b860-ff9834be952d"
# META         }
# META       ]
# META     }
# META   }
# META }

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

# PARAMETERS CELL ********************

workspaceID = "z_WV_FIN_DE"
silverlakehouseID = "Silver_PBAS_Test.lakehouse"
silver_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{silverlakehouseID}/"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(f"{silver_path}Tables/dbo/silver_pbas_test")
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_grouped = (
    df.groupBy(
        "office_code",
        "funding_office_code",
        "program_number",
        "mypbas_doc_number",
        "reserve_number",
        "reserve_name",
        "fiscal_year",
        "pbas_as_of_date",
        "projectNumber",
        "date_added",
        "iscurrent",
        "isendofmonth"
    )
    .agg(
        F.sum("no_planned_budget").alias("no_planned_budget"),
        F.sum("approved_commitment").alias("approved_commitment"),
        F.sum("actual_ytd_expense").alias("actual_ytd_expense"),
        F.sum("approved_cf").alias("approved_cf"),
        F.sum("proposed_cf").alias("proposed_cf"),
        F.sum("so_forecast_budget").alias("so_forecast_budget")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_grouped.filter(df_grouped.office_code != "NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_grouped.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_name = f"gold_pbas_fact"
df_grouped.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Gold_Finance_LH.dbo.{gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
