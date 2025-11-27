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

# CELL ********************

workspaceID = "z_WV_FIN_DE"
silverlakehouseID = "Silver_SUN_Test.lakehouse"
silver_lakehouse = silverlakehouseID.split(".")[0]
silver_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{silverlakehouseID}/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(f"{silver_path}/Tables/dbo/silver_sun_test")
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_period = 2025100  

df_result = (
    df.filter((F.col("period") >= 2025001) & (F.col("period") <= current_period))
      .groupBy(
          "unqacctcode", "bu_code", "period", "ledger_code", "fiscal_year", "account_code",
          "unqt0", "asofendmonth", "mypbas_doc_number", "unq_mypbas_codet6", "a8_main_account","a9_project_no",
          "data_refresh", "date_added", "unqt1", "unqt2", "unqt3", "unqt4",
          "unqt5", "unqt6", "unqt7", "unqt8", "unqt9"
      )
      .agg(
          F.sum("local_currency_amount").alias("Local_Amount"),
          F.sum("usd_amount").alias("USD_Amount")
      )
)

display(df_result.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_name = f"gold_sun_fact_ytd"
df_result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Gold_Finance_LH.dbo.{gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("gold_sun_fact_ytd")
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
