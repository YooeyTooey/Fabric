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
silver_path = "abfss://z_WV_FIN_DE@onelake.dfs.fabric.microsoft.com/Silver_SUN_Test.Lakehouse"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(f"{silver_path}/Tables/dbo/silver_sun_chartofaccounts")
silver_lakehouse = silverlakehouseID.split(".")[0]

window_spec = (
    Window.partitionBy("bu_code", "account_code")
    .orderBy(
        F.desc(F.when(F.col("status") == 0, 1).otherwise(0)),  
        F.desc("created_datetime"),  
        F.desc("id")  
    )
)

df_deduped = (
    df.withColumn("row_rank", F.row_number().over(window_spec))
      .filter(F.col("row_rank") == 1)
      .drop("row_rank")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_deduped.head(10))

df_deduped.groupBy("id")\
        .count()\
        .filter(F.col("count") > 1)\
        .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_deduped.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

anal_cols = [
    F.col(f"acnt_anal_{i}").alias(f"A{i}") for i in range(0, 11) if f"acnt_anal_{i}" in df_deduped.columns
]

print(anal_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_table = (
    df_deduped
    .withColumn("unqcode", F.concat(F.col("bu_code"), F.col("account_code")))
    .select(
        "unqcode",
        "account_code",
        "account_name",
        "account_type",
        *anal_cols, 
        "date_added"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(gold_dim_table.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_dupes = (
    gold_dim_table.groupBy("unqcode")
          .count()
          .filter(F.col("count") > 1)
)


display(df_dim_dupes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_table_name = f"gold_sun_dim_chartofaccounts"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_table.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"Gold_Finance_LH.dbo.{gold_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
