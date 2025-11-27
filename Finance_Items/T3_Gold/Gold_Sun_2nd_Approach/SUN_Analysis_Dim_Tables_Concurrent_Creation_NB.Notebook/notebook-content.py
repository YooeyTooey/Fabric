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

from concurrent.futures import ThreadPoolExecutor, as_completed
import ast
import re
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceID = "z_WV_FIN_DE"
silverlakehouseID = "Silver_SUN_Test.lakehouse"
silver_lakehouse = silverlakehouseID.split(".")[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mount_name = "/SUN_mnt"

silver_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{silverlakehouseID}/"
mssparkutils.fs.mount(silver_path, mount_name)
mount_points = mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet(f"{silver_path}Tables/dbo/silver_sun_analysis")
silver_lakehouse = silverlakehouseID.split(".")[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def process_dimension(dim, info, df_deduped, i):
    la_code = info["code"].upper()
    name_label = info["name"]
    t_name_col = f"{dim}_{name_label}"

    df_dim = df_deduped.filter(F.col("position") == la_code)

    df_dim = df_dim.withColumn(
        "unqcode", F.concat_ws("", F.col("bu_code"), F.col("analysis_code"))
    )

    df_dim = df_dim.withColumn(t_name_col, F.lit(f"t{i}_{name_label}"))

    df_dim = df_dim.select(
        "unqcode",
        F.col("analysis_code").alias(f"{dim}_code"),
        F.col("analysis_name").alias(t_name_col),
        "date_added"
    )

    print(f"Preview for {dim}:")
    df_dim.show(5, truncate=False)
    df_dim_dupes = (
        df_dim.groupBy("unqcode")
              .count()
              .filter(F.col("count") > 1)
    )

    print(f"Duplicates in {dim}:")
    df_dim_dupes.show(truncate=False)

    gold_table_name = f"gold_sun_dim_{dim}"

    df_dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Gold_Finance_LH.dbo.{gold_table_name}")
    print(f"✅ Finished writing {gold_table_name}")

    return dim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = (
        Window.partitionBy("bu_code", "analysis_code")
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

df_mapping = spark.read.option("multiline", "true").json(f"{silver_path}/Files/Configuration/Gold_SUN_Column_Information.json")
column_standards = {row["dimension"]: {"code": row["code"], "name": row["name"]}
    for row in df_mapping.collect()}
print(column_standards)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [
        executor.submit(process_dimension, dim, info, df_deduped, i)
        for i, (dim, info) in enumerate(column_standards.items())
    ]

    for future in as_completed(futures):
        try:
            dim_processed = future.result()
            print(f"✅ Completed {dim_processed}")
        except Exception as e:
            print(f"❌ Error processing dimension: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
