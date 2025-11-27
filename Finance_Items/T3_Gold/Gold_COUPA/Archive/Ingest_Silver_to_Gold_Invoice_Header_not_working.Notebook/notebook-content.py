# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cbdc82e1-4401-48a5-91dd-1264d23fb23b",
# META       "default_lakehouse_name": "Silver_COUPA_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "cbdc82e1-4401-48a5-91dd-1264d23fb23b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import json
import re
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src_view = "Silver_COUPA_Test.dbo.silver_invoice_header_incremental"   # your source in the Lakehouse / temp view
tgt_table = "Gold_Finance.dbo.gold_coupa_invoice_header"        # Delta target in the Lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 1: Read silver Delta and register temp view


# <-- SET THIS: path to your silver incremental delta (source)
abfss_delta_path = "abfss://z_WV_FIN_DE@onelake.dfs.fabric.microsoft.com/Silver_COUPA_Test.Lakehouse/Tables/silver_invoice_header_incremental"

print("Loading silver Delta from:", abfss_delta_path)
df_raw = spark.read.format("delta").load(abfss_delta_path)

print("Source row count:", df_raw.count())

# Register temp view for downstream gold processing
tmp_view = "tmp_silver_invoice_header_incremental"
view_test = df_raw.createOrReplaceTempView(tmp_view)
print(f"Temporary view created: {tmp_view}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target = spark.read.synapsesql("Gold_Finance.dbo.gold_coupa_invoice_header")
print(target.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_test = spark.read.table("silver_invoice_header_incremental")
print(df_test.columns)
df_test = df_test.withColumn("inbox_entry_date", F.lit(None).cast("timestamp")).withColumn("hub_entity", F.lit(None)).withColumn("blanket_po", F.lit(None)).withColumn("on_time_payment", F.lit(None))\
.withColumn("on_time_payment", F.lit(None)).withColumn("scanning_on_time", F.lit(None)).withColumn("overdue_from_supplier", F.lit(None)).withColumn("registering_on_time", F.lit(None))
display(df_test.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = (
    Window.partitionBy("invoice_id")
    .orderBy(
        F.desc("created_date"),   
    )
)

df_deduped = (
    df_test.withColumn("row_rank", F.row_number().over(window_spec))
      .filter(F.col("row_rank") == 1)
      .drop("row_rank")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_dupes = (
    df_deduped.groupBy("invoice_id")
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

display(df_deduped.head(1000))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target = spark.read.synapsesql("Gold_Finance.dbo.gold_coupa_invoice_header")

display(target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
target = spark.read.synapsesql("Gold_Finance.dbo.gold_coupa_invoice_header")

# 'target_table' would be an existing table you want to merge into
#merge_writer = df_deduped.mergeInto(target, "df_deduped.invoice_id = target.invoice_id")
#merger_write = merge_writer.whenMatched().updateAll().whenNotMatched().insertAll()

#merge_writer.merge()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "gold_coupa_invoice_header")

delta_table.alias("target").merge(
    df_deduped.alias("source"),
    "target.invoice_id = source.invoice_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tgt_table = "Gold_Finance.dbo.gold_coupa_invoice_header" 

#tgt_table.alias("t").merge(df_deduped.alias("s"), "t.invoice_id = s.invoice_id").whenMatched().updateAll().whenNotMatched().insertAll().execute()

df_deduped.merge(synapsesql(tgt_table),df_deduped.col("invoice_id")).whenMatched().updateAll().whenNotMatched().insertAll()

tgt_table.write.mode("overwrite")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
