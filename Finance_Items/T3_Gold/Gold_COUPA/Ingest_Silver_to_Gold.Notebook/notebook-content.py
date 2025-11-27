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
# META         },
# META         {
# META           "id": "cbdc82e1-4401-48a5-91dd-1264d23fb23b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspaceID = "z_WV_FIN_DE"
silverlakehouseID = "Silver_COUPA_Test.lakehouse"
silver_lakehouse = silverlakehouseID.split(".")[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ======================
# CONFIG
# ======================
# If you created a OneLake shortcut in the GOLD Lakehouse:
#SOURCE_TABLE = "dbo.silver_invoice_header_incremental"    # shortcut name under Tables
TARGET_TABLE = "dbo.gold_coupa_invoice_header"

# If you can't use a shortcut, load by ABFS path instead:
SOURCE_PATH = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{silverlakehouseID}/Tables/silver_invoice_header_incremental"
src_raw = spark.read.format("delta").load(SOURCE_PATH)


# Filter out null keys (equivalent to WHERE s.invoice_id IS NOT NULL)
src_raw = src_raw.filter(F.col("invoice_id").isNotNull())

display(src_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ======================
# APPLY "TRY_CONVERT"-style CASTS + STATUS REMAP
# (Spark casts return null on failure, matching TRY_CONVERT semantics)
# ======================
sdf = src_raw.select(
    F.col("invoice_id"),
    F.col("created_date").cast("timestamp").alias("created_date"),
    F.col("updated_date").cast("timestamp").alias("updated_date"),
    F.col("delivery_method"),
    F.col("invoice_date").cast("timestamp").alias("invoice_date"),
    F.col("invoice_number"),
    F.when(F.lower(F.col("invoice_status")) == "new", "New")
     .when(F.lower(F.col("invoice_status")) == "voided", "Voided")
     .otherwise(F.col("invoice_status")).alias("invoice_status"),
    F.col("total").cast(DecimalType(28, 2)).alias("total"),
    F.col("net_due_date").cast("timestamp").alias("net_due_date"),
    F.col("paid").cast("boolean").alias("paid"),
    F.col("invoice_receipt_date_from_supplier").cast("timestamp").alias("invoice_receipt_date_from_supplier"),
    F.col("gpo_processing_fee"),
    F.col("gl_date").cast("timestamp").alias("gl_date"),
    F.col("printer_location"),
    F.col("national_office_po"),
    F.col("invoice_description"),
    F.col("lco_t8_code"),
    F.col("currency"),
    F.col("payment_term"),
    F.col("supplier_number"),
    F.col("created_by_login"),
    F.col("created_by_name"),
    F.col("updated_by_login"),
    F.col("updated_by_name"),
    F.col("time_stamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ======================
# DEDUP: keep one row per invoice_id
# Rule: latest by updated_date -> time_stamp -> created_date (desc),
# then tie-break with invoice_date (desc) and invoice_number (asc)
# ======================
sdf = (sdf
    .withColumn("sort_key",
        F.coalesce(F.col("created_date"), F.col("time_stamp"))
    )
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("invoice_id")
              .orderBy(
                  F.col("sort_key").desc(),
                  F.col("invoice_date").desc_nulls_last(),
                  F.col("invoice_number").asc_nulls_last()
              )
    ))
    .filter(F.col("rn") == 1)
    .drop("rn", "sort_key")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ======================
# ENSURE TARGET TABLE EXISTS (create empty Delta table if needed)
# ======================
if not spark._jsparkSession.catalog().tableExists(TARGET_TABLE):
    (sdf.limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE))

# ======================
# Quick counts for updated vs inserted (pre-merge)
# (Join against current target keys)
# ======================
tgt_ids = spark.table(TARGET_TABLE).select("invoice_id")
updated_rows = sdf.join(tgt_ids, "invoice_id", "inner").count()
inserted_rows = sdf.join(tgt_ids, "invoice_id", "left_anti").count()

# ======================
# MERGE/UPSERT (atomic in Delta)
# ======================
tgt = DeltaTable.forName(spark, TARGET_TABLE)
(tgt.alias("d")
    .merge(sdf.alias("s"), "d.invoice_id = s.invoice_id")
    .whenMatchedUpdate(set={
        "invoice_id": "s.invoice_id",
        "created_date": "s.created_date",
        "updated_date": "s.updated_date",
        "delivery_method": "s.delivery_method",
        "invoice_date": "s.invoice_date",
        "invoice_number": "s.invoice_number",
        "invoice_status": "s.invoice_status",
        "total": "s.total",
        "net_due_date": "s.net_due_date",
        "paid": "s.paid",
        "invoice_receipt_date_from_supplier": "s.invoice_receipt_date_from_supplier",
        "gpo_processing_fee": "s.gpo_processing_fee",
        "gl_date": "s.gl_date",
        "printer_location": "s.printer_location",
        "national_office_po": "s.national_office_po",
        "invoice_description": "s.invoice_description",
        "lco_t8_code": "s.lco_t8_code",
        "currency": "s.currency",
        "payment_term": "s.payment_term",
        "supplier_number": "s.supplier_number",
        "created_by_login": "s.created_by_login",
        "created_by_name": "s.created_by_name",
        "updated_by_login": "s.updated_by_login",
        "updated_by_name": "s.updated_by_name",
        "time_stamp": "s.time_stamp"
    })
    .whenNotMatchedInsert(values={
        "invoice_id": "s.invoice_id",
        "created_date": "s.created_date",
        "updated_date": "s.updated_date",
        "delivery_method": "s.delivery_method",
        "invoice_date": "s.invoice_date",
        "invoice_number": "s.invoice_number",
        "invoice_status": "s.invoice_status",
        "total": "s.total",
        "net_due_date": "s.net_due_date",
        "paid": "s.paid",
        "invoice_receipt_date_from_supplier": "s.invoice_receipt_date_from_supplier",
        "gpo_processing_fee": "s.gpo_processing_fee",
        "gl_date": "s.gl_date",
        "printer_location": "s.printer_location",
        "national_office_po": "s.national_office_po",
        "invoice_description": "s.invoice_description",
        "lco_t8_code": "s.lco_t8_code",
        "currency": "s.currency",
        "payment_term": "s.payment_term",
        "supplier_number": "s.supplier_number",
        "created_by_login": "s.created_by_login",
        "created_by_name": "s.created_by_name",
        "updated_by_login": "s.updated_by_login",
        "updated_by_name": "s.updated_by_name",
        "time_stamp": "s.time_stamp"
    })
    .execute())

print({"updated_rows_estimate": updated_rows, "inserted_rows_estimate": inserted_rows})


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
