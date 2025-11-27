# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa",
# META       "default_lakehouse_name": "Bronze_COUPA_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "2dcbfe8e-d9e8-4137-95fe-44a4db0795aa"
# META         },
# META         {
# META           "id": "cbdc82e1-4401-48a5-91dd-1264d23fb23b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Change column name format from - to _

# CELL ********************

from pyspark.sql.functions import col
from collections import Counter

raw_table = "Bronze_COUPA_Test.bronze_invoice_header_incremental"
cured_table = "Silver_COUPA_Test.silver_invoice_header_incremental"

df = spark.read.table(raw_table)

old_cols = df.columns
norm_cols = [c.replace('-', '_') for c in old_cols]

# Handle collisions after normalization by adding suffixes
seen = Counter()
new_cols = []
for name in norm_cols:
    if seen[name] == 0:
        new_cols.append(name)
    else:
        new_cols.append(f"{name}__dup{seen[name]}")
    seen[name] += 1

# Apply all renames in one select
df_renamed = df.select([col(o).alias(n) for o, n in zip(old_cols, new_cols)])

# Check duplicates (should be none after our dedupe step)
dupes = [c for c, n in Counter(df_renamed.columns).items() if n > 1]
print("Duplicate columns:", dupes)

# (Optional) save to silver
df_renamed.write.mode("overwrite").saveAsTable(cured_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ## Change Data Types, Get only required columns, Business rules

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE Silver_COUPA_Test.silver_invoice_header_incremental
USING DELTA AS
SELECT
  CAST(id AS BIGINT)                        AS invoice_id,
  to_timestamp(created_at)                  AS created_date,
  to_timestamp(updated_at)                  AS updated_date,
  channel                                   AS delivery_method,
  to_timestamp(invoice_date)                AS invoice_date,
  invoice_number                            AS invoice_number,
  CASE
    WHEN status = "new" THEN "New"
    WHEN status = "void" THEN "Void"
    ELSE status                                   
  END                                       AS invoice_status,
  CAST(total_with_taxes AS DECIMAL(18,2))   AS total,
  to_timestamp(net_due_date)                AS net_due_date,
  CASE
    WHEN lower(CAST(paid AS STRING)) IN ('true','1','yes','y') THEN TRUE
    WHEN lower(CAST(paid AS STRING)) IN ('false','0','no','n') THEN FALSE
    ELSE NULL
  END                                       AS paid,
  CAST(NULL AS TIMESTAMP)                   AS payment_date,
  CAST(NULL AS TIMESTAMP)                   AS date_received,
  to_timestamp(date_rs)                     AS invoice_receipt_date_from_supplier,
  CAST(gpo_processing_fee AS DECIMAL(18,2)) AS gpo_processing_fee,
  to_timestamp(gl_date)                     AS gl_date,
  CAST(NULL AS STRING)                      AS printer_location,
  national_office_po                        AS national_office_po,
  invoice_description                       AS invoice_description,
  CAST(NULL AS STRING)                      AS lco_t3_code,
  custom_fields_lco_t8_code                 AS lco_t8_code,
  CAST(NULL AS STRING)                      AS lco_t9_code,
  CAST(NULL AS STRING)                      AS lco_tax_id,
  CAST(NULL AS STRING)                      AS coa,
  currency_code                             AS currency,
  payment_method                            AS payment_term,
  CAST(NULL AS STRING)                      AS shipping_term,
  CAST(NULL AS STRING)                      AS supplier_number,
  CAST(NULL AS STRING)                      AS supplier_name,
  created_by_login                          AS created_by_login,
  created_by_fullname                       AS created_by_name,
  updated_by_login                          AS updated_by_login,
  updated_by_fullname                       AS updated_by_name,
  CAST(NULL AS TIMESTAMP)                   AS invoice_approval_date,
  time_stamp                                AS time_stamp
FROM Silver_COUPA_Test.silver_invoice_header_incremental
""")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Silver_COUPA_Test.silver_invoice_header_incremental")

cols_df = spark.createDataFrame([(c,) for c in df.columns], ["column_name"])
display(cols_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver_COUPA_Test.silver_invoice_header_incremental LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
