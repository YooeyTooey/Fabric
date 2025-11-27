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

raw_table = "Bronze_COUPA_Test.bronze_supplier_header_incremental"
cured_table = "Silver_COUPA_Test.silver_supplier_header_incremental"

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

# ## Change Data Types, Get only required columns

# CELL ********************

spark.sql("""
CREATE OR REPLACE TABLE Silver_COUPA_Test.silver_supplier_header_incremental
USING DELTA AS
SELECT
  CAST(id AS BIGINT)                        AS supplier_id,
  account_number                            AS account_number,
  contacts_id                               AS contacts_id,
  contacts_name_fullname                    AS contacts_name_fullname,
  contacts_email                            AS contacts_email,
  preferred_commodities                     AS preferred_commodities,
  commodity_active                          AS commoditiy_active,
  commodity_category                        AS commodity_category,
  to_timestamp(created_at)                  AS created_date,
  created_by_fullname                       AS created_by_fullname,
  invoice_matching_level                    AS invoice_matching_level,
  name                                      AS supplier_name,
  number                                    AS supplier_number,
  on_hold                                   AS supplier_on_hold,
  one_time_supplier                         AS one_time_supplier,
  primary_address_country_name              AS primary_address_country_name,
  primary_contact_email                     AS primary_contact_email,
  status                                    AS status,
  to_timestamp(updated_at)                  AS updated_date,
  updated_by_fullname                       AS updated_by_fullname,
  type                                      AS type,
  time_stamp                                AS time_stamp
FROM Silver_COUPA_Test.silver_supplier_header_incremental
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for col in df.columns:
    print(col)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
