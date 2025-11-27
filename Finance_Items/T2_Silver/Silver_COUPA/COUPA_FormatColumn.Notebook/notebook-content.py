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

# MARKDOWN ********************

# ## Rename columns from - to _

# PARAMETERS CELL ********************

table_name = "invoice_header"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from collections import Counter

raw_table = f"Bronze_COUPA_Test.bronze_{table_name}_incremental"
cured_table = f"Silver_COUPA_Test.silver_{table_name}_incremental"

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
df_renamed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cured_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
