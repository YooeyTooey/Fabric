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
# META           "id": "6726287a-4815-4a83-8397-99b3ffbf7d29"
# META         },
# META         {
# META           "id": "eb337e93-2630-42a8-b860-ff9834be952d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import col, count, trim, regexp_extract, lit, when
import os
import pandas as pd
import re
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_name = "bronze_grantresourcetypemap"
table_name2 = "bronze_grantprofile"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table(table_name)
display(df.head(10))
df2 = spark.read.table(table_name2)
display(df2.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "gpcode" in df.columns:
    key_column = "gpcode"
elif "grantcode" in df.columns:
    key_column = "grantcode"
else:
    mssparkutils.notebook.exit("Neither gpcode nor grantcode found in this table")

if "gpcode" in df2.columns:
    key_column2 = "gpcode"
elif "grantcode" in df2.columns:
    key_column2 = "grantcode"
else:
    mssparkutils.notebook.exit("Neither gpcode nor grantcode found in this table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


test = df.select(trim(col(key_column)).alias(key_column)).distinct()


test_issues = test.withColumn(
    "Issue",
    when(col(key_column).isNull(), lit("NULL value"))
    .when(col(key_column) == "", lit("Blank value"))
    .when(regexp_extract(col(key_column), r"[^a-zA-Z0-9_-]", 0) != "", lit("Contains special characters"))
    .otherwise(lit("Clean"))
)


test_issues_filtered = test_issues.filter(col("Issue") != "Clean")

display(test_issues_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = df.filter(col("grantcode") == "GFROWFPO0003")
display(test)

test2 = df2.filter(col("gpcode") == "GFROWFPO0003")
display(test2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicates = (
    df.groupBy(key_column)
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
)
display(duplicates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
