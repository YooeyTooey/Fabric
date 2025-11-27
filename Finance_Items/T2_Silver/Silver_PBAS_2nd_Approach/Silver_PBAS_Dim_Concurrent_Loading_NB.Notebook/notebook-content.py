# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1bc7e1c8-5772-47d5-9649-4d937d1d7450",
# META       "default_lakehouse_name": "Bronze_PBAS_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "1bc7e1c8-5772-47d5-9649-4d937d1d7450"
# META         },
# META         {
# META           "id": "d8662dd1-f8a5-4ecc-8fc8-b2475b3b498e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from concurrent.futures import ThreadPoolExecutor, as_completed
import ast
import re
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

tables = "['Bronze_PBAS_Test', 'bronze_dim_pbas_budgettypedw', 'bronze_dim_pbas_countrycode', 'bronze_dim_pbas_directfund', 'bronze_dim_pbas_fundingofficedw', 'bronze_dim_pbas_fundingtypedw', 'bronze_dim_pbas_grantattributes', 'bronze_dim_pbas_implementingofficedw', 'bronze_dim_pbas_isprogramtpapothersbothdw', 'bronze_dim_pbas_ministrytypedw', 'bronze_dim_pbas_mypbasdocnumberdw', 'bronze_dim_pbas_resourcetypedw', 'bronze_dim_pbas_specialcode', 'bronze_dim_pbas_specialflagname', 'bronze_dim_pbas_specialfunddw', 'bronze_dim_pbas_t7primarycatdw']"
silverlakehouseID = "Silver_PBAS_Test.lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = ast.literal_eval(tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(tables)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns_to_drop = ["date_added"]
silver_lakehouse = silverlakehouseID.split(".")[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_col(col_name):
     return re.sub(r'[^0-9a-zA-Z_]', '', col_name.strip().replace(" ", "_").lower())

def drop_cols_and_write_tables_to_silverlh_parallel(table):
    try:
        if table == "Bronze_PBAS_Test":
           
            return f"CREATED: {silver_table_name}"

        else:
            df = spark.read.table(table)
        
            for col in columns_to_drop:
                if col in df.columns:
                    df = df.drop(col)

            silver_table_name = table.replace("bronze_", "silver_")
            df.write.mode("overwrite").option("mergeSchema", "True"). saveAsTable(f"{silver_lakehouse}.dbo.{silver_table_name}")
            return f"CREATED: {silver_table_name}"
    except Exception as e:
        raise ValueError(f"AN ERROR OCCURED IN THIS TABLE: {table} ERROR: {e}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(drop_cols_and_write_tables_to_silverlh_parallel, table): table for table in tables}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
