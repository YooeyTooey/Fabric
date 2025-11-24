# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c39d0feb-b253-47c3-afd9-29b725d43c5d",
# META       "default_lakehouse_name": "Bronze_SUN_Test",
# META       "default_lakehouse_workspace_id": "cb65a0a4-f1b1-4adf-aaed-7cbff0148ef8",
# META       "known_lakehouses": [
# META         {
# META           "id": "c39d0feb-b253-47c3-afd9-29b725d43c5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils
import os
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

workspaceID = ""
silverlakehouseID = ""
bronzelakehouseID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mount_name = "/Horizon_mnt"

base_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{bronzelakehouseID}/"
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#spark.catalog.listTables()

#df = spark.catalog.listTables()
#table = spark.createDataFrame(df, )

#display(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)

print(local_path)

print(os.path.exists(local_path))
print(os.listdir(local_path + "/Tables/dbo"))

file_list = os.listdir(local_path + "/Tables/dbo")



mssparkutils.notebook.exit(file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
