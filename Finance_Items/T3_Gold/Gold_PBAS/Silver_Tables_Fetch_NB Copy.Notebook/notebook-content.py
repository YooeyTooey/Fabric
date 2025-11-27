# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

workspaceID = "z_WV_FIN_DE"
silverlakehouseID = "Silver_PBAS_Test.lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mount_name = "/PBAS_mnt"

base_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{silverlakehouseID}/"
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()

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

table_list = os.listdir(local_path + "/Tables/dbo")

mssparkutils.notebook.exit(table_list)

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
