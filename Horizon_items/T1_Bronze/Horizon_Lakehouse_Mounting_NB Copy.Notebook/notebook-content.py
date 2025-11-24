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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Library Imports

# CELL ********************

from notebookutils import mssparkutils
import os
import pandas as pd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pipeline Parameters
# 
# - Variables coming from the notebook's pipeline
# - empty placeholders in this specific cell, takes on the value of any variable passed on to this notebook.

# PARAMETERS CELL ********************

workspaceID = "z_WV_ITD_DE"
lakehouseID = "Bronze_Horizon_Test.lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Mount Path

# CELL ********************

mount_name = "/Horizon_mnt"

base_path = f"abfss://{workspaceID}@onelake.dfs.fabric.microsoft.com/{lakehouseID}/"
mssparkutils.fs.mount(base_path, mount_name)
mount_points = mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # File List Retrieval
# 
# - used the mount path to utilize the os library of python to access whatever we need
# - the mount path + the relative file path (/Files/Input) to point to the Input folder where csv files are stored
# - Passes on the list of file names using the notebook.exit() function of mssparkutils

# CELL ********************



local_path = next((mp["localPath"] for mp in mount_points if mp["mountPoint"] == mount_name), None)

print(local_path)

print(os.path.exists(local_path))
print(os.listdir(local_path + "/Files/Input"))

file_list = os.listdir(local_path + "/Files/Input")

mssparkutils.notebook.exit(file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
