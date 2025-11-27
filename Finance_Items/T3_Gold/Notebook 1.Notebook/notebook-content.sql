-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "eb337e93-2630-42a8-b860-ff9834be952d",
-- META       "default_lakehouse_name": "Gold_Finance_LH",
-- META       "default_lakehouse_workspace_id": "5a5a7ce9-c78d-4181-9489-87935e86fb7d",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "d8662dd1-f8a5-4ecc-8fc8-b2475b3b498e"
-- META         },
-- META         {
-- META           "id": "eb337e93-2630-42a8-b860-ff9834be952d"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create materialized lake views 
-- 1. Use this notebook to create materialized lake views. 
-- 2. Select **Run all** to run the notebook. 
-- 3. When the notebook run is completed, return to your lakehouse and refresh your materialized lake views graph. 


-- CELL ********************

-- Welcome to your new notebook 
-- Type here in the cell editor to add code! 
-- CREATE MATERIALIZED LAKE VIEW <mlv_name> AS select_statement

   CREATE SCHEMA IF NOT EXISTS GOLD;

   CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.ApprovedCombyProject AS
   SELECT
       funding_office_code,
       SUM(approved_commitment) AS approvedcom,
     
   FROM
     dbo.silver_pbas_test
   GROUP BY
       funding_office_code;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
