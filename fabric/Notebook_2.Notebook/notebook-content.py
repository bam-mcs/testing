# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "72f7b7ed-666c-4cc3-a71d-1eb68a86252c",
# META       "default_lakehouse_name": "test_silver",
# META       "default_lakehouse_workspace_id": "0d47f26f-33f7-4e16-828e-d6bf3c6ad36e",
# META       "known_lakehouses": [
# META         {
# META           "id": "72f7b7ed-666c-4cc3-a71d-1eb68a86252c"
# META         },
# META         {
# META           "id": "ba924fe7-2ebc-40d7-8c21-35bf4c531c3b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Failed to load the workspace name of the lakehouse. You can try again later or manually fill in the workspace name of the lakehouse.
df = spark.sql("SELECT * FROM Silver.silver_nsw_current_weather.silver_nsw_current_weather LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('hello world!')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('2')

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

# CELL ********************

df = spark.sql("SELECT * FROM test_silver.dbo.silver_nsw_current_weather LIMIT 1000")
display(df)

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
