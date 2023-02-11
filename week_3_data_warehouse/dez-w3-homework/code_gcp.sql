{
 "cells": [
  {
   "attachments": {},
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "-- Question 1\n",
    "CREATE OR REPLACE EXTERNAL TABLE `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework`\n",
    "OPTIONS (\n",
    "  format = 'CSV',\n",
    "  uris = ['gs://prefect-de-zoomcamp-hfelipini/data/fhv/fhv_tripdata_2019-*.csv']\n",
    ");\n",
    "\n",
    "SELECT COUNT(*) FROM `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework`;\n",
    "\n",
    "-- Question 2\n",
    "SELECT DISTINCT(Affiliated_base_number) FROM `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework`;\n",
    "\n",
    "SELECT DISTINCT(Affiliated_base_number) FROM `sacred-pursuit-376214.dezoomcamp.fhv_tripdata_w3_homework`;\n",
    "\n",
    "-- Question 3\n",
    "SELECT COUNT(*) FROM `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework` WHERE DOlocationID IS NULL AND PUlocationID IS NULL;\n",
    "\n",
    "-- Question 4\n",
    "-- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?\n",
    "-- Answer: Partition by pickup_datetime Cluster on affiliated_base_number\n",
    "\n",
    "-- Question 5\n",
    "-- Create a partitioned table from external table\n",
    "CREATE OR REPLACE TABLE `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework_partitioned`\n",
    "PARTITION BY DATE(pickup_datetime)\n",
    "CLUSTER BY affiliated_base_number AS\n",
    "SELECT * FROM `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework`;\n",
    "\n",
    "SELECT distinct(affiliated_base_number)\n",
    "FROM `sacred-pursuit-376214.dezoomcamp.external_fhv_tripdata_w3_homework_partitioned`\n",
    "WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';\n",
    "\n",
    "SELECT distinct(affiliated_base_number)\n",
    "FROM `sacred-pursuit-376214.dezoomcamp.fhv_tripdata_w3_homework`\n",
    "WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';"
   ]
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
