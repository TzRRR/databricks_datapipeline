# **Databricks Data Pipeline**

[![CI](https://github.com/TzRRR/databricks_datapipeline/actions/workflows/cicd.yml/badge.svg)](https://github.com/TzRRR/databricks_datapipeline/actions/workflows/cicd.yml)

## **Project Overview**

This project implements an Extract, Transform, and Load (ETL) pipeline using Databricks and PySpark. The pipeline processes airline safety data and uploads it to Databricks FileStore, transforms it into Delta tables, and performs queries for analysis.

The project is structured to handle large-scale data processing efficiently, leveraging Databricks for distributed data processing and analysis.

---

## **Features**

1. **Data Extraction**:

   - Downloads CSV files from external sources.
   - Uploads the files to Databricks FileStore for further processing.

2. **Data Transformation & Loading**:

   - Reads the raw data from Databricks FileStore.
   - Cleans and transforms the data using PySpark.
   - Stores the data as Delta tables in Databricks.

3. **Data Querying**:

   - Performs SQL-based analytics on the Delta tables.
   - Aggregates metrics such as total incidents and fatalities.

4. **Testing**:
   - Unit tests are implemented using `pytest` for ETL steps.

---
