from pyspark.sql import SparkSession

def query_airline_safety_data():
    """
    Perform a complex query on the `airline_safety_delta` table using PySpark SQL.
    
    Returns:
        DataFrame: Result of the query as a Spark DataFrame.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("Airline Safety Data Query").getOrCreate()

    # Read the Delta table
    airline_df = spark.table("airline_safety_delta")

    # Register the DataFrame as a temporary view for SQL querying
    airline_df.createOrReplaceTempView("airline_safety")

    # Define and execute the SQL query
    query = """
        SELECT 
            airline, 
            SUM(incidents_85_99) AS total_incidents_85_99,
            SUM(fatalities_85_99) AS total_fatalities_85_99,
            SUM(incidents_00_14) AS total_incidents_00_14,
            SUM(fatalities_00_14) AS total_fatalities_00_14,
            SUM(incidents_85_99 + incidents_00_14) AS total_incidents,
            SUM(fatalities_85_99 + fatalities_00_14) AS total_fatalities
        FROM 
            airline_safety
        GROUP BY 
            airline
        HAVING 
            total_incidents > 0
        ORDER BY 
            total_fatalities DESC, total_incidents DESC
    """
    result_df = spark.sql(query)

    print(result_df)
    return result_df

if __name__ == "__main__":
    # Execute the query
    result_df = query_airline_safety_data()