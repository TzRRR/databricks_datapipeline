from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(
    dataset1="dbfs:/FileStore/tr2bx_mini_project11/airline_safety.csv", 
):
    """
    Transform and load data into Delta Lake tables on Databricks.
    
    :param dataset1: Path to the first dataset in Databricks FileStore.
    :param dataset2: Path to the second dataset in Databricks FileStore.
    :return: Status of the load process.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("Transform and Load").getOrCreate()

    # Load the datasets as DataFrames with inferred schema
    df1 = spark.read.csv(dataset1, header=True, inferSchema=True)

    # Add unique identifiers to the DataFrames
    df1 = df1.withColumn("id", monotonically_increasing_id())

    # Write DataFrames to Delta tables
    df1.write.format("delta").mode("overwrite").saveAsTable("airline_safety_delta")

    # Count the number of rows in each table for validation
    df1_count = df1.count()

    print(f"airline_safety_delta table row count: {df1_count}")

    return "Finished transform and load"

if __name__ == "__main__":
    # Run the transform and load process
    load()
