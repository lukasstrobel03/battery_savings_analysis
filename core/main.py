from pathlib import Path
from pyspark.sql import SparkSession

data_path = Path(__file__).parents[1] / "data"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SPBD Project Analysis") \
        .getOrCreate()

    readings = spark.read.parquet(*[str(f) for f in data_path.rglob("*.parquet")])

    readings.printSchema()
    readings.show(5, truncate=False)