import pyspark.sql.functions as F

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame

data_path = Path(__file__).parents[1] / "data"
def load_dataset() -> DataFrame:
    """Load all the parquet files."""

    spark = SparkSession.builder \
        .appName("SPBD Project Analysis") \
        .getOrCreate()
    
    readings = spark.read \
        .parquet(str(data_path)) \

    def __split_date_column(readings: DataFrame) -> DataFrame:  
        readings = readings \
            .withColumn("year", F.year("Date")) \
            .withColumn("month", F.month("Date")) \
            .withColumn("day", F.day("Date")) \
            .withColumn("hour", F.hour("Date")) \
            .withColumn("minute_of_day", F.hour("Date") * 60 + F.minute("Date"))
        return readings

    def __add_tariff_periods(readings: DataFrame) -> DataFrame:
        """Add the tariff periods."""
        min_of_day = F.col("minute_of_day")

        # 10 * 60 + 30 correlates to 10:30,
        # 8 * 60 to 8:00
        # 22 * 60 to 22:00
        readings = readings \
            .withColumn(
                "trio_period", 
                F.when((min_of_day >= 22 * 60) | (min_of_day < 8 * 60), "off-peak")
                .when(((min_of_day >= 10 * 60 + 30) & (min_of_day < 13 * 60)) | ((min_of_day >= 19 * 60 + 30) & (min_of_day < 22 * 60)), "peak")
                .otherwise("mid-peak")
            ) \
            .withColumn(
                "trio_price",
                F.when(F.col("trio_period") == "off-peak", 0.1087)
                .when(F.col("trio_period") == "mid-peak", 0.1690)
                .otherwise(0.2495)
            ) \
            .withColumn(
                "bi_period",
                F.when((min_of_day >= 22 * 60) | (min_of_day <= 8 * 60), "off-peak")
                .otherwise("standard_hours")
            ) \
            .withColumn(
                "bi_price",
                F.when(F.col("bi_period") == "off-peak", 0.1087)
                .otherwise(0.1988)
            ) \
            .withColumn("standard_period", F.lit("standard")) \
            .withColumn("standard_price", F.lit(0.1654))
        return readings

    readings = __add_tariff_periods(__split_date_column(readings))
    return readings
    
def calculate_baseline_prices(readings: DataFrame) -> DataFrame:
    """Calculate the annual cost for all electricity plans."""
    return NotImplemented


if __name__ == "__main__":

    readings = load_dataset()

    readings.printSchema()
    readings.show(5, truncate=False)
