import pyspark.sql.functions as F

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, Window

data_path = Path(__file__).parents[1] / "data"

def load_dataset() -> DataFrame:
    """Load all the parquet files."""

    spark = SparkSession.builder \
        .appName("SPBD Project Analysis") \
        .getOrCreate()
    
    readings = spark.read \
        .parquet(str(data_path))


    def _calculcate_reading_intervals() -> DataFrame:
        """Extract the kw that changed after every reading."""
        INTERVAL = 900 # 15 Minute interval

        raw = readings.select(
            "Meter",
            F.col("Date").cast("timestamp").cast("long").alias("time"),
            F.col("Reading").cast("double").alias("reading"),
        ) \
        .dropDuplicates(["Meter", "time"])

        window = Window.partitionBy("Meter").orderBy("time")
        segments = raw \
            .withColumn("t1", F.lead("time").over(window)) \
            .withColumn("r1", F.lead("reading").over(window)) \
            .filter(F.col("t1").isNotNull())

        first = F.ceil(F.col("time") / INTERVAL) * INTERVAL
        last = (F.ceil(F.col("t1") / INTERVAL) - 1) * INTERVAL

        grid = segments \
            .withColumn("first", first.cast("long")) \
            .withColumn("last", last.cast("long")) \
            .filter(F.col("first") <= F.col("last")) \
            .withColumn("g", F.explode(F.sequence("first", "last", F.lit(INTERVAL)))) \
            .withColumn(
                "counter",
                F.col("reading") + (F.col("r1") - F.col("reading")) * (F.col("g") - F.col("time")) / (F.col("t1") - F.col("time")),
            ) \
            .select("Meter", "g", "counter")

        wg = Window.partitionBy("Meter").orderBy("g")

        intervals = grid \
            .withColumn("next_g", F.lead("g").over(wg)) \
            .withColumn("next_counter", F.lead("counter").over(wg)) \
            .filter(F.col("next_g") - F.col("g") == INTERVAL) \
            .withColumn("kwh", F.col("next_counter") - F.col("counter")) \
            .withColumn("interval_start", F.timestamp_seconds("g")) \
            .select("Meter", "interval_start", "kwh")
        
        return intervals


    readings = _calculcate_reading_intervals()

    def _add_minutes_of_day() -> DataFrame:  
        return readings \
            .withColumn(
                "minute_of_day", 
                F.hour("interval_start") * 60 + F.minute("interval_start")
            )

    def _add_tariff_periods(readings: DataFrame) -> DataFrame:
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
    
    return _add_tariff_periods(_add_minutes_of_day())

    
def calculate_baseline_prices(readings: DataFrame) -> DataFrame:
    """Calculate the annual cost for all electricity plans."""
    readings = readings \
        .withColumn("trio_price", F.col("kwh") * F.col("trio_price")) \
        .withColumn("bi_price", F.col("kwh") * F.col("bi_price")) \
        .withColumn("standard_price", F.col("kwh") * F.col("standard_price")) \
        .select("Meter", "trio_price", "bi_price", "standard_price")

    readings = readings \
        .groupBy("Meter") \
        .agg(
            F.round(F.sum("trio_price"), 2).alias("total_trio_price"),
            F.round(F.sum("bi_price"), 2).alias("total_bi_price"),
            F.round(F.sum("standard_price"), 2).alias("total_standard_price"),
        ) \
        .sort("Meter")

    return readings \
        .withColumn("change_to_bi", F.round(F.col("total_bi_price") - F.col("total_trio_price"), 2)) \
        .withColumn("change_to_standard", F.round(F.col("total_standard_price") - F.col("total_trio_price"), 2)) \


def characterize_consumption_patterns(readings: DataFrame) -> DataFrame:
    """Characterize the specific consumption patterns of each customer."""

    return NotImplemented

if __name__ == "__main__":

    readings = load_dataset()
    calc_readings = calculate_baseline_prices(readings)

    # readings.printSchema()
    #calc_readings.printSchema()
    # readings.show(5, truncate=False)

    # IMPORTANT FOR REPORT: all measurements are for the first part of objective 1
    calc_readings.show(truncate=False)
