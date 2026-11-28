import matplotlib.pyplot as plt
import pyspark.sql.functions as F

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, Window

data_path = Path(__file__).parents[1] / "data"


def load_dataset() -> DataFrame:
    """Load all the parquet files."""

    spark = SparkSession.builder \
        .appName("SPBD Project Analysis") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    readings = spark.read \
        .parquet(str(data_path))
    readings = _calculcate_reading_intervals(readings)
    return _add_tariff_periods(_add_minutes_of_day(readings))


def _calculcate_reading_intervals(readings: DataFrame) -> DataFrame:
    """Extract the kw that changed after every reading."""
    INTERVAL = 900 # 15 Minute interval in seconds

    w_cum = Window.partitionBy("Meter") \
        .orderBy("time") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    raw = readings.select(
        "Meter",
        F.col("Date").cast("timestamp").cast("long").alias("time"),
        F.col("Reading").cast("double").alias("reading"),
    ) \
    .dropna() \
    .dropDuplicates(["Meter", "time"]) \
    .withColumn("mx", F.max("reading").over(w_cum)) \
    .filter(F.col("reading") >= F.col("mx")) \
    .drop("mx")

    # before = readings.count()
    # after = raw.count()
    # print(before - after)

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


def _add_minutes_of_day(readings: DataFrame) -> DataFrame:  
    return readings \
        .withColumn(
            "minute_of_day", 
            F.hour("interval_start") * 60 + F.minute("interval_start")
        )


def _add_tariff_periods(readings: DataFrame) -> DataFrame:
    """Add the tariff periods."""
    min_of_day = F.col("minute_of_day")

    # 10 * 60 + 30 correlates to 10:30,
    # 8 * 60 to 8:00 and so on
    readings = readings \
        .withColumn(
            "trio_period",
            F.when((min_of_day >= 22 * 60) | (min_of_day < 8 * 60), "off-peak")
            .when(((min_of_day >= 10 * 60 + 30) & (min_of_day < 13 * 60)) | ((min_of_day >= 19 * 60 + 30) & (min_of_day < 21 * 60)), "peak")
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
            F.when((min_of_day >= 22 * 60) | (min_of_day < 8 * 60), "off-peak")
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
    e = readings \
        .withColumn("date", F.to_date("interval_start")) \
        .withColumn("month", F.month("interval_start"))

    kwh, month, period = F.col("kwh"), F.col("month"), F.col("trio_period")
    
    share = lambda p: F.round(F.sum(F.when(period == p, kwh)) / F.sum(kwh) * 100, 1)
    season = lambda months: F.avg(F.when(month.isin(*months), kwh))

    slots = e.groupBy("Meter").agg(
        F.round(F.sum(kwh)).alias("kwh_year"),
        share("off-peak").alias("Offpeak_pct"),
        share("mid-peak").alias("Midpeak_pct"),
        share("peak").alias("Peak_pct"),
        F.round(season([12, 1, 2]) * 96, 2).alias("Winter_kwh_day"),
        F.round(season([3, 4, 5]) * 96, 2).alias("Spring_kwh_day"),
        F.round(season([6, 7, 8]) * 96, 2).alias("Summer_kwh_day"),
        F.round(season([9, 10, 11]) * 96, 2).alias("Autumn_kwh_day"),
        F.round(F.max(kwh) / F.avg(kwh), 1).alias("Peak_to_Average"),
        F.round(F.max(kwh) * 4, 2).alias("Max_kw"),
    )

    day = e.groupBy("Meter", "date").agg(
        F.sum(kwh).alias("total"),
        F.sum(F.when(period != "off-peak", kwh)).alias("not_offpeak"),
    )

    days = day.groupBy("Meter").agg(
        F.round(F.avg("total"), 2).alias("kwh_day"),
        F.round(F.avg("not_offpeak"), 2).alias("Not_offpeak_kwh_day"),
    )

    return _add_groups(slots.join(days, "Meter")).orderBy("Meter")


def _add_groups(features: DataFrame) -> DataFrame:
    off, peak, kwh = F.col("Offpeak_pct"), F.col("Peak_pct"), F.col("kwh_year")

    timing = F.when(off >= 45, "Night-Heavy") \
        .when(peak >= 24, "Peak-Heavy") \
        .otherwise("Mixed")
    size = F.when(kwh >= 3000, "High") \
        .when(kwh < 2000, "Low") \
        .otherwise("Medium")
    shape = F.when(F.col("Peak_to_Average") >= 18, "Peaky").otherwise("Flat")

    return features \
        .withColumn("Timing_Group", timing) \
        .withColumn("Size_Group", size) \
        .withColumn("Shape_Group", shape)


def plot_daily_profiles(readings: DataFrame, path: Path) -> None:
    """Plot the average daily load curve (kW) for every meter."""
    prof = readings \
        .groupBy("Meter", "minute_of_day") \
        .agg((F.avg("kwh") * 4).alias("kw")) \
        .orderBy("Meter", "minute_of_day") \
        .toPandas()
    prof["hour"] = prof["minute_of_day"] / 60

    meters = sorted(prof["Meter"].unique())
    cols = 4
    rows = -(-len(meters) // cols)

    fig, axes = plt.subplots(rows, cols, figsize=(14, 8), sharex=True, sharey=True)
    for ax, (meter, d) in zip(axes.flat, prof.groupby("Meter")):
        ax.axvspan(10.5, 13, color="lightgrey")
        ax.axvspan(19.5, 21, color="lightgrey")
        ax.plot(d.hour, d.kw)
        ax.set_title(meter)
        ax.set_xticks(range(0, 25, 3))
        ax.tick_params(labelbottom=True, labelleft=True)
    for ax in list(axes.flat)[len(meters):]:
        ax.axis("off")

    fig.supxlabel("Time of Day")
    fig.supylabel("Electricity Used (kW)")
    fig.suptitle("Average Daily Use of Electricity Per House (grey = peak hours)")
    fig.tight_layout()
    fig.savefig(path)
    print("Saved", path)


if __name__ == "__main__":
    import os 
    if not os.path.exists(Path(__file__).parents[1] / "output/"):
        os.mkdir("output")

    readings = load_dataset()
    calc_readings = calculate_baseline_prices(readings)
    calc_readings.show(10, truncate=False)

    output_path = Path(__file__).parents[1] / "output"
    output_path.mkdir(exist_ok=True)

    features = characterize_consumption_patterns(readings)
    features.show(10, truncate=False)
    features.toPandas().to_csv(output_path / "Customers_Patterns.csv", index=False)

    plot_daily_profiles(readings, output_path / "Customers_Daily_Patterns.png")