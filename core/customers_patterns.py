from pathlib import Path
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).parents[1]
OUT = ROOT / "output"


def add_columns(e):
    hour = F.hour("ts") + F.minute("ts") / 60

    is_cheap = (hour >= 22) | (hour < 8)
    is_expensive = ((hour >= 10.5) & (hour < 13)) | ((hour >= 19.5) & (hour < 21))

    period = F.when(is_cheap, "Cheap").when(is_expensive, "Expensive").otherwise("Medium")

    return (e.withColumn("period", period)
             .withColumn("date", F.to_date("ts"))
             .withColumn("month", F.month("ts")))


def customer_features(e):
    kwh, month, period = F.col("kwh"), F.col("month"), F.col("period")
    share = lambda p: F.round(F.sum(F.when(period == p, kwh)) / F.sum(kwh) * 100, 1)
    winter = F.avg(F.when(month.isin(12, 1, 2), kwh))
    spring = F.avg(F.when(month.isin(3, 4, 5), kwh))
    summer = F.avg(F.when(month.isin(6, 7, 8), kwh))
    autumn = F.avg(F.when(month.isin(9, 10, 11), kwh))

    slots = e.groupBy("Meter").agg(
        F.round(F.sum(kwh)).alias("kwh_year"),
        share("Cheap").alias("Cheap_pct"),
        share("Medium").alias("Medium_pct"),
        share("Expensive").alias("Expensive_pct"),
        F.round(winter * 96, 2).alias("Winter_kwh_Day"),
        F.round(spring * 96, 2).alias("Spring_kwh_Day"),
        F.round(summer * 96, 2).alias("Summer_kwh_Day"),
        F.round(autumn * 96, 2).alias("Autumn_kwh_Day"),
        F.round(F.max(kwh) / F.avg(kwh), 1).alias("Peak_to_Average"),
        F.round(F.max(kwh) * 4, 2).alias("Max_kw"))

    day = e.groupBy("Meter", "date").agg(
        F.sum(kwh).alias("total"),
        F.sum(F.when(period != "Cheap", kwh)).alias("not_cheap"))

    days = day.groupBy("Meter").agg(
        F.round(F.avg("total"), 2).alias("kwh_day"),
        F.round(F.avg("not_cheap"), 2).alias("Not_Cheap_kwh_Day"))

    return slots.join(days, "Meter").orderBy("Meter")


def add_groups(f):
    off, peak, kwh = F.col("Cheap_pct"), F.col("Expensive_pct"), F.col("kwh_year")
    timing = F.when(off >= 45, "Night-Heavy").when(peak >= 24, "Peak-Heavy").otherwise("Mixed")
    size = F.when(kwh >= 3000, "High").when(kwh < 2000, "Low").otherwise("Medium")
    shape = F.when(F.col("Peak_to_Average") >= 18, "Peaky").otherwise("Flat")
    return (f.withColumn("Timing_Group", timing)
             .withColumn("Size_Group", size)
             .withColumn("Shape_Group", shape))


def plot_daily_profiles(e, path):
    prof = (e.withColumn("hour", F.hour("ts") + F.minute("ts") / 60)
             .groupBy("Meter", "hour").agg((F.avg("kwh") * 4).alias("kw"))
             .orderBy("Meter", "hour").toPandas())

    fig, axes = plt.subplots(3, 4, figsize=(14, 8), sharex=True, sharey=True)
    for ax, (meter, d) in zip(axes.flat, prof.groupby("Meter")):
        ax.axvspan(10.5, 13, color="lightgrey")
        ax.axvspan(19.5, 21, color="lightgrey")
        ax.plot(d.hour, d.kw)
        ax.set_title(meter)
        ax.set_xticks(range(0, 25, 3))
        ax.tick_params(labelbottom=True, labelleft=True)
    axes.flat[-1].axis("off")

    fig.supxlabel("Time of Day")
    fig.supylabel("Electricity Used (kW)")
    fig.suptitle("Average Daily Use of Electricity Per House (grey = peak hours)")
    fig.tight_layout()
    fig.savefig(path)
    print("Saved", path)


if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    energy = add_columns(spark.read.parquet(str(OUT / "energy_15min")))

    features = add_groups(customer_features(energy))
    features.show()
    features.toPandas().to_csv(OUT / "Customers_Patterns.csv", index=False)

    plot_daily_profiles(energy, OUT / "Customers_Daily_Patterns.png")