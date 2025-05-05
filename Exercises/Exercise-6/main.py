import zipfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, count, desc, expr, row_number
from pyspark.sql.window import Window
from datetime import datetime, timedelta

import os

DATA_DIR = "data"

if not os.path.exists(DATA_DIR):
    print(f"❌ Folder '{DATA_DIR}' not found in: {os.getcwd()}")
    exit(1)
DATA_DIR = "data"
REPORTS_DIR = "reports"


def extract_zip_files(data_dir):
    for file in os.listdir(data_dir):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(data_dir, file), 'r') as zip_ref:
                zip_ref.extractall(data_dir)


def read_data(spark, data_dir):
    csv_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]
    return spark.read.option("header", True).csv(csv_files, inferSchema=True)


def avg_trip_duration_per_day(df):
    return df.withColumn("date", to_date("start_time")) \
             .groupBy("date") \
             .agg(avg("tripduration").alias("avg_trip_duration"))


def trip_count_per_day(df):
    return df.withColumn("date", to_date("start_time")) \
             .groupBy("date") \
             .agg(count("*").alias("trip_count"))


def most_popular_start_station_per_month(df):
    df = df.withColumn("month", expr("date_format(to_date(start_time), 'yyyy-MM')"))
    window = Window.partitionBy("month").orderBy(desc("count"))
    return df.groupBy("month", "from_station_name") \
             .agg(count("*").alias("count")) \
             .withColumn("rank", row_number().over(window)) \
             .filter(col("rank") == 1) \
             .drop("rank")


def top_3_stations_last_2_weeks(df):
    df = df.withColumn("date", to_date("start_time"))
    max_date = df.agg({"date": "max"}).collect()[0][0]
    two_weeks_ago = (datetime.strptime(max_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y-%m-%d")
    df = df.filter(col("date") >= two_weeks_ago)
    window = Window.partitionBy("date").orderBy(desc("count"))
    return df.groupBy("date", "from_station_name") \
             .agg(count("*").alias("count")) \
             .withColumn("rank", row_number().over(window)) \
             .filter(col("rank") <= 3)


def avg_trip_duration_by_gender(df):
    return df.groupBy("gender").agg(avg("tripduration").alias("avg_trip_duration"))


def top_10_ages_by_trip_duration(df):
    df = df.withColumn("age", 2025 - col("birthyear"))
    longest = df.orderBy(desc("tripduration")).select("age", "tripduration").distinct().limit(10)
    shortest = df.orderBy("tripduration").select("age", "tripduration").distinct().limit(10)
    return longest, shortest


def save_report(df, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{REPORTS_DIR}/{name}")


def main():
    spark = SparkSession.builder.appName("Exercise6").getOrCreate()
    os.makedirs(REPORTS_DIR, exist_ok=True)

    extract_zip_files(DATA_DIR)
    df = read_data(spark, DATA_DIR)

    save_report(avg_trip_duration_per_day(df), "avg_trip_duration_per_day")
    save_report(trip_count_per_day(df), "trip_count_per_day")
    save_report(most_popular_start_station_per_month(df), "popular_start_station_per_month")
    save_report(top_3_stations_last_2_weeks(df), "top_3_start_stations_last_2_weeks")
    save_report(avg_trip_duration_by_gender(df), "avg_trip_duration_by_gender")

    longest, shortest = top_10_ages_by_trip_duration(df)
    save_report(longest, "top_10_ages_longest_trips")
    save_report(shortest, "top_10_ages_shortest_trips")

    spark.stop()


if __name__ == "__main__":
    main()
