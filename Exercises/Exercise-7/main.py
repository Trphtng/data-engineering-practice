from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    # Step 1: Đọc file CSV nén
    file_path = "data/hard-drive-2022-01-01-failures.csv.zip"
    df = spark.read.option("header", True).csv(f"zip://{file_path}")

    # Step 2: Thêm cột source_file
    df = df.withColumn("source_file", F.lit(file_path))

    # Step 3: Tách ngày từ source_file thành file_date
    df = df.withColumn(
        "file_date",
        F.to_date(
            F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1),
            "yyyy-MM-dd"
        )
    )

    # Step 4: Thêm cột brand
    df = df.withColumn(
        "brand",
        F.when(F.instr(F.col("model"), " ") > 0,
               F.split(F.col("model"), " ").getItem(0)
               ).otherwise(F.lit("unknown"))
    )

    # Step 5: Xếp hạng dung lượng theo model
    df = df.withColumn("capacity_bytes", F.col("capacity_bytes").cast("long"))
    capacity_window = Window.orderBy(F.desc("capacity_bytes"))
    capacity_ranked = (
        df.select("model", "capacity_bytes")
        .dropna(subset=["capacity_bytes"])
        .dropDuplicates(["model"])
        .withColumn("storage_ranking", F.dense_rank().over(capacity_window))
    )
    df = df.join(capacity_ranked.select("model", "storage_ranking"), on="model", how="left")

    # Step 6: Tạo primary_key từ tất cả các cột
    df = df.withColumn("primary_key", F.sha2(F.concat_ws("||", *df.columns), 256))

    # Hiển thị kết quả
    df.select("source_file", "file_date", "brand", "model", "capacity_bytes", "storage_ranking", "primary_key").show(10, truncate=False)

    # Ghi ra output nếu cần
    # df.write.mode("overwrite").parquet("output/exercise7_result")


if __name__ == "__main__":
    main()
