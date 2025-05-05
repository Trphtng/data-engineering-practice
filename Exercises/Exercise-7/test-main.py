import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("Exercise7Tests") \
        .getOrCreate()


def test_brand_extraction(spark):
    data = [("HGST HUH728080ALE600",), ("ST14000NM001G",)]
    df = spark.createDataFrame(data, ["model"])
    df = df.withColumn(
        "brand",
        F.when(F.instr(F.col("model"), " ") > 0,
               F.split(F.col("model"), " ").getItem(0)
               ).otherwise(F.lit("unknown"))
    )

    result = df.select("brand").rdd.flatMap(lambda x: x).collect()
    assert result == ["HGST", "unknown"]


def test_file_date_extraction(spark):
    data = [("data/hard-drive-2022-01-01-failures.csv.zip",)]
    df = spark.createDataFrame(data, ["source_file"])

    df = df.withColumn(
        "file_date",
        F.to_date(
            F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1),
            "yyyy-MM-dd"
        )
    )

    result = df.select("file_date").first()["file_date"]
    assert str(result) == "2022-01-01"


def test_storage_ranking(spark):
    data = [
        ("ModelA", 1000),
        ("ModelB", 3000),
        ("ModelC", 2000)
    ]
    df = spark.createDataFrame(data, ["model", "capacity_bytes"])

    window_spec = Window.orderBy(F.desc("capacity_bytes"))
    ranked_df = df.withColumn("storage_ranking", F.dense_rank().over(window_spec))

    result = ranked_df.orderBy("storage_ranking").select("model", "storage_ranking").collect()
    assert result[0]["model"] == "ModelB"
    assert result[0]["storage_ranking"] == 1


def test_primary_key(spark):
    data = [("2022-01-01", "XYZ123", "ModelX", 1000)]
    df = spark.createDataFrame(data, ["date", "serial_number", "model", "capacity_bytes"])
    df = df.withColumn("primary_key", F.sha2(F.concat_ws("||", *df.columns), 256))

    pk = df.select("primary_key").first()["primary_key"]
    assert pk is not None
    assert len(pk) == 64  # SHA256 in hex

