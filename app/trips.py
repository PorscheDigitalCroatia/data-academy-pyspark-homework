import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from pyspark.sql import Column, DataFrame, DataFrameWriter, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, TimestampType

USER_BUCKET = "s3a://ai-academy-marko-novina"
USER_DB = "db_markon"

ROOT_DIR = Path(__file__).parents[1]
HIVE_DIR = ROOT_DIR / "hive"
SPARK_WAREHOUSE_DIR = HIVE_DIR / "spark-warehouse"

TELEMETRY_DATA_PATH = ROOT_DIR / "sample_data/telemetry-data.parquet"


@dataclass
class HiveTable:
    name: str
    database: Optional[str] = None
    path: Optional[str] = None

    @property
    def full_table_name(self):
        return self.name if not self.database else f"{self.database}.{self.name}"

    def __str__(self) -> str:
        return self.full_table_name

    def __repr__(self) -> str:
        return str(self)

    def saveAsTable(self, writer: DataFrameWriter):
        if self.path:
            writer = writer.option("path", self.path)
        writer.saveAsTable(self.full_table_name)


def get_spark_session(dev: bool) -> SparkSession:
    builder = SparkSession.builder.appName("calculate-trips")
    if dev:
        builder = (
            builder.master("local")
            .config("spark.executor.cores", 4)
            .config("spark.executor.memory", "2g")
            .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dderby.system.home='{HIVE_DIR}'",
            )
            .enableHiveSupport()
        )
    else:
        builder = (
            builder.config("spark.executor.cores", 2)
            .config("spark.executor.memory", "2g")
            .config(
                "spark.kerberos.access.hadoopFileSystems",
                f"s3a://cdp-ai-academy,{USER_BUCKET}",
            )
            .config("spark.executor.cores", 3)
            .config("spark.executor.memory", "8g")
            .config("spark.dynamicAllocation.initialExecutors", 2)
            .config("spark.dynamicAllocation.minExecutors", 2)
            .config("spark.dynamicAllocation.maxExecutors", 10)
        )
    return builder.getOrCreate()


def get_raw_data(spark: SparkSession, dev: bool) -> DataFrame:
    if dev:
        return spark.read.parquet(str(TELEMETRY_DATA_PATH))
    return spark.table("default.telemetry_data")


def get_destination_table(dev: bool) -> HiveTable:
    if dev:
        table_path = str(SPARK_WAREHOUSE_DIR / "trips")
        return HiveTable(name="trips", path=table_path)
    return HiveTable(name="trips", database=USER_DB, path=f"{USER_BUCKET}/trips")


def ms_to_timestamp(col_name: str) -> Column:
    return (F.col(col_name) / 1000).cast(TimestampType())


def ts_to_sec(col: Union[Column, str]) -> Column:
    if isinstance(col, str):
        return F.col(col).cast(LongType())
    return col.cast(LongType())


def get_signal_data(df: DataFrame, signal_name: str) -> DataFrame:
    return df.filter(F.col("signaldata_signalname") == signal_name)


def get_rows_implying_trip(df: DataFrame) -> DataFrame:
    # combine two signals which are collected with all vehicle types
    df_speed = get_signal_data(df, "KBI_angez_Geschw_K2CAN")
    df_speed = df_speed.filter(F.col("signaldata_interpretedvalue_asdoublevalue") > 0)

    df_acc = get_signal_data(df, "SARA_Accel_X_r")
    df_acc = df_acc.filter(F.col("signaldata_interpretedvalue_asdoublevalue") != 0)

    return df_speed.union(df_acc)


def calculate_trips(df: DataFrame) -> DataFrame:
    df = get_rows_implying_trip(df)

    # convert timestamp to appropriate type
    df = df.withColumn("signal_ts", ms_to_timestamp("signaldata_timeofoccurrence"))

    window_spec = Window.partitionBy("metadata_pvin").orderBy("signal_ts")
    next_ts = F.lead("signal_ts").over(window_spec)
    # far enough in the future to detect that row as trip end
    default_next_ts = F.col("signal_ts") + F.expr("INTERVAL 1 DAY")

    df = df.select(
        "metadata_pvin",
        "signal_ts",
        F.first("signal_ts").over(window_spec).alias("first_pvin_ts"),
        F.coalesce(next_ts, default_next_ts).alias("next_ts"),
    )

    # remove all subsequent values within threshold
    next_ts_sec = ts_to_sec(F.coalesce("next_ts", "signal_ts"))
    current_ts_sec = ts_to_sec("signal_ts")
    df = df.filter((next_ts_sec - current_ts_sec) >= (20.0 * 60))

    # define trips start and end
    df = df.select(
        "metadata_pvin",
        F.md5(F.concat("metadata_pvin", F.col("signal_ts").cast(StringType()))).alias(
            "trip_id"
        ),
        F.coalesce(F.lag("next_ts").over(window_spec), "first_pvin_ts").alias(
            "trip_start_ts"
        ),
        F.col("signal_ts").alias("trip_end_ts"),
    )

    # filter out too short "trips"
    duration_sec = ts_to_sec("trip_end_ts") - ts_to_sec("trip_start_ts")
    df = df.filter(duration_sec > (5.0 * 60))

    return df


def main(dev: bool):
    spark = get_spark_session(dev)
    dst_table = get_destination_table(dev)
    df_raw = get_raw_data(spark, dev)

    df_trips = calculate_trips(df_raw)

    writer = df_trips.write.mode("overwrite").format("parquet")
    dst_table.saveAsTable(writer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dev", action="store_true", help="Run in development mode (locally)"
    )
    args = parser.parse_args()
    main(args.dev)
