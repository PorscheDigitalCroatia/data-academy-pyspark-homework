import datetime

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType, TimestampType

from app.trips import ms_to_timestamp

TIMESTAMP_1 = datetime.datetime(2024, 2, 20, 22, 57, 45, 2000)
TIMESTAMP_2 = datetime.datetime(2023, 1, 10, 12, 47, 35, 1000)


def test_ms_to_timestamp(spark: SparkSession):
    # Given
    df = spark.createDataFrame(
        data=[
            (int(TIMESTAMP_1.timestamp() * 1000),),
            (int(TIMESTAMP_2.timestamp() * 1000),),
        ],
        schema=StructType(
            [
                StructField("unix_timestamp_ms", LongType()),
            ]
        ),
    )

    # When
    df_res = df.select(ms_to_timestamp("unix_timestamp_ms").alias("ts"))

    # Then
    df_expected = spark.createDataFrame(
        data=[
            (TIMESTAMP_1,),
            (TIMESTAMP_2,),
        ],
        schema=StructType(
            [
                StructField("ts", TimestampType()),
            ]
        ),
    )

    assert_df_equality(df_res, df_expected, ignore_row_order=True)
