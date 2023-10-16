import os

import pytest
from pyspark.sql import SparkSession

os.environ["TZ"] = "UTC"


def create_testing_spark_session():
    spark = (
        SparkSession.builder.appName("RunningTests")
        .master("local")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def spark():
    return create_testing_spark_session()
