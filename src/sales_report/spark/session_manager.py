import os
import sys

from pyspark.sql import SparkSession


class SparkSessionManager:
    """Encapsulates Spark session creation."""

    def __init__(self, app_name: str = "SalesReportApp"):
        self.app_name = app_name

    def get_session(self) -> SparkSession:
        # Ensure PySpark uses the same Python executable and avoid Windows native IO issues.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        builder = SparkSession.builder.appName(self.app_name).master("local[*]")
        builder = builder.config(
            "spark.hadoop.io.native.lib.available", "false"
        ).config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        return builder.getOrCreate()
