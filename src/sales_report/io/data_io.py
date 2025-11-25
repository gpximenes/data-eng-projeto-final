from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sales_report.config.settings import AppConfig
from sales_report.io.schemas import PAGAMENTOS_SCHEMA, PEDIDOS_SCHEMA


class DataIO:
    """Handles reading and writing of datasets with explicit schemas."""

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config

    def read_pagamentos(self) -> DataFrame:
        return self.spark.read.schema(PAGAMENTOS_SCHEMA).json(
            self.config.pagamentos_path
        )

    def read_pedidos(self) -> DataFrame:
        return (
            self.spark.read.schema(PEDIDOS_SCHEMA)
            .option("header", True)
            .option("delimiter", ";")
            .csv(self.config.pedidos_path)
        )

    def write_report(self, df: DataFrame) -> None:
        output_path = Path(self.config.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(output_path))
