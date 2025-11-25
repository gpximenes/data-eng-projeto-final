import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ReportGenerator:
    """Encapsulates business rules for building the sales report."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_report(self, pedidos_df: DataFrame, pagamentos_df: DataFrame) -> DataFrame:
        try:
            self.logger.info("Starting report generation pipeline")

            pagamentos_legitimos = pagamentos_df.filter(
                (F.col("status") == F.lit(False))
                & (F.col("avaliacao_fraude.fraude") == F.lit(False))
            )
            self.logger.info(
                "Filtered payments to recusados e legitimos (status=false, fraude=false)"
            )

            pedidos_totais = (
                pedidos_df.withColumn(
                    "valor_total_item", F.col("valor_unitario") * F.col("quantidade")
                )
                .groupBy("id_pedido", "data_criacao", "uf")
                .agg(F.sum("valor_total_item").alias("valor_total_pedido"))
            )
            self.logger.info("Calculated order totals")

            pedidos_2025 = pedidos_totais.filter(F.year(F.col("data_criacao")) == 2025)
            self.logger.info("Filtered orders to year 2025")

            joined = pedidos_2025.join(
                pagamentos_legitimos.select("id_pedido", "forma_pagamento"),
                on="id_pedido",
                how="inner",
            )
            self.logger.info("Joined pedidos and pagamentos")

            report_df = (
                joined.select(
                    "id_pedido",
                    "uf",
                    "forma_pagamento",
                    F.col("valor_total_pedido").alias("valor_total"),
                    "data_criacao",
                ).orderBy("uf", "forma_pagamento", "data_criacao")
            )
            self.logger.info("Report dataframe assembled")

            return report_df
        except Exception as exc:  # pragma: no cover - defensive logging
            self.logger.error("Failed to generate report", exc_info=True)
            raise
