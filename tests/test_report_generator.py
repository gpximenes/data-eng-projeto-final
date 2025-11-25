from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession

from sales_report.business.report_generator import ReportGenerator
from sales_report.io.schemas import PAGAMENTOS_SCHEMA, PEDIDOS_SCHEMA


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = (
        SparkSession.builder.master("local[*]").appName("sales-report-tests").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_build_report_filters_and_aggregates(spark: SparkSession):
    pedidos_data = [
        (
            "pedido-1",
            "PRODUTO-A",
            100.0,
            2,
            datetime(2025, 1, 10, 10, 0, 0),
            "SP",
            1,
        ),
        (
            "pedido-1",
            "PRODUTO-B",
            50.0,
            1,
            datetime(2025, 1, 10, 10, 0, 0),
            "SP",
            1,
        ),
        (
            "pedido-2",
            "PRODUTO-C",
            75.0,
            1,
            datetime(2025, 2, 5, 12, 0, 0),
            "RJ",
            2,
        ),
        (
            "pedido-3",
            "PRODUTO-D",
            80.0,
            1,
            datetime(2024, 12, 31, 23, 0, 0),
            "MG",
            3,
        ),
    ]
    pedidos_df = spark.createDataFrame(pedidos_data, schema=PEDIDOS_SCHEMA)

    pagamentos_data = [
        Row(
            id_pedido="pedido-1",
            forma_pagamento="PIX",
            valor_pagamento=250.0,
            status=False,
            data_processamento=datetime(2025, 1, 11, 12, 0, 0),
            avaliacao_fraude=Row(fraude=False, score=0.1),
        ),
        Row(
            id_pedido="pedido-2",
            forma_pagamento="CARTAO_CREDITO",
            valor_pagamento=75.0,
            status=True,
            data_processamento=datetime(2025, 2, 6, 12, 0, 0),
            avaliacao_fraude=Row(fraude=False, score=0.2),
        ),
        Row(
            id_pedido="pedido-3",
            forma_pagamento="BOLETO",
            valor_pagamento=80.0,
            status=False,
            data_processamento=datetime(2024, 12, 31, 23, 0, 0),
            avaliacao_fraude=Row(fraude=False, score=0.3),
        ),
    ]
    pagamentos_df = spark.createDataFrame(pagamentos_data, schema=PAGAMENTOS_SCHEMA)

    report_df = ReportGenerator().build_report(pedidos_df, pagamentos_df)
    rows = report_df.collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.id_pedido == "pedido-1"
    assert row.uf == "SP"
    assert row.forma_pagamento == "PIX"
    assert pytest.approx(row.valor_total, rel=1e-6) == 250.0
    assert row.data_criacao.year == 2025
