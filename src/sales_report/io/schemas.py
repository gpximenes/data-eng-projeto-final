from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PAGAMENTOS_SCHEMA = StructType(
    [
        StructField("id_pedido", StringType(), False),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", DoubleType(), True),
        StructField("status", BooleanType(), True),
        StructField("data_processamento", TimestampType(), True),
        StructField(
            "avaliacao_fraude",
            StructType(
                [
                    StructField("fraude", BooleanType(), True),
                    StructField("score", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)

PEDIDOS_SCHEMA = StructType(
    [
        StructField("id_pedido", StringType(), False),
        StructField("produto", StringType(), True),
        StructField("valor_unitario", DoubleType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
    ]
)
