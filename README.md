# Relatório de pedidos 2025 (PySpark)

Projeto em PySpark para gerar um relatório de pedidos de 2025 com pagamentos recusados (`status=false`) cuja avaliação de fraude foi legítima (`fraude=false`). O resultado contém `id_pedido`, `uf`, `forma_pagamento`, `valor_total` e `data_criacao`, ordenado por UF, forma de pagamento e data, gravado em Parquet.

## Estrutura
- `main.py`: aggregation root que instancia dependências e aciona o pipeline.
- `src/sales_report/config`: configurações centralizadas (`AppConfig`).
- `src/sales_report/spark`: gerenciamento da sessão Spark.
- `src/sales_report/io`: schemas explícitos e I/O de dados.
- `src/sales_report/business`: lógica de negócio com logging e tratamento de erros.
- `src/sales_report/orchestration`: orquestração do pipeline.
- `tests/`: testes unitários (`pytest`) da lógica de negócio.

## Pré-requisitos
- Python 3.10+
- PySpark (veja `requirements.txt` para dependências)

## Como executar
1. Instale dependências: `python3 -m pip install -r requirements.txt`
2. Execute o pipeline: `python3 main.py`
   - Leitura: `datasets-csv-pedidos/data/pedidos` e `dataset-json-pagamentos/data/pagamentos`
   - Saída: `output/relatorio_pedidos` (Parquet)

## Testes
Execute `python3 -m pytest` para rodar o teste unitário da classe de lógica de negócio (`ReportGenerator`).
