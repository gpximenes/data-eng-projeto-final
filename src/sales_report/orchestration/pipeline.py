from sales_report.business.report_generator import ReportGenerator
from sales_report.io.data_io import DataIO


class PipelineOrchestrator:
    """Coordinates reading, transforming, and writing datasets."""

    def __init__(self, data_io: DataIO, report_generator: ReportGenerator):
        self.data_io = data_io
        self.report_generator = report_generator

    def run(self) -> None:
        pedidos_df = self.data_io.read_pedidos()
        pagamentos_df = self.data_io.read_pagamentos()
        report_df = self.report_generator.build_report(pedidos_df, pagamentos_df)
        self.data_io.write_report(report_df)
