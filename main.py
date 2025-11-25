from pathlib import Path
import sys

# Allow running without installing the package by adding src to sys.path.
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sales_report.business.report_generator import ReportGenerator
from sales_report.config.settings import AppConfig
from sales_report.io.data_io import DataIO
from sales_report.orchestration.pipeline import PipelineOrchestrator
from sales_report.spark.session_manager import SparkSessionManager


def main() -> None:
    config = AppConfig.default()
    spark_manager = SparkSessionManager(app_name="SalesReportPipeline")
    spark = spark_manager.get_session()

    data_io = DataIO(spark, config)
    report_generator = ReportGenerator()
    orchestrator = PipelineOrchestrator(data_io, report_generator)

    orchestrator.run()
    spark.stop()


if __name__ == "__main__":
    main()
