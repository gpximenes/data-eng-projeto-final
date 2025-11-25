from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
    """Centralized application settings."""

    pedidos_path: str
    pagamentos_path: str
    output_path: str

    @classmethod
    def default(cls) -> "AppConfig":
        """Build default config assuming datasets live at repo root."""
        base_dir = Path(__file__).resolve().parents[3]
        return cls(
            pedidos_path=str(base_dir / "datasets-csv-pedidos" / "data" / "pedidos"),
            pagamentos_path=str(base_dir / "dataset-json-pagamentos" / "data" / "pagamentos"),
            output_path=str(base_dir / "output" / "relatorio_pedidos"),
        )
