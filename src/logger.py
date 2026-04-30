# src/logger.py
# Configuración centralizada del registro de eventos (logging).
# Todos los módulos obtienen su logger con: from src.logger import get_logger

import logging
from src.config import LOG_FILE


def _setup_root_logger() -> None:
    """Configura el logger raíz con handler de archivo y consola."""
    logger = logging.getLogger("pipeline_precios")
    if logger.handlers:          # evitar duplicar handlers al reimportar
        return

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # Handler de archivo
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler de consola
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)


_setup_root_logger()


def get_logger(name: str) -> logging.Logger:
    """
    Retorna un logger hijo del logger raíz del pipeline.

    Parámetros: name (str) nombre del módulo que solicita el logger.
    Retorna: logging.Logger con el nombre 'pipeline_precios.<name>'.
    """
    return logging.getLogger(f"pipeline_precios.{name}")
