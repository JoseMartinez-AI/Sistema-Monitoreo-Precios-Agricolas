# src/config.py
# Carga centralizada de todas las variables de configuración desde .env
# y definición de rutas de proyecto.
# Todos los demás módulos importan desde aquí; NUNCA leen el .env directamente.

from pathlib import Path
from dotenv import load_dotenv
import os

# ── Cargar .env desde la raíz del proyecto ───────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent   # Preject_Version_Prueba/
load_dotenv(_ROOT / ".env")

# ── Credenciales y endpoints (leídos del .env) ───────────────────────────────
POWERBI_REPORT_URL : str = os.getenv(
    "POWERBI_REPORT_URL",
    "https://app.powerbi.com/view?r=eyJrIjoiMDNmMWM5NWItZWYyMS00NGYxLTg2NmYtYTRhM2MwOWViOGU4Iiw"
    "idCI6IjdmMDg0NjI3LTdmNDAtNDg3OS04OTE3LTk0Yjg2ZmQzNWYzZiJ9&pageName=ReportSection",
)
POWERBI_BASE_API   : str = os.getenv(
    "POWERBI_BASE_API",
    "https://wabi-paas-1-scus-api.analysis.windows.net/public/reports",
)
POWERBI_TIMEOUT    : int = int(os.getenv("POWERBI_TIMEOUT", "30"))
POWERBI_WORKERS    : int = int(os.getenv("POWERBI_WORKERS", "4"))

EMMSA_AJAX_URL     : str = os.getenv(
    "EMMSA_AJAX_URL",
    "https://old.emmsa.com.pe/emmsa_spv/app/reportes/ajax/rpt07_gettable_new_web.php",
)
EMMSA_TIMEOUT      : int = int(os.getenv("EMMSA_TIMEOUT", "20"))

# ── Parámetros del pipeline ──────────────────────────────────────────────────
FECHA_INICIO  : str = os.getenv("FECHA_INICIO", "2021-01-01")
FECHA_FIN     : str = os.getenv("FECHA_FIN",    "2026-03-31")
DEPARTAMENTO  : str = os.getenv("DEPARTAMENTO", "LIMA")

# ── Productos objetivo (lista fija del dominio del negocio) ──────────────────
PRODUCTOS = [
    {"genero": "PAPA",    "categoria_deseada": "PAPA YUNGAY"},
    {"genero": "CEBOLLA", "categoria_deseada": "CEBOLLA CABEZA ROJA"},
    {"genero": "CHOCLO",  "categoria_deseada": "CHOCLO (TIPO CUSCO)"},
]

PRODUCTOS_VALIDOS  : list[str] = ["Papa", "Cebolla", "Choclo"]

CATEGORIAS_VALIDAS : list[str] = [
    # Papa
    "PAPA AMARILLA", "PAPA BLANCA", "PAPA YUNGAY",
    # Cebolla
    "CEBOLLA CABEZA BLANCA", "CEBOLLA CABEZA ROJA",
    # Choclo
    "CHOCLO PARDO", "CHOCLO (TIPO CUSCO)",
]

DEPARTAMENTOS_VALIDOS : list[str] = ["LIMA", "AREQUIPA", "CUSCO"]

# Mapa producto → categorías válidas (para validación en cleaner)
CATEGORIAS_POR_PRODUCTO : dict[str, list[str]] = {
    "Papa"  : ["PAPA AMARILLA", "PAPA BLANCA", "PAPA YUNGAY"],
    "Cebolla": ["CEBOLLA CABEZA BLANCA", "CEBOLLA CABEZA ROJA"],
    "Choclo" : ["CHOCLO PARDO", "CHOCLO (TIPO CUSCO)"],
}

# Objetivos tiempo real EMMSA
OBJETIVOS_RT = [
    {"producto": "CEBOLLA", "variedad": "CEBOLLA CABEZA ROJA/MAJ/TAMB/LOC/CAM/MIL",
     "id_producto": "14", "id_variedad": "1402"},
    {"producto": "CHOCLO",  "variedad": "CHOCLO SERRANO TIPO CUZCO",
     "id_producto": "18", "id_variedad": "1804"},
    {"producto": "PAPA",    "variedad": "PAPA YUNGAY",
     "id_producto": "38", "id_variedad": "3811"},
]

# ── Rutas del proyecto ───────────────────────────────────────────────────────
DATA_DIR          : Path = _ROOT / os.getenv("DATA_DIR", "data")
LOGS_DIR          : Path = _ROOT / os.getenv("LOGS_DIR", "logs")

RAW_DIR           : Path = DATA_DIR / "raw"
HISTORICOS_DIR    : Path = RAW_DIR  / "datos_historicos"
TIEMPO_REAL_DIR   : Path = RAW_DIR  / "datos_tiempo_real"
PRODUCTOS_DIR     : Path = HISTORICOS_DIR / "productos"
RT_PRODUCTOS_DIR  : Path = TIEMPO_REAL_DIR / "productos"
PROCESSED_DIR     : Path = DATA_DIR / "processed"

LOG_FILE          : Path = LOGS_DIR / "pipeline_precios.log"
PROGRESS_FILE     : Path = LOGS_DIR / "pipeline_precios_progress.json"

# Nombres fijos de salida en processed/
CSV_SALIDA     : Path = PROCESSED_DIR / "reporte_precios_mayorista.csv"
PARQUET_SALIDA : Path = PROCESSED_DIR / "reporte_precios.parquet"

# ── Crear directorios al importar (idempotente) ──────────────────────────────
for _d in [HISTORICOS_DIR, TIEMPO_REAL_DIR, PRODUCTOS_DIR, RT_PRODUCTOS_DIR,
           PROCESSED_DIR, LOGS_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# Precrea subcarpeta por producto
for _prod in PRODUCTOS:
    (PRODUCTOS_DIR    / _prod["genero"].lower()).mkdir(parents=True, exist_ok=True)
    (RT_PRODUCTOS_DIR / _prod["genero"].lower()).mkdir(parents=True, exist_ok=True)
