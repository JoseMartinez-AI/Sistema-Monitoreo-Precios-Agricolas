# src/extract/api_utils.py
# Manejo de sesiones HTTP, retries, decodificación base64 de tokens Power BI
# y sistema de progreso persistente con threading lock.

import base64
import json
import threading
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pandas as pd
import requests

from src.config import PROGRESS_FILE
from src.logger import get_logger

logger = get_logger("api_utils")
PROGRESS_LOCK = threading.Lock()

# ── Nombres de meses en español (para rutas de archivos) ─────────────────────
MESES_ES_MAYUS = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO",  6: "JUNIO",   7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE",
}


# ── Helpers Power BI ──────────────────────────────────────────────────────────

def _pad_b64(s: str) -> str:
    """Completa padding base64 para decodificar tokens de Power BI."""
    return s + "=" * (-len(s) % 4)


def get_resource_key(report_url: str) -> str:
    """Extrae la resource key desde la URL pública del reporte Power BI."""
    qs = parse_qs(urlparse(report_url).query)
    token = qs.get("r", [None])[0]
    if not token:
        raise ValueError("No se encontró token r en la URL")
    payload = base64.b64decode(_pad_b64(unquote(token))).decode("utf-8")
    return json.loads(payload)["k"]


def build_headers(resource_key: str) -> dict:
    """Construye headers requeridos por endpoints internos de Power BI."""
    return {
        "X-PowerBI-ResourceKey": resource_key,
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "ActivityId": str(uuid.uuid4()),
        "RequestId": str(uuid.uuid4()),
    }


def build_session() -> requests.Session:
    """Reutiliza conexiones HTTP para reducir latencia en múltiples requests."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def safe_get(session: requests.Session, url: str,
             headers: dict, timeout: int = 30) -> dict:
    """Ejecuta GET con control de errores y devuelve JSON."""
    try:
        r = session.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"GET falló: {e}")


def safe_post(session: requests.Session, url: str, headers: dict,
              payload: dict, timeout: int = 30) -> dict:
    """Ejecuta POST con control de errores y devuelve JSON."""
    try:
        r = session.post(url, headers=headers, json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"POST falló: {e}")


# ── Sistema de progreso persistente ──────────────────────────────────────────

def _empty_progress() -> dict:
    return {
        "status": "not_started",
        "updated_at": None,
        "completed_snapshots": [],
        "errors": [],
    }


def load_progress() -> dict:
    """Lee el estado actual del pipeline desde el archivo JSON de progreso."""
    if not PROGRESS_FILE.exists():
        return _empty_progress()
    try:
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("No se pudo leer el archivo de progreso. Se reiniciará el seguimiento.")
        return _empty_progress()


def save_progress(progress: dict) -> None:
    """Persiste el estado actual del pipeline en disco."""
    progress["updated_at"] = datetime.now().isoformat(timespec="seconds")
    PROGRESS_FILE.write_text(
        json.dumps(progress, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def mark_snapshot_completed(snapshot_id: str) -> None:
    """Registra un snapshot como completado de forma thread-safe."""
    with PROGRESS_LOCK:
        progress = load_progress()
        completed = set(progress.get("completed_snapshots", []))
        completed.add(snapshot_id)
        progress["status"] = "running"
        progress["completed_snapshots"] = sorted(completed)
        save_progress(progress)


def mark_error(message: str) -> None:
    """Registra un error en el archivo de progreso (retiene los últimos 20)."""
    with PROGRESS_LOCK:
        progress = load_progress()
        errors = progress.get("errors", [])
        errors.append({"at": datetime.now().isoformat(timespec="seconds"), "message": message})
        progress["errors"] = errors[-20:]
        save_progress(progress)


# ── Utilidades de ruta y nombre de archivo ────────────────────────────────────

def _rango_semana(fecha: pd.Timestamp) -> str:
    """
    Calcula el rango de 7 días tomando `fecha` como día final.
    Ejemplo: fecha=2021-08-13 → rango '07-13'.
    """
    fin = pd.Timestamp(fecha)
    inicio = fin - pd.Timedelta(days=6)
    return f"{inicio.day:02d}-{fin.day:02d}"


def nombre_snapshot(fecha: pd.Timestamp) -> str:
    """Devuelve el nombre de archivo para un snapshot: YYYY_MM_ddA-ddB.csv"""
    return f"{fecha.year}_{fecha.month:02d}_{_rango_semana(fecha)}.csv"
