# src/extract/emmsa.py
# Clase para scraping de precios en tiempo real desde el endpoint AJAX de EMMSA.

from io import StringIO
from pathlib import Path

import pandas as pd
import urllib3
from requests.exceptions import SSLError

from src.config import (
    EMMSA_AJAX_URL, EMMSA_TIMEOUT,
    OBJETIVOS_RT, RT_PRODUCTOS_DIR,
)
from src.extract.api_utils import build_session, nombre_snapshot
from src.logger import get_logger

logger = get_logger("emmsa")


# ── Rutas de tiempo real ──────────────────────────────────────────────────────

def build_ruta_tiempo_real(producto: str, fecha: pd.Timestamp) -> Path:
    """
    Construye la ruta para un archivo de tiempo real por producto.

    Estructura:
        data/raw/datos_tiempo_real/productos/<producto>/<YYYY_MM_ddA-ddB>.csv
    """
    carpeta = RT_PRODUCTOS_DIR / producto.lower()
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta / nombre_snapshot(fecha)


# ── Envío POST al AJAX de EMMSA ───────────────────────────────────────────────

def _post_emmsa(session, payload: dict) -> str:
    """Envía POST al endpoint AJAX de EMMSA con fallback SSL para entorno académico."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
    }
    try:
        r = session.post(
            EMMSA_AJAX_URL, data=payload, headers=headers,
            timeout=EMMSA_TIMEOUT, verify=True,
        )
        r.raise_for_status()
        return r.text
    except SSLError:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = session.post(
            EMMSA_AJAX_URL, data=payload, headers=headers,
            timeout=EMMSA_TIMEOUT, verify=False,
        )
        r.raise_for_status()
        return r.text


# ── Persistencia por producto ─────────────────────────────────────────────────

def _guardar_tiempo_real_por_producto(df_rt: pd.DataFrame) -> list[str]:
    """
    Guarda las extracciones de tiempo real por producto en:
        data/raw/datos_tiempo_real/productos/<producto>/<YYYY_MM_ddA-ddB>.csv
    """
    if df_rt.empty:
        return []
    rutas = []
    for producto, g in df_rt.groupby(df_rt["producto"].astype(str).str.upper()):
        fecha_ref = pd.to_datetime(g["fecha"].iloc[0], errors="coerce")
        if pd.isna(fecha_ref):
            continue
        ruta = build_ruta_tiempo_real(producto, fecha_ref)
        g.to_csv(ruta, index=False, encoding="utf-8-sig")
        rutas.append(str(ruta))
    return rutas


# ── Punto de entrada principal del módulo ────────────────────────────────────

def extraer_datos_scraping(max_dias_atras: int = 5) -> pd.DataFrame:
    """
    Busca y extrae precios de EMMSA retrocediendo por fechas hasta encontrar datos.

    Parámetros: max_dias_atras (int) ventana de búsqueda hacia atrás desde hoy.
    Retorna: pd.DataFrame con los precios en tiempo real o DataFrame vacío.
    """
    hoy    = pd.Timestamp.today().normalize()
    claves = {(x["producto"], x["variedad"]) for x in OBJETIVOS_RT}
    logger.info("Inicio de scraping EMMSA. Ventana de búsqueda: %s días", max_dias_atras)

    with build_session() as session:
        for d in range(max_dias_atras + 1):
            fecha_eval = hoy - pd.Timedelta(days=d)
            logger.info("Consultando EMMSA para fecha %s", fecha_eval.strftime("%Y-%m-%d"))

            payload = {
                "vid_tipo": "1",
                "vprod": ",".join(x["id_producto"] for x in OBJETIVOS_RT),
                "vvari": ",".join(x["id_variedad"]  for x in OBJETIVOS_RT),
                "vfecha": fecha_eval.strftime("%d/%m/%Y"),
            }

            try:
                html   = _post_emmsa(session, payload)
                tablas = pd.read_html(StringIO(html))
            except Exception:
                tablas = []

            if not tablas:
                continue

            df = tablas[0].copy()
            if len(df.columns) < 5:
                continue

            df = df.iloc[:, :5].copy()
            df.columns = ["producto", "variedad", "precio_min", "precio_max", "precio_prom"]
            df["producto"] = df["producto"].astype(str).str.strip().str.upper()
            df["variedad"] = df["variedad"].astype(str).str.strip().str.upper()
            for c in ["precio_min", "precio_max", "precio_prom"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            pares = pd.Series(list(zip(df["producto"], df["variedad"])), index=df.index)
            df    = df[pares.isin(claves)].copy()
            if df.empty:
                continue

            df["fecha"]            = fecha_eval
            df["fecha_extraccion"] = pd.Timestamp.now()
            df["departamento"]     = "LIMA"
            df["genero"]           = df["producto"]
            df["producto"]         = df["producto"].str.title()
            df["categoria"]        = df["variedad"]
            df["precio_mayorista"] = df["precio_prom"]
            df["fuente"]           = "tiempo_real_emmsa"

            df_out = df[["fecha", "fecha_extraccion", "departamento", "genero",
                          "precio_mayorista", "producto", "categoria", "fuente"]]

            rutas_guardadas = _guardar_tiempo_real_por_producto(df_out)
            logger.info("Scraping EMMSA completado con %s filas", len(df_out))
            if rutas_guardadas:
                logger.info("Tiempo real guardado por producto: %s", rutas_guardadas)

            return df_out

    logger.warning("Scraping EMMSA no devolvió filas.")
    return pd.DataFrame()
