# src/extract/midagri.py
# Clase para extraer precios históricos desde el reporte Power BI de MIDAGRI.

import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from src.config import (
    POWERBI_REPORT_URL, POWERBI_BASE_API,
    POWERBI_TIMEOUT, POWERBI_WORKERS,
    FECHA_INICIO, FECHA_FIN, DEPARTAMENTO,
    PRODUCTOS, PRODUCTOS_DIR,
)
from src.extract.api_utils import (
    get_resource_key, build_headers, build_session,
    safe_get, safe_post,
    load_progress, save_progress,
    mark_snapshot_completed, mark_error,
    nombre_snapshot, MESES_ES_MAYUS,
)
from src.logger import get_logger

logger = get_logger("midagri")


# ── Rutas de snapshot ─────────────────────────────────────────────────────────

def build_ruta_snapshot(producto: str, fecha: pd.Timestamp) -> Path:
    """
    Construye la ruta completa para un snapshot histórico.

    Estructura:
        data/raw/datos_historicos/productos/<producto>/<YYYY>/<MM_NOMBREMES>/<YYYY_MM_ddA-ddB>.csv
    """
    mes_nombre = MESES_ES_MAYUS[fecha.month]
    carpeta = (
        PRODUCTOS_DIR
        / producto.lower()
        / str(fecha.year)
        / f"{fecha.month:02d}_{mes_nombre}"
    )
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta / nombre_snapshot(fecha)


# ── Reporte de estado en disco ────────────────────────────────────────────────

def reportar_ultimo_archivo() -> None:
    """
    Imprime un resumen del último archivo CSV generado por producto,
    indicando año, mes y último rango de días descargado.
    """
    patron = re.compile(r"^(\d{4})_(\d{2})_(\d{2}-\d{2})\.csv$")
    meses_es = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }
    if not PRODUCTOS_DIR.exists():
        print("  [sin archivos previos en disco]")
        return

    productos_dirs = [p for p in PRODUCTOS_DIR.iterdir() if p.is_dir()]
    if not productos_dirs:
        print("  [sin archivos previos en disco]")
        return

    for prod_dir in sorted(productos_dirs):
        ultimo = None
        for csv_path in prod_dir.rglob("*.csv"):
            m = patron.match(csv_path.name)
            if not m:
                continue
            anio, mes, rango = m.groups()
            dia_fin = int(rango.split("-")[1])
            clave = (int(anio), int(mes), dia_fin)
            if ultimo is None or clave > ultimo[0]:
                ultimo = (clave, rango)

        if not ultimo:
            continue

        ultimo_anio, ultimo_mes, _ = ultimo[0]
        ultimo_rango = ultimo[1]
        total = sum(1 for _ in prod_dir.rglob("*.csv"))
        print(
            f"  OK Producto [{prod_dir.name:10}]: "
            f"último descargado -> {ultimo_anio} / {meses_es[ultimo_mes]} / rango {ultimo_rango}  "
            f"({total} archivos en total)"
        )
        print("     Continuará descargando los archivos faltantes a partir de ese rango.")


# ── Parseo de respuesta Power BI ──────────────────────────────────────────────

def _is_timestamp_like(v) -> bool:
    """Valida si un valor parece timestamp en milisegundos."""
    try:
        iv = int(float(v))
        return 946684800000 <= iv <= 4102444800000
    except Exception:
        return False


def _extract_rows_from_dsr(data: dict) -> list:
    """Extrae filas desde estructuras DSR variables (dict/list y bloques DM0)."""
    dsr = data.get("dsr")
    datasets = []
    if isinstance(dsr, dict):
        datasets.extend(dsr.get("DS", []))
    elif isinstance(dsr, list):
        for item in dsr:
            if isinstance(item, dict):
                datasets.extend(item.get("DS", []))
    rows = []
    for ds in datasets:
        ph = ds.get("PH", {})
        if isinstance(ph, dict):
            if "DM0" in ph and isinstance(ph["DM0"], list):
                rows.extend(ph["DM0"])
            elif ph:
                first = next(iter(ph.values()))
                if isinstance(first, list):
                    rows.extend(first)
        if isinstance(ph, list):
            for block in ph:
                if not isinstance(block, dict):
                    continue
                if "DM0" in block and isinstance(block["DM0"], list):
                    rows.extend(block["DM0"])
                else:
                    for v in block.values():
                        if isinstance(v, list):
                            rows.extend(v)
    return rows


def parse_querydata_to_df(querydata_json: dict) -> pd.DataFrame:
    """Convierte respuesta querydata de Power BI en DataFrame robusto."""
    data = querydata_json["results"][0]["result"]["data"]
    select_meta = data["descriptor"]["Select"]
    col_names = [s.get("Name", f"col_{i}") for i, s in enumerate(select_meta)]
    value_keys = [s.get("Value") for s in select_meta]
    rows = _extract_rows_from_dsr(data)
    out = []
    prev = [None] * len(col_names)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if "C" in r:
            vals = list(r.get("C", []))
            if not vals:
                continue
            if not _is_timestamp_like(vals[0]) and prev[0] is not None:
                vals = [prev[0]] + vals
            while len(vals) < len(col_names):
                vals.append(None)
            if len(vals) > len(col_names):
                vals = vals[:len(col_names)]
            prev = vals
            out.append(dict(zip(col_names, vals)))
        else:
            vals = []
            for i, vk in enumerate(value_keys):
                if vk is not None and vk in r:
                    vals.append(r.get(vk))
                else:
                    vals.append(r.get(col_names[i]))
            out.append(dict(zip(col_names, vals)))
    return pd.DataFrame(out)


def parse_with_fallback(querydata_json: dict, command: dict) -> pd.DataFrame:
    """Parsea querydata y aplica fallback cuando la estructura varía."""
    try:
        return parse_querydata_to_df(querydata_json)
    except Exception:
        data = querydata_json["results"][0]["result"]["data"]
        rows = _extract_rows_from_dsr(data)
        select_meta = command["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"]
        col_names = [s.get("Name", f"col_{i}") for i, s in enumerate(select_meta)]
        out, prev = [], [None] * len(col_names)
        for r in rows:
            if not isinstance(r, dict):
                continue
            vals = list(r.get("C", [])) if "C" in r else []
            if not vals:
                keys = [k for k in r.keys() if isinstance(k, str) and len(k) >= 2
                        and k[0].isalpha() and k[1:].isdigit()]
                keys = sorted(keys, key=lambda k: (k[0], int(k[1:])))
                vals = [r.get(k) for k in keys]
            if not vals:
                continue
            if not _is_timestamp_like(vals[0]) and prev[0] is not None:
                vals = [prev[0]] + vals
            while len(vals) < len(col_names):
                vals.append(None)
            vals = vals[:len(col_names)]
            prev = vals
            out.append(dict(zip(col_names, vals)))
        return pd.DataFrame(out)


# ── Comandos semánticos Power BI ──────────────────────────────────────────────

def build_hist_command(genero: str, variedad: str,
                       fecha_inicio: str, departamento: str) -> dict:
    """Construye comando semántico para extraer fechas semanales por producto."""
    return {
        "Commands": [{
            "SemanticQueryDataShapeCommand": {
                "Query": {
                    "Version": 2,
                    "From": [
                        {"Name": "f", "Entity": "FECHA PRECIO", "Type": 0},
                        {"Name": "m", "Entity": "MEDIDAS", "Type": 0},
                        {"Name": "b", "Entity": "BD_GENERO_CULTIVO", "Type": 0},
                        {"Name": "u", "Entity": "BD_UBIGEO", "Type": 0},
                    ],
                    "Select": [
                        {"Column": {"Expression": {"SourceRef": {"Source": "f"}},
                                    "Property": "FECHA"}, "Name": "FECHA PRECIO.FECHA"},
                        {"Measure": {"Expression": {"SourceRef": {"Source": "m"}},
                                     "Property": "PRECIO_CHACRA"}, "Name": "MEDIDAS.PRECIO_CHACRA"},
                        {"Measure": {"Expression": {"SourceRef": {"Source": "m"}},
                                     "Property": "PRECIO_MAYORISTA"}, "Name": "MEDIDAS.PRECIO_MAYORISTA"},
                    ],
                    "Where": [
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "u"}}, "Property": "NOMBDEP"}}],
                            "Values": [[{"Literal": {"Value": f"'{departamento}'"}}]]}}},
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "b"}}, "Property": "GENERO"}}],
                            "Values": [[{"Literal": {"Value": f"'{genero}'"}}]]}}},
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "b"}}, "Property": "VARIEDAD"}}],
                            "Values": [[{"Literal": {"Value": f"'{variedad}'"}}]]}}},
                        {"Condition": {"Comparison": {
                            "ComparisonKind": 2,
                            "Left": {"Column": {"Expression": {"SourceRef": {"Source": "f"}},
                                                "Property": "FECHA"}},
                            "Right": {"Literal": {"Value": f"datetime'{fecha_inicio}T00:00:00'"}}}}},
                    ],
                },
                "Binding": {
                    "Primary": {"Groupings": [{"Projections": [0, 1, 2]}]},
                    "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 30000}}},
                    "Version": 1,
                },
                "ExecutionMetricsKind": 1,
            }
        }]
    }


def build_table_snapshot_command(genero: str, variedad: str,
                                 departamento: str, fecha_iso: str) -> dict:
    """Construye comando snapshot para recuperar valores correctos por fecha."""
    return {
        "Commands": [{
            "SemanticQueryDataShapeCommand": {
                "Query": {
                    "Version": 2,
                    "From": [
                        {"Name": "m",  "Entity": "MEDIDAS", "Type": 0},
                        {"Name": "b1", "Entity": "BD_GENERO_CULTIVO", "Type": 0},
                        {"Name": "u",  "Entity": "BD_UBIGEO", "Type": 0},
                        {"Name": "f",  "Entity": "FECHA PRECIO", "Type": 0},
                    ],
                    "Select": [
                        {"Column": {"Expression": {"SourceRef": {"Source": "u"}},
                                    "Property": "NOMBDEP"}, "Name": "BD_UBIGEO.NOMBDEP"},
                        {"Column": {"Expression": {"SourceRef": {"Source": "b1"}},
                                    "Property": "GENERO"}, "Name": "BD_GENERO_CULTIVO.GENERO"},
                        {"Measure": {"Expression": {"SourceRef": {"Source": "m"}},
                                     "Property": "PRECIO_CHACRA"}, "Name": "MEDIDAS.PRECIO_CHACRA"},
                        {"Measure": {"Expression": {"SourceRef": {"Source": "m"}},
                                     "Property": "PRECIO_MAYORISTA"}, "Name": "MEDIDAS.PRECIO_MAYORISTA"},
                    ],
                    "Where": [
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "u"}}, "Property": "NOMBDEP"}}],
                            "Values": [[{"Literal": {"Value": f"'{departamento}'"}}]]}}},
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "b1"}}, "Property": "GENERO"}}],
                            "Values": [[{"Literal": {"Value": f"'{genero}'"}}]]}}},
                        {"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {
                            "Source": "b1"}}, "Property": "VARIEDAD"}}],
                            "Values": [[{"Literal": {"Value": f"'{variedad}'"}}]]}}},
                        {"Condition": {"Comparison": {
                            "ComparisonKind": 2,
                            "Left": {"Column": {"Expression": {"SourceRef": {"Source": "f"}},
                                                "Property": "FECHA"}},
                            "Right": {"Literal": {"Value": f"datetime'{fecha_iso}T00:00:00'"}}}}},
                    ],
                    "OrderBy": [{"Direction": 2, "Expression": {"Measure": {
                        "Expression": {"SourceRef": {"Source": "m"}},
                        "Property": "PRECIO_MAYORISTA"}}}],
                },
                "Binding": {
                    "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3]}]},
                    "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}},
                    "Version": 1,
                },
                "ExecutionMetricsKind": 1,
            }
        }]
    }


# ── Consulta de fechas disponibles ───────────────────────────────────────────

def get_weekly_dates(session, headers: dict, model_id: int,
                     genero: str, variedad: str) -> list:
    """Obtiene fechas semanales disponibles para un producto objetivo."""
    cmd = build_hist_command(genero, variedad, FECHA_INICIO, DEPARTAMENTO)
    body = {
        "version": "1.0.0",
        "queries": [{"Query": cmd, "QueryId": f"fechas_{genero.lower()}"}],
        "cancelQueries": [],
        "modelId": model_id,
        "userPreferredLocale": "es-PE",
    }
    raw = safe_post(session, f"{POWERBI_BASE_API}/querydata?synchronous=true",
                    headers=headers, payload=body, timeout=POWERBI_TIMEOUT)
    df = parse_with_fallback(raw, cmd)
    if "FECHA PRECIO.FECHA" not in df.columns:
        logger.warning("No se encontraron fechas para %s - %s", genero, variedad)
        return []
    fechas = pd.to_datetime(df["FECHA PRECIO.FECHA"], unit="ms", errors="coerce").dropna()
    fechas = fechas[
        (fechas >= pd.to_datetime(FECHA_INICIO)) &
        (fechas <= pd.to_datetime(FECHA_FIN))
    ]
    logger.info("Fechas disponibles para %s - %s: %s", genero, variedad, len(fechas))
    return sorted(fechas.drop_duplicates().tolist())


# ── Normalización y persistencia de snapshot ─────────────────────────────────

def normalizar_fila_snapshot(df_raw: pd.DataFrame, fecha: pd.Timestamp,
                              genero: str, categoria: str) -> pd.DataFrame:
    """Normaliza una fila snapshot y exige par precio_chacra + precio_mayorista."""
    rename_map = {
        "BD_UBIGEO.NOMBDEP": "departamento",
        "BD_GENERO_CULTIVO.GENERO": "genero",
        "MEDIDAS.PRECIO_CHACRA": "precio_chacra",
        "MEDIDAS.PRECIO_MAYORISTA": "precio_mayorista",
    }
    if df_raw.empty:
        return pd.DataFrame()
    df = df_raw.rename(columns=rename_map).copy()
    for c in ["departamento", "genero", "precio_chacra", "precio_mayorista"]:
        if c not in df.columns:
            df[c] = pd.NA
    df["departamento"] = (df["departamento"].replace({0: DEPARTAMENTO})
                          .fillna(DEPARTAMENTO).astype(str).str.upper())
    df["genero"] = (df["genero"].replace({0: genero})
                    .fillna(genero).astype(str).str.upper())
    df["precio_chacra"]    = pd.to_numeric(df["precio_chacra"],    errors="coerce")
    df["precio_mayorista"] = pd.to_numeric(df["precio_mayorista"], errors="coerce")
    df = df.dropna(subset=["precio_chacra", "precio_mayorista"], how="any")
    if df.empty:
        return pd.DataFrame()
    df["fecha"]           = pd.to_datetime(fecha)
    df["fecha_extraccion"] = pd.Timestamp.now()
    df["producto"]        = genero.title()
    df["categoria"]       = categoria
    df["fuente"]          = "powerbi_midagri"
    cols = ["fecha", "fecha_extraccion", "departamento", "genero",
            "precio_chacra", "precio_mayorista", "producto", "categoria", "fuente"]
    return df[cols].head(1)


def _guardar_snapshot_csv(fila: pd.DataFrame, producto: str,
                          fecha: pd.Timestamp) -> Path:
    """
    Persiste INMEDIATAMENTE el resultado de un snapshot en su CSV individual.
    Si el archivo ya existe lo sobreescribe (idempotente).
    """
    ruta = build_ruta_snapshot(producto, fecha)
    fila.to_csv(ruta, index=False, encoding="utf-8-sig")
    return ruta


def _fallback_historico_local() -> pd.DataFrame:
    """
    Reconstruye el histórico leyendo todos los CSV de snapshots individuales
    ya guardados en data/raw/datos_historicos/productos/.
    """
    csvs = list(PRODUCTOS_DIR.rglob("*.csv"))
    if csvs:
        partes = []
        for csv_path in sorted(csvs):
            try:
                partes.append(pd.read_csv(csv_path))
            except Exception as e:
                logger.warning("No se pudo leer %s: %s", csv_path, e)
        if partes:
            df = pd.concat(partes, ignore_index=True)
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
            if "fecha_extraccion" in df.columns:
                df["fecha_extraccion"] = pd.to_datetime(df["fecha_extraccion"], errors="coerce")
            logger.info("Fallback desde snapshots CSV: %s filas de %s archivos", len(df), len(partes))
            return df.sort_values(["producto", "fecha"]).reset_index(drop=True)
    logger.warning("No se encontró histórico local para fallback.")
    return pd.DataFrame()


# ── Descarga concurrente de un snapshot ──────────────────────────────────────

def _fetch_snapshot(session, headers: dict, model_id: int,
                    genero: str, categoria: str, fecha: pd.Timestamp) -> pd.DataFrame:
    """
    Descarga, persiste y registra un único snapshot por fecha.
    Flujo:
      1. Verifica si snapshot_id ya figura en progress.json → lo omite.
      2. Verifica si el CSV ya existe en disco → lo omite (idempotencia).
      3. Consulta la API de Power BI.
      4. Guarda el resultado INMEDIATAMENTE en su CSV individual en disco.
      5. Registra el snapshot como completado en progress.json.
    """
    meses_es = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }
    snapshot_id = f"{genero}|{categoria}|{fecha.strftime('%Y-%m-%d')}"
    ruta_csv    = build_ruta_snapshot(genero, fecha)
    desc = (f"producto {genero} | {fecha.year} / {meses_es[fecha.month]}"
            f"  ({fecha.strftime('%Y-%m-%d')})")

    # Verificación 1: ya en progress.json
    progress = load_progress()
    if snapshot_id in set(progress.get("completed_snapshots", [])):
        logger.info("[OMITIDO - ya descargado] %s", desc)
        if ruta_csv.exists():
            return pd.read_csv(ruta_csv)
        return pd.DataFrame()

    # Verificación 2: CSV ya en disco
    if ruta_csv.exists():
        logger.info("[OMITIDO - CSV en disco]  %s", desc)
        mark_snapshot_completed(snapshot_id)
        return pd.read_csv(ruta_csv)

    # Consulta a la API
    logger.info("[DESCARGANDO] %s", desc)
    fecha_iso = fecha.strftime("%Y-%m-%d")
    cmd = build_table_snapshot_command(genero, categoria, DEPARTAMENTO, fecha_iso)
    body = {
        "version": "1.0.0",
        "queries": [{"Query": cmd, "QueryId": f"snap_{genero.lower()}_{fecha_iso}"}],
        "cancelQueries": [],
        "modelId": model_id,
        "userPreferredLocale": "es-PE",
    }
    try:
        raw   = safe_post(session, f"{POWERBI_BASE_API}/querydata?synchronous=true",
                          headers=headers, payload=body, timeout=POWERBI_TIMEOUT)
        df_raw = parse_with_fallback(raw, cmd)
        fila   = normalizar_fila_snapshot(df_raw, fecha, genero, categoria)
        if not fila.empty:
            ruta_guardada = _guardar_snapshot_csv(fila, genero, fecha)
            mark_snapshot_completed(snapshot_id)
            logger.info("[GUARDADO]    %s  ->  %s", desc, ruta_guardada.name)
        else:
            logger.warning("[SIN DATOS]   %s", desc)
        return fila
    except Exception as exc:
        mensaje = f"Snapshot falló | {desc} | {exc}"
        logger.exception(mensaje)
        mark_error(mensaje)
        raise


# ── Punto de entrada principal del módulo ────────────────────────────────────

def extraer_datos_api() -> pd.DataFrame:
    """
    Extrae el histórico de precios usando snapshots por fecha de forma concurrente.
    Muestra el estado previo de descargas al inicio de cada ejecución.

    Retorna: pd.DataFrame con todos los registros históricos obtenidos.
    """
    resource_key = get_resource_key(POWERBI_REPORT_URL)
    headers      = build_headers(resource_key)

    print("=" * 70)
    print("  ESTADO DEL PIPELINE - ARCHIVOS YA DESCARGADOS EN DISCO")
    print("=" * 70)
    reportar_ultimo_archivo()
    print()
    progress = load_progress()
    ya_completados = set(progress.get("completed_snapshots", []))
    if ya_completados:
        print(f"  Snapshots en progress.json : {len(ya_completados)} completados")
        print("  El pipeline omitirá esos registros y descargará los faltantes.")
    else:
        print("  Sin descargas previas registradas. Iniciando desde el principio.")
    print("=" * 70)
    print()

    progress["status"] = "running"
    save_progress(progress)
    logger.info("Inicio extracción histórica Power BI")

    with build_session() as session:
        meta_url = f"{POWERBI_BASE_API}/{resource_key}/modelsAndExploration?preferReadOnlySession=true"
        meta     = safe_get(session, meta_url, headers=headers, timeout=POWERBI_TIMEOUT)
        model_id = meta["models"][0]["id"]

        frames = []
        for prod in PRODUCTOS:
            genero   = prod["genero"]
            categoria = prod["categoria_deseada"]
            logger.info("Iniciando producto %s - %s", genero, categoria)

            fechas = get_weekly_dates(session, headers, model_id, genero, categoria)
            if not fechas:
                continue

            pendientes = [
                f for f in fechas
                if f"{genero}|{categoria}|{f.strftime('%Y-%m-%d')}" not in ya_completados
            ]
            logger.info(
                "Producto %-8s | total: %s fechas | ya descargadas: %s | pendientes: %s",
                genero, len(fechas), len(fechas) - len(pendientes), len(pendientes),
            )

            producto_rows = []
            with ThreadPoolExecutor(max_workers=POWERBI_WORKERS) as executor:
                futures = [
                    executor.submit(_fetch_snapshot, session, headers, model_id,
                                    genero, categoria, f)
                    for f in fechas
                ]
                for future in as_completed(futures):
                    try:
                        fila = future.result()
                    except Exception:
                        continue
                    if not fila.empty:
                        producto_rows.append(fila)

            if producto_rows:
                df_prod = pd.concat(producto_rows, ignore_index=True)
                frames.append(df_prod)
                logger.info("Producto %s listo con %s filas", genero, len(df_prod))
            else:
                logger.warning("Producto %s no devolvió filas válidas", genero)

    if not frames:
        logger.warning("Sin frames remotos. Intentando fallback local.")
        return _fallback_historico_local()

    df_api = pd.concat(frames, ignore_index=True)
    if df_api.empty:
        logger.warning("DataFrame histórico vacío. Intentando fallback local.")
        return _fallback_historico_local()

    df_api = df_api.dropna(subset=["fecha", "precio_chacra", "precio_mayorista"], how="any")
    df_api = df_api.sort_values(["producto", "fecha"]).reset_index(drop=True)

    progress = load_progress()
    progress["status"] = "completed_powerbi"
    save_progress(progress)
    logger.info("Extracción histórica completada con %s filas", len(df_api))
    return df_api
