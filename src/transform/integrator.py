# src/transform/integrator.py
# Unión histórico/tiempo real, cálculo de brechas (IBC),
# regla de negocio + Z-score, optimización de memoria y persistencia.

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import CSV_SALIDA, PARQUET_SALIDA, PROCESSED_DIR
from src.logger import get_logger

logger = get_logger("integrator")


# ── Transformación e integración ────────

def transformar_datos(df_api: pd.DataFrame, df_scraping: pd.DataFrame) -> pd.DataFrame:
    """
    Integra histórico y tiempo real, calcula brechas e IBC y depura calidad.
    """
    if df_api.empty:
        raise ValueError("df_api está vacío")

    hist = df_api.copy()
    hist["diferencia_en_soles"] = hist["precio_mayorista"] - hist["precio_chacra"]
    hist["porcentaje"] = np.where(
        hist["precio_chacra"] > 0,
        (hist["diferencia_en_soles"] / hist["precio_chacra"]) * 100,
        np.nan,
    )

    rt = df_scraping.copy()
    if not rt.empty:
        last_chacra = (
            hist.sort_values("fecha")
            .groupby(["producto", "categoria"], as_index=False)
            .last()[["producto", "categoria", "precio_chacra", "fecha"]]
            .rename(columns={"fecha": "fecha_precio_chacra_usada"})
        )
        # Cruzamos usando ambas claves para que el tiempo real encuentre su chacra exacto
        rt = rt.merge(last_chacra, on=["producto", "categoria"], how="left")
        
        rt["diferencia_en_soles"] = rt["precio_mayorista"] - rt["precio_chacra"]
        rt["porcentaje"] = np.where(
            rt["precio_chacra"] > 0,
            (rt["diferencia_en_soles"] / rt["precio_chacra"]) * 100,
            np.nan,
        )
    else:
        for c in ["precio_chacra", "diferencia_en_soles", "porcentaje"]:
            rt[c] = pd.Series(dtype="float64")

    # Columnas finales (se elimina 'genero' para evitar duplicidad con 'producto')
    cols = [
        "fecha", "fecha_extraccion", "departamento",
        "precio_chacra", "precio_mayorista", "diferencia_en_soles", "porcentaje",
        "producto", "categoria", "fuente",
    ]
    for c in cols:
        if c not in hist.columns:
            hist[c] = pd.NA
        if c not in rt.columns:
            rt[c] = pd.NA

    df = pd.concat([hist[cols], rt[cols]], ignore_index=True)
    df["fecha"]            = pd.to_datetime(df["fecha"],            errors="coerce")
    df["fecha_extraccion"] = pd.to_datetime(df["fecha_extraccion"], errors="coerce")
    for c in ["precio_chacra", "precio_mayorista", "diferencia_en_soles", "porcentaje"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # IBC del proyecto (Fórmula corregida asegurando valor diario)
    df["ibc_pct"] = np.where(
        df["precio_chacra"] > 0,
        ((df["precio_mayorista"] - df["precio_chacra"]) / df["precio_chacra"]) * 100,
        np.nan,
    )

    # Calidad de datos
    df = df.dropna(subset=["fecha", "producto", "categoria", "precio_mayorista"]).copy()
    df = df.drop_duplicates(subset=["fecha", "producto", "categoria", "fuente"], keep="last").copy()
    df = df.sort_values(["producto", "fecha"]).reset_index(drop=True)
    return df


# ── Regla de negocio + Z-score ──────────────────────────────────

def aplicar_regla_negocio_y_zscore(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Aplica la gobernanza mínima de calidad pero CONSERVA las alertas de especulación.
    """
    df_t4 = df.copy()

    # Asegurar tipo numérico en precios
    price_cols = ["precio_chacra", "precio_mayorista"]
    df_t4[price_cols] = df_t4[price_cols].apply(pd.to_numeric, errors="coerce")

    # Regla de negocio mínima (para no romper el IBC)
    # Permite precio_chacra nulo si es necesario, pero filtra absurdos
    mask_regla_negocio = (
        (df_t4["precio_mayorista"] > 0)
    )

    # Z-score por producto y categoría
    grp_precio = df_t4.groupby(["producto", "categoria"])["precio_mayorista"]
    zscore = (
        (df_t4["precio_mayorista"] - grp_precio.transform("mean"))
        / grp_precio.transform("std").replace(0, np.nan)
    ).fillna(0)

    df_t4["zscore_precio_mayorista"] = zscore
    # CORRECCIÓN: Etiquetamos el outlier pero NO LO BORRAMOS
    df_t4["flag_outlier_zscore"]     = zscore.abs() > 3

    filas_iniciales  = len(df_t4)
    no_cumple_regla  = int((~mask_regla_negocio).sum())
    outliers         = int(df_t4["flag_outlier_zscore"].sum())

    # Solo eliminamos lo que incumple la regla de precios coherentes, pero dejamos los outliers
    df_transformado = df_t4.loc[mask_regla_negocio].reset_index(drop=True)

    stats = {
        "filas_iniciales"        : filas_iniciales,
        "no_cumplen_regla_negocio": no_cumple_regla,
        "outliers_zscore"        : outliers,
        "filas_finales"          : len(df_transformado),
    }
    logger.info(
        "Gobernanza aplicada: iniciales=%s | eliminados por regla=%s | alertas Z-score=%s | finales=%s",
        filas_iniciales, no_cumple_regla, outliers, len(df_transformado),
    )
    return df_transformado, stats


# ── Optimización de memoria ─────────────────────────────────────

def optimizar_memoria(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Reduce el uso de memoria con downcasting.
    """
    df_opt    = df.reset_index(drop=True).copy()
    mem_antes = df_opt.memory_usage(deep=True).sum() / 1024 ** 2

    float_cols = ["precio_chacra", "precio_mayorista", "ibc_pct", "porcentaje", "diferencia_en_soles", "zscore_precio_mayorista"]
    for col in df_opt.columns.intersection(float_cols):
        df_opt[col] = pd.to_numeric(df_opt[col], errors="coerce", downcast="float")

    cat_cols = ["producto", "categoria", "fuente", "departamento"]
    for col in df_opt.columns.intersection(cat_cols):
        df_opt[col] = df_opt[col].astype("category")

    mem_despues = df_opt.memory_usage(deep=True).sum() / 1024 ** 2
    ahorro_pct  = ((mem_antes - mem_despues) / mem_antes * 100) if mem_antes > 0 else 0

    logger.info(
        "Optimización: %.3f MB → %.3f MB (ahorro %.2f%%)",
        mem_antes, mem_despues, ahorro_pct,
    )
    return df_opt, {
        "mem_antes_mb"  : round(mem_antes, 3),
        "mem_despues_mb": round(mem_despues, 3),
        "ahorro_pct"    : round(ahorro_pct, 2),
    }


# ── Persistencia de salida ─────────────────

def guardar_datos(df: pd.DataFrame) -> dict:
    """
    Guarda el DataFrame procesado como CSV y Parquet.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # CSV
    df.to_csv(CSV_SALIDA, index=False, encoding="utf-8-sig")

    # Parquet
    df_pq = df.copy()
    for col in df_pq.select_dtypes(include=["category"]).columns:
        df_pq[col] = df_pq[col].astype(str)
    for col in df_pq.select_dtypes(include=["float32"]).columns:
        df_pq[col] = df_pq[col].astype("float64")
    for col in df_pq.select_dtypes(include=["object"]).columns:
        try:
            converted = pd.to_datetime(df_pq[col], errors="raise")
            df_pq[col] = converted
        except Exception:
            df_pq[col] = df_pq[col].astype(str)
    
    tabla = pa.Table.from_pandas(df_pq, preserve_index=False)
    pq.write_table(tabla, PARQUET_SALIDA, compression="snappy")

    result = {
        "csv"     : str(CSV_SALIDA),
        "parquet" : str(PARQUET_SALIDA),
        "filas"   : len(df),
        "columnas": len(df.columns),
        "fuentes" : df["fuente"].value_counts().to_dict() if "fuente" in df.columns else {},
    }
    logger.info("Archivos guardados: CSV=%s | Parquet=%s | filas=%s",
                CSV_SALIDA.name, PARQUET_SALIDA.name, len(df))
    return result


# ── Resumen final ────────────────────────────────────────────────

def imprimir_resumen(df: pd.DataFrame) -> None:
    """
    Imprime el resumen final de ejecución del pipeline.
    """
    print("=" * 65)
    print("RESUMEN FINAL - PIPELINE PRECIOS MAYORISTAS")
    print("=" * 65)

    stats_basicos = {
        "Filas finales"   : len(df),
        "Columnas finales": len(df.columns),
        "Rango de fechas" : (
            f"{pd.to_datetime(df['fecha'], errors='coerce').min()} -> "
            f"{pd.to_datetime(df['fecha'], errors='coerce').max()}"
            if "fecha" in df.columns else "N/A"
        ),
    }
    for etiqueta, valor in stats_basicos.items():
        print(f"{etiqueta:<18}: {valor}")

    metricas = {
        "precio_chacra"   : ("Precio chacra prom.", ".3f"),
        "precio_mayorista": ("Precio mayorista prom.", ".3f"),
        "ibc_pct"         : ("IBC promedio (%)", ".2f"),
    }
    for col, (etiqueta, fmt) in metricas.items():
        if col in df.columns:
            print(f"{etiqueta:<20}: {df[col].mean():{fmt}}")

    print("\nFilas por fuente:")
    if "fuente" in df.columns:
        for fuente, total in df["fuente"].value_counts().items():
            print(f"  - {fuente}: {total:,}")
    else:
        print("  - No existe columna fuente")
    
    if "flag_outlier_zscore" in df.columns:
        total_alertas = df["flag_outlier_zscore"].sum()
        print(f"\nAlertas de Especulación Detectadas: {total_alertas}")