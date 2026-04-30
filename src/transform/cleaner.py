# src/transform/cleaner.py
# Normalización, eliminación de nulos, negativos y validación de productos.

import numpy as np
import pandas as pd

from src.config import PRODUCTOS_VALIDOS, CATEGORIAS_VALIDAS, DEPARTAMENTOS_VALIDOS
from src.logger import get_logger

logger = get_logger("cleaner")


# ── Diagnóstico de calidad ────────────

def diagnostico_calidad(nombre: str, df: pd.DataFrame) -> None:
    """
    Imprime un resumen de calidad del DataFrame: nulos, duplicados y outliers IQR.
    """
    print("=" * 70)
    print(f"DIAGNÓSTICO: {nombre}")
    print("=" * 70)
    print(f"Filas: {len(df):,} | Columnas: {len(df.columns)}")

    if df.empty:
        print("Dataset vacío.")
        return

    # Completitud
    nulos_pct = (df.isna().mean() * 100).sort_values(ascending=False).head(10)
    print("\nTop 10 columnas con mayor porcentaje de nulos:")
    print(nulos_pct.to_frame("nulos_%").round(2).to_string())

    # Unicidad por llave funcional
    llaves = [c for c in ["fecha", "producto", "categoria", "fuente"] if c in df.columns]
    if llaves:
        n_dup = df.duplicated(subset=llaves).sum()
        print(f"Duplicados por llave {llaves}: {n_dup:,}")

    # Validez de precios
    for col in ["precio_chacra", "precio_mayorista"]:
        if col in df.columns:
            serie     = pd.to_numeric(df[col], errors="coerce")
            invalidos = (serie < 0).sum()
            print(f"Valores inválidos ({col} < 0): {invalidos:,}")

    # Outliers IQR (solo diagnóstico)
    if "precio_mayorista" in df.columns:
        s = pd.to_numeric(df["precio_mayorista"], errors="coerce").dropna()
        if len(s) >= 4:
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr    = q3 - q1
            n_out  = ((s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)).sum()
            print(f"Outliers IQR en precio_mayorista (solo diagnóstico): {n_out:,}")


# ── Reporte de errores ─────────────────

def mostrando_errores(df: pd.DataFrame) -> None:
    """
    Muestra una tabla resumen de vacíos y negativos con totales y porcentajes.
    """
    print("#" * 55)
    print("CONTEO DE ERRORES (NaN y negativos)")
    print("#" * 55)

    errores_nan      = df.isna().sum()
    errores_negativos = (df.select_dtypes(include="number") < 0).sum()

    errores_df = pd.DataFrame({"NaN": errores_nan, "Negativos": errores_negativos}).fillna(0)
    errores_df["Total errores"] = errores_df.sum(axis=1)
    errores_df["% error"]       = (errores_df["Total errores"] / len(df) * 100).round(2)
    errores_df = errores_df.sort_values("% error", ascending=False)

    print(errores_df.to_string())
    print(f"Porcentaje de errores: {errores_df['% error'].sum()}%")


# ── Limpieza avanzada ──────────────────

def limpieza_avanzada(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las reglas de limpieza del pipeline T2:
    normalización de tipos, eliminación de fechas futuras y deduplicación.
    """
    if df.empty:
        return df.copy()

    out = df.copy()

    # Normalización de tipos fecha
    for c in ["fecha", "fecha_extraccion"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    # Normalización de texto
    if "departamento" in out.columns:
        out["departamento"] = out["departamento"].astype(str).str.strip().str.upper()
    if "producto" in out.columns:
        out["producto"] = out["producto"].astype(str).str.strip().str.title()
    if "categoria" in out.columns:
        out["categoria"] = out["categoria"].astype(str).str.strip().str.upper()
    if "fuente" in out.columns:
        out["fuente"] = out["fuente"].astype(str).str.strip().str.lower()

    # Regla de validez: precios no pueden ser negativos
    for c in ["precio_chacra", "precio_mayorista"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
            out.loc[out[c] < 0, c] = np.nan

    # Regla temporal: eliminar fechas futuras
    if "fecha" in out.columns:
        out = out[out["fecha"] <= pd.Timestamp.today().normalize()].copy()

    # Unicidad por llave funcional
    llaves = [c for c in ["fecha", "producto", "categoria", "fuente"] if c in out.columns]
    if llaves:
        out = out.drop_duplicates(subset=llaves, keep="last").copy()

    return out.reset_index(drop=True)


# ── Imputación simple ──────────────────

def imputacion_simple(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputa nulos: mediana para numéricas, moda para categóricas.
    """
    if df.empty:
        return df.copy()

    out      = df.copy()
    num_cols = out.select_dtypes(include=["number"]).columns.tolist()
    cat_cols = out.select_dtypes(include=["object", "category"]).columns.tolist()

    for c in num_cols:
        if out[c].isna().any():
            out[c] = out[c].fillna(out[c].median())

    for c in cat_cols:
        if out[c].isna().any():
            moda = out[c].mode(dropna=True)
            if not moda.empty:
                out[c] = out[c].fillna(moda.iloc[0])

    return out


# ── Eliminación de errores ──────────────

def limpiar_errores(df: pd.DataFrame) -> pd.DataFrame:
    """
    CORREGIDO: Elimina filas solo si faltan datos críticos.
    Evita el borrado total (dropna) que eliminaba el tiempo real.
    """
    # Solo eliminamos si la fila no tiene estos datos vitales para el análisis
    cols_criticas = ["fecha", "producto", "categoria", "precio_mayorista"]
    df_limpio = df.dropna(subset=cols_criticas).copy()
    
    # Aseguramos que los precios no sean negativos (los nulos se permiten temporalmente)
    cols_num = df_limpio.select_dtypes(include="number").columns
    
    # Mantenemos las filas donde todas las columnas numéricas sean >= 0 o NaN
    # Esto salva los datos en tiempo real.
    return df_limpio[~((df_limpio[cols_num] < 0).any(axis=1))]


# ── Filtrado de productos válidos ──────

def filtrar_productos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retiene solo los tres productos monitoreados.
    """
    out = df[df["producto"].isin(PRODUCTOS_VALIDOS)].copy()
    out = out[out["categoria"].isin(CATEGORIAS_VALIDAS)].copy()
    return out.reset_index(drop=True)