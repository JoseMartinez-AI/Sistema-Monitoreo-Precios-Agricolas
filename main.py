# main.py
# Orquestador principal del pipeline de precios.
# Entry point del proyecto: ejecuta extracción (T1) y transformación (T2) respetando la arquitectura modular.

# Uso:
#   python main.py              → extracción + transformación

import argparse
import sys
from pathlib import Path

import pandas as pd

# ── Garantizar que el proyecto sea importable desde cualquier directorio ──────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import (
    PRODUCTOS_DIR, RT_PRODUCTOS_DIR,
    CSV_SALIDA, PARQUET_SALIDA,
)
from src.logger import get_logger

# Extracción (T1)
from src.extract.midagri import extraer_datos_api
from src.extract.emmsa   import extraer_datos_scraping

# Transformación (T2)
from src.transform.cleaner import (
    diagnostico_calidad,
    limpieza_avanzada,
    imputacion_simple,
    limpiar_errores,
    filtrar_productos,
    mostrando_errores,
)
from src.transform.integrator import (
    transformar_datos,
    aplicar_regla_negocio_y_zscore,
    optimizar_memoria,
    guardar_datos,
    imprimir_resumen,
)

logger = get_logger("main")


# ── Carga de CSVs desde disco ──────────

def _cargar_csvs_desde_disco() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee todos los CSV de snapshots históricos y de tiempo real ya generados
    y los devuelve como dos DataFrames separados.

    Retorna: tuple (df_api, df_scraping) ambos con columna origen_dataset.
    """
    # Históricos
    csv_historicos = sorted(PRODUCTOS_DIR.rglob("*.csv"))
    if csv_historicos:
        partes = [pd.read_csv(p) for p in csv_historicos]
        df_api = pd.concat(partes, ignore_index=True)
    else:
        df_api = pd.DataFrame()

    # Tiempo real
    csv_tiempo_real = sorted(RT_PRODUCTOS_DIR.rglob("*.csv"))
    if csv_tiempo_real:
        partes = [pd.read_csv(p) for p in csv_tiempo_real]
        df_scraping = pd.concat(partes, ignore_index=True)
    else:
        df_scraping = pd.DataFrame()

    df_api["origen_dataset"]      = "historico_api"
    df_scraping["origen_dataset"] = "tiempo_real_scraping"

    logger.info(
        "CSVs cargados desde disco: histórico=%s filas | tiempo_real=%s filas",
        len(df_api), len(df_scraping),
    )
    return df_api, df_scraping


# ── FASE T1: Extracción ───────────────────────────────────────────────────────

def fase_extraccion() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ejecuta la extracción de datos (Entregable T1):
      - Histórico: Power BI MIDAGRI (midagri.py)
      - Tiempo real: Web scraping EMMSA (emmsa.py)

    Retorna: tuple (df_api, df_scraping).
    """
    logger.info("=" * 60)
    logger.info("FASE T1 — EXTRACCIÓN")
    logger.info("=" * 60)

    logger.info("Extrayendo histórico desde Power BI MIDAGRI...")
    df_api = extraer_datos_api()
    logger.info("API histórico: %s filas", len(df_api))

    logger.info("Extrayendo tiempo real desde EMMSA...")
    df_scraping = extraer_datos_scraping(max_dias_atras=5)
    logger.info("Scraping EMMSA: %s filas", len(df_scraping))

    if not df_api.empty:
        df_api["origen_dataset"] = "historico_api"
    if not df_scraping.empty:
        df_scraping["origen_dataset"] = "tiempo_real_scraping"

    return df_api, df_scraping


# ── FASE T2: Transformación ───────────────────────────────────────────────────

def fase_transformacion(df_api: pd.DataFrame, df_scraping: pd.DataFrame) -> pd.DataFrame:
    """
    Ejecuta la transformación completa (Entregable T2):
      1. Diagnóstico de calidad
      2. Limpieza avanzada + imputación
      3. Transformación e integración (IBC, brechas)
      4. Concatenación con mis_queridos_errores.csv
      5. Limpieza de errores + filtrado de productos válidos
      6. Regla de negocio + Z-score
      7. Optimización de memoria
      8. Guardado CSV + Parquet en data/processed/

    Parámetros: df_api, df_scraping ya cargados (desde API o disco).
    Retorna: pd.DataFrame final transformado.
    """
    logger.info("=" * 60)
    logger.info("FASE T2 — TRANSFORMACIÓN")
    logger.info("=" * 60)

    # 2.3 Diagnóstico antes de limpieza
    diagnostico_calidad("Histórico API - antes de limpieza", df_api)
    diagnostico_calidad("Tiempo real Scraping - antes de limpieza", df_scraping)

    # 2.4 Limpieza avanzada + 2.5 Imputación
    df_api_limpio      = imputacion_simple(limpieza_avanzada(df_api))
    df_scraping_limpio = imputacion_simple(limpieza_avanzada(df_scraping))

    print(f"\nHistórico API:  {len(df_api):,} → {len(df_api_limpio):,} filas")
    print(f"Tiempo real:    {len(df_scraping):,} → {len(df_scraping_limpio):,} filas")

    # 3. Transformación e integración
    df_transformado = transformar_datos(df_api_limpio, df_scraping_limpio)
    logger.info("Transformación OK. Filas: %s", len(df_transformado))

    # 4. Guardado intermedio CSV + Parquet 
    guardar_datos(df_transformado)
    logger.info("Archivos intermedios guardados en data/processed/")

    # # 5. Cargar y concatenar mis_queridos_errores.csv 
    # errores_path = Path("data/raw/mis_queridos_errores.csv")
    # if errores_path.exists():
    #     df_errores = pd.read_csv(
    #         errores_path,
    #         parse_dates=["fecha", "fecha_extraccion"],
    #         encoding="utf-8",
    #         on_bad_lines="skip",
    #     )
    #     print(f"\nTamaño antes de errores: {len(df_transformado):,}")
    #     print(f"Tamaño del CSV errores:  {len(df_errores):,}")
    #     df_transformado = pd.concat([df_transformado, df_errores], ignore_index=True)
    #     print(f"Tamaño combinado:        {len(df_transformado):,}")

    #     # Reporte de errores 
    #     mostrando_errores(df_transformado)
    # else:
    #     logger.warning("mis_queridos_errores.csv no encontrado en data/raw/")

    # 6. Limpieza de errores + filtrado de productos válidos
    df_limpio   = limpiar_errores(df_transformado)
    df_filtrado = filtrar_productos(df_limpio)
    logger.info("Post-limpieza_errores+filtrado: %s filas", len(df_filtrado))

    # 7. Regla de negocio + Z-score 
    df_transformado, stats_gov = aplicar_regla_negocio_y_zscore(df_filtrado)
    print("\nTema 4 aplicado correctamente")
    print(f"Filas iniciales              : {stats_gov['filas_iniciales']:,}")
    print(f"No cumplen regla negocio     : {stats_gov['no_cumplen_regla_negocio']:,}")
    print(f"Outliers por Z-score (|z|>3) : {stats_gov['outliers_zscore']:,}")
    print(f"Filas finales                : {stats_gov['filas_finales']:,}")

    # 8. Optimización de memoria 
    df_transformado, mem_stats = optimizar_memoria(df_transformado)
    print(f"\nOptimización: {mem_stats['mem_antes_mb']:.3f} MB "
          f"→ {mem_stats['mem_despues_mb']:.3f} MB "
          f"(ahorro {mem_stats['ahorro_pct']:.2f}%)")

    # 9. Guardado final
    resultado = guardar_datos(df_transformado)
    print(f"\nArchivos finales generados:")
    print(f"  CSV     : {resultado['csv']}")
    print(f"  Parquet : {resultado['parquet']}")

    # 10. Resumen final 
    imprimir_resumen(df_transformado)

    return df_transformado


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline de precios: extracción (T1) + transformación (T2)"
    )
    parser.add_argument(
        "--solo-t2",
        action="store_true",
        help="Omite la extracción y usa los CSVs ya descargados en disco.",
    )
    args = parser.parse_args()

    logger.info("Pipeline iniciado")

    if args.solo_t2:
        logger.info("Modo --solo-t2: cargando CSVs desde disco")
        df_api, df_scraping = _cargar_csvs_desde_disco()
    else:
        df_api, df_scraping = fase_extraccion()

    if df_api.empty:
        logger.error("No hay datos históricos disponibles. Abortando.")
        sys.exit(1)

    df_final = fase_transformacion(df_api, df_scraping)
    logger.info("Pipeline completado. Filas finales: %s", len(df_final))


if __name__ == "__main__":
    main()
