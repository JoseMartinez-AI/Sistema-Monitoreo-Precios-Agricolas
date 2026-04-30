# app.py
# Entry point de la Data App con Streamlit.
# Arquitectura en 3 capas:
#   - DATA LAYER    : carga del Parquet consolidado + CSVs adicionales subidos
#   - BUSINESS LAYER: limpieza, validación, fusión e IBC de los nuevos CSVs
#   - UI LAYER      : sidebar con filtros + KPIs + gráficos que responden preguntas de negocio
#
# Ejecutar con:
#   streamlit run app.py
#


import sys
from pathlib import Path
import io

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Garantizar importaciones del proyecto ─────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import (
    PARQUET_SALIDA, CSV_SALIDA, PROCESSED_DIR,
    PRODUCTOS_VALIDOS, CATEGORIAS_VALIDAS,
    DEPARTAMENTOS_VALIDOS, CATEGORIAS_POR_PRODUCTO,
)
from src.transform.cleaner import limpieza_avanzada, imputacion_simple, filtrar_productos
from src.transform.integrator import aplicar_regla_negocio_y_zscore, guardar_datos

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTES DE LA APP
# ══════════════════════════════════════════════════════════════════════════════
APP_TITULO    = "Sistema de Monitoreo de Brechas"
APP_SUBTITULO = "Detección de Especulación de precios en la Canasta Básica Peruana · CIBERTEC"
COLS_REQUERIDAS = {
    "fecha", "departamento", "precio_chacra",
    "precio_mayorista", "producto", "categoria",
}


# ══════════════════════════════════════════════════════════════════════════════
# CAPA DE DATOS  (data_layer)
# Responsabilidad: leer fuentes y retornar datos crudos
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def cargar_consolidado() -> pd.DataFrame:
    """
    Carga el Parquet consolidado generado por el pipeline T1+T2.
    Si no existe, intenta el CSV; si tampoco, retorna DataFrame vacío.

    Retorna: pd.DataFrame con datos históricos limpios y gobernados.
    """
    if PARQUET_SALIDA.exists():
        df = pd.read_parquet(PARQUET_SALIDA)
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        return df
    if CSV_SALIDA.exists():
        df = pd.read_csv(CSV_SALIDA, parse_dates=["fecha"])
        return df
    return pd.DataFrame()


def leer_csv_subido(archivo) -> pd.DataFrame:
    """
    Lee un archivo CSV subido por st.file_uploader y retorna el DataFrame crudo.

    Parámetros: archivo — objeto UploadedFile de Streamlit.
    Retorna: pd.DataFrame crudo o DataFrame vacío si falla la lectura.
    """
    try:
        contenido = archivo.read()
        df = pd.read_csv(
            io.BytesIO(contenido),
            encoding="utf-8-sig",
            on_bad_lines="skip",
        )
        return df
    except Exception as e:
        st.error(f"[ERROR] No se pudo leer '{archivo.name}': {e}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# CAPA DE LÓGICA  (business_layer)
# Responsabilidad: validar, limpiar, fusionar CSVs nuevos e IBC
# ══════════════════════════════════════════════════════════════════════════════

def _calcular_ibc(df: pd.DataFrame) -> pd.DataFrame:
    """Recalcula ibc_pct para filas donde no está presente o es NaN."""
    mask = df["precio_chacra"] > 0
    df.loc[mask, "ibc_pct"] = (
        (df.loc[mask, "precio_mayorista"] - df.loc[mask, "precio_chacra"])
        / df.loc[mask, "precio_chacra"]
    ) * 100
    return df


def validar_y_limpiar_csv(df_crudo: pd.DataFrame, nombre_archivo: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Valida columnas requeridas, aplica limpieza, imputación y filtra productos
    según las reglas del proyecto.

    Parámetros:
        df_crudo       : DataFrame recién leído del CSV subido.
        nombre_archivo : nombre del archivo (para mensajes de error).
    Retorna: tuple (DataFrame limpio, lista de advertencias).
    """
    advertencias = []

    # 1. Verificar columnas mínimas
    cols_faltantes = COLS_REQUERIDAS - set(df_crudo.columns)
    if cols_faltantes:
        advertencias.append(f"❌ Columnas faltantes: {cols_faltantes}")
        return pd.DataFrame(), advertencias

    # 2. Limpieza avanzada + imputación 
    df = imputacion_simple(limpieza_avanzada(df_crudo))

    # 3. Verificar que tenga al menos un producto válido
    if "producto" in df.columns:
        productos_en_csv = set(df["producto"].unique())
        validos = productos_en_csv & set(PRODUCTOS_VALIDOS)
        if not validos:
            advertencias.append(
                f"⚠️ Ningún producto reconocido. "
                f"Encontrados: {productos_en_csv}. "
                f"Válidos: {set(PRODUCTOS_VALIDOS)}"
            )
            return pd.DataFrame(), advertencias

    # 4. Filtrar productos y categorías válidas (ampliadas en config.py)
    n_antes = len(df)
    df = filtrar_productos(df)
    n_despues = len(df)
    eliminados = n_antes - n_despues
    if eliminados > 0:
        advertencias.append(
            f"{eliminados} filas eliminadas por producto/categoría no válidos."
        )

    if df.empty:
        advertencias.append("Sin filas válidas tras filtrado de productos.")
        return pd.DataFrame(), advertencias

    # 5. Recalcular IBC si falta
    if "ibc_pct" not in df.columns:
        df["ibc_pct"] = np.nan
    df = _calcular_ibc(df)

    # 6. Asegurar columna fuente
    if "fuente" not in df.columns:
        df["fuente"] = "manual_upload"

    advertencias.append(f"✅ {len(df)} filas válidas listas para integrar.")
    return df, advertencias


def fusionar_con_consolidado(df_base: pd.DataFrame, df_nuevo: pd.DataFrame) -> pd.DataFrame:
    """
    Fusiona el dataset consolidado con los datos del nuevo CSV,
    elimina duplicados por llave funcional y aplica la regla de negocio + Z-score.

    Parámetros:
        df_base  : DataFrame consolidado cargado del Parquet.
        df_nuevo : DataFrame limpio del CSV recién subido.
    Retorna: pd.DataFrame fusionado y gobernado.
    """
    if df_base.empty:
        df_fusionado = df_nuevo.copy()
    else:
        # Alinear columnas comunes
        cols_comunes = [c for c in df_base.columns if c in df_nuevo.columns]
        df_fusionado = pd.concat(
            [df_base[cols_comunes], df_nuevo[cols_comunes]],
            ignore_index=True,
        )

    # Deduplicar
    llaves = [c for c in ["fecha", "producto", "categoria", "departamento", "fuente"]
              if c in df_fusionado.columns]
    df_fusionado = df_fusionado.drop_duplicates(subset=llaves, keep="last")

    # Aplicar regla de negocio + Z-score
    df_gobernado, _ = aplicar_regla_negocio_y_zscore(df_fusionado)
    df_gobernado["fecha"] = pd.to_datetime(df_gobernado["fecha"], errors="coerce")
    return df_gobernado.sort_values(["producto", "fecha"]).reset_index(drop=True)


def calcular_kpis(df: pd.DataFrame) -> dict:
    """
    Calcula KPIs globales del dataset activo para el header del dashboard.

    Parámetros: df (pd.DataFrame) dataset filtrado activo.
    Retorna: dict con métricas clave.
    """
    if df.empty:
        return {}
    return {
        "registros"       : len(df),
        "productos"       : int(df["producto"].nunique()),
        "departamentos"   : int(df["departamento"].nunique()) if "departamento" in df.columns else 0,
        "ibc_promedio"    : round(float(df["ibc_pct"].mean()), 2) if "ibc_pct" in df.columns else 0.0,
        "precio_chacra"   : round(float(df["precio_chacra"].mean()), 2),
        "precio_mayorista": round(float(df["precio_mayorista"].mean()), 2),
        "alertas"         : int(df["flag_outlier_zscore"].sum()) if "flag_outlier_zscore" in df.columns else 0,
        "fecha_min"       : str(df["fecha"].min().date()) if "fecha" in df.columns else "—",
        "fecha_max"       : str(df["fecha"].max().date()) if "fecha" in df.columns else "—",
    }


def aplicar_filtros(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    """
    Aplica los filtros del sidebar sobre el DataFrame completo.

    Parámetros:
        df      : DataFrame consolidado (puede incluir datos nuevos).
        filtros : dict con claves productos, categorias, departamentos,
                  rango_ibc, rango_fechas, solo_alertas.
    Retorna: pd.DataFrame filtrado.
    """
    out = df.copy()

    if filtros["productos"]:
        out = out[out["producto"].astype(str).isin(filtros["productos"])]

    if filtros["categorias"]:
        out = out[out["categoria"].astype(str).isin(filtros["categorias"])]

    if "departamento" in out.columns and filtros["departamentos"]:
        out = out[out["departamento"].astype(str).isin(filtros["departamentos"])]

    if "ibc_pct" in out.columns:
        out = out[
            (out["ibc_pct"] >= filtros["rango_ibc"][0]) &
            (out["ibc_pct"] <= filtros["rango_ibc"][1])
        ]

    if "fecha" in out.columns and filtros.get("rango_fechas"):
        f_ini = pd.Timestamp(filtros["rango_fechas"][0])
        f_fin = pd.Timestamp(filtros["rango_fechas"][1])
        out = out[(out["fecha"] >= f_ini) & (out["fecha"] <= f_fin)]

    if filtros["solo_alertas"] and "flag_outlier_zscore" in out.columns:
        out = out[out["flag_outlier_zscore"] == True]

    return out.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# CAPA DE PRESENTACIÓN  (ui_layer)
# Responsabilidad: renderizar la interfaz con Streamlit
# ══════════════════════════════════════════════════════════════════════════════

def render_config_pagina() -> None:
    """Configura la página Streamlit: título, icono y layout wide."""
    st.set_page_config(
        page_title=APP_TITULO,
        page_icon="🌽",
        layout="wide",
    )


def render_header(df_total: pd.DataFrame, df_filtrado: pd.DataFrame) -> None:
    """
    Renderiza el encabezado con título y KPIs comparativos (total vs filtrado).

    Parámetros: df_total (DataFrame completo), df_filtrado (DataFrame con filtros).
    Retorna: None — efecto visual directo en Streamlit.
    """
    st.title(f"🌽 {APP_TITULO}")
    st.caption(APP_SUBTITULO)

    kpis_t = calcular_kpis(df_total)
    kpis_f = calcular_kpis(df_filtrado)

    if not kpis_t:
        st.warning("Sin datos disponibles. Corre primero el pipeline (main.py) o sube un CSV.")
        return

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("📋 Registros",
              f"{kpis_f['registros']:,}",
              delta=f"{kpis_f['registros'] - kpis_t['registros']:+,} vs total")
    c2.metric("🥔 Productos",    kpis_f["productos"])
    c3.metric("📍 Departamentos", kpis_f["departamentos"])
    c4.metric("📈 IBC Promedio (%)",
              f"{kpis_f['ibc_promedio']:.1f}",
              delta=f"{kpis_f['ibc_promedio'] - kpis_t['ibc_promedio']:+.1f}")
    c5.metric("💰 P. Chacra Prom.",    f"S/ {kpis_f['precio_chacra']:.2f}")
    c6.metric("🏪 P. Mayorista Prom.", f"S/ {kpis_f['precio_mayorista']:.2f}")
    st.divider()


def render_sidebar(df: pd.DataFrame) -> dict:
    """
    Renderiza todos los controles interactivos en el sidebar.
    Implementa: multiselect, slider de rango, date_input y checkbox (Tema 5 §3.1.2).

    Parámetros: df (pd.DataFrame) dataset completo para calcular rangos dinámicos.
    Retorna: dict con los valores actuales de todos los filtros.
    """
    st.sidebar.title("🎛️ Panel de Control")
    st.sidebar.caption("Filtra para explorar y comparar")
    st.sidebar.divider()

    # ── Filtro: productos (multiselect) ──────────────────────────────────────
    productos_disp = sorted(df["producto"].astype(str).unique().tolist()) if not df.empty else PRODUCTOS_VALIDOS
    productos_sel = st.sidebar.multiselect(
        "🥔 Producto(s):",
        options=productos_disp,
        default=productos_disp,
    )

    # ── Filtro: categorías (multiselect dinámico según productos elegidos) ───
    if productos_sel and not df.empty:
        cats_disp = sorted(
            df[df["producto"].astype(str).isin(productos_sel)]["categoria"]
            .astype(str).unique().tolist()
        )
    else:
        cats_disp = CATEGORIAS_VALIDAS
    categorias_sel = st.sidebar.multiselect(
        "🏷️ Categoría(s):",
        options=cats_disp,
        default=cats_disp,
    )

    # ── Filtro: departamentos (multiselect) ──────────────────────────────────
    if "departamento" in df.columns and not df.empty:
        deptos_disp = sorted(df["departamento"].astype(str).unique().tolist())
    else:
        deptos_disp = DEPARTAMENTOS_VALIDOS
    deptos_sel = st.sidebar.multiselect(
        "📍 Departamento(s):",
        options=deptos_disp,
        default=deptos_disp,
    )

    st.sidebar.divider()

    # ── Filtro: rango IBC (slider) ────────────────────────────────────────────
    if "ibc_pct" in df.columns and not df.empty:
        ibc_min = float(df["ibc_pct"].min())
        ibc_max = float(df["ibc_pct"].max())
    else:
        ibc_min, ibc_max = 0.0, 100.0
    rango_ibc = st.sidebar.slider(
        "📊 Rango IBC (%):",
        min_value=round(ibc_min, 1),
        max_value=round(ibc_max, 1),
        value=(round(ibc_min, 1), round(ibc_max, 1)),
        step=0.5,
    )

    # ── Filtro: rango de fechas (date_input) ─────────────────────────────────
    if "fecha" in df.columns and not df.empty:
        f_min = df["fecha"].min().date()
        f_max = df["fecha"].max().date()
    else:
        import datetime
        f_min = datetime.date(2021, 1, 1)
        f_max = datetime.date.today()

    rango_fechas = st.sidebar.date_input(
        "📅 Rango de fechas:",
        value=(f_min, f_max),
        min_value=f_min,
        max_value=f_max,
    )

    st.sidebar.divider()

    # ── Filtro: solo alertas de especulación (checkbox) ───────────────────────
    solo_alertas = st.sidebar.checkbox("🚨 Solo alertas de especulación (|z|>3)", value=False)

    return {
        "productos"    : productos_sel,
        "categorias"   : categorias_sel,
        "departamentos": deptos_sel,
        "rango_ibc"    : rango_ibc,
        "rango_fechas" : rango_fechas if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2
                         else (f_min, f_max),
        "solo_alertas" : solo_alertas,
    }


def render_carga_csv(df_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Renderiza la sección de carga de nuevos CSVs (st.file_uploader).
    Valida, limpia y fusiona cada archivo con el consolidado.
    Implementa integración de fuentes dinámicas (Tema 5 §3.1.3).

    Parámetros: df_consolidado (pd.DataFrame) dataset base ya cargado.
    Retorna: pd.DataFrame consolidado actualizado (con los nuevos CSVs incluidos).
    """
    st.subheader("📂 Integrar nuevas fuentes de datos")

    with st.expander("Formato requerido del CSV", expanded=False):
        st.markdown("""
        El CSV debe contener al menos estas columnas:

        | Columna | Ejemplo |
        |---|---|
        | `fecha` | 2024-06-15 |
        | `departamento` | AREQUIPA / LIMA / CUSCO |
        | `producto` | Papa / Cebolla / Choclo |
        | `categoria` | PAPA AMARILLA / PAPA BLANCA / PAPA YUNGAY / CEBOLLA CABEZA BLANCA / CEBOLLA CABEZA ROJA / CHOCLO PARDO / CHOCLO (TIPO CUSCO) |
        | `precio_chacra` | 2.85 |
        | `precio_mayorista` | 3.65 |

        Columnas opcionales que se calculan automáticamente si faltan:
        `diferencia_en_soles`, `porcentaje`, `ibc_pct`, `fuente`
        """)

    archivos = st.file_uploader(
        "📎 Sube uno o varios archivos CSV:",
        type=["csv"],
        accept_multiple_files=True,
        help="Los archivos pasan por limpieza y validación antes de integrarse.",
    )

    if not archivos:
        return df_consolidado

    df_acumulado = df_consolidado.copy()

    for archivo in archivos:
        st.markdown(f"**Procesando: `{archivo.name}`**")
        df_crudo   = leer_csv_subido(archivo)
        df_limpio, advertencias = validar_y_limpiar_csv(df_crudo, archivo.name)

        for msg in advertencias:
            if msg.startswith("✅"):
                st.success(msg)
            elif msg.startswith("⚠️"):
                st.warning(msg)
            else:
                st.error(msg)

        if not df_limpio.empty:
            n_antes    = len(df_acumulado)
            df_acumulado = fusionar_con_consolidado(df_acumulado, df_limpio)
            nuevas_filas = len(df_acumulado) - n_antes
            st.info(f"➕ `{archivo.name}` integrado. Filas nuevas añadidas: **{nuevas_filas}**")

    # Persistir el consolidado actualizado en disco (Parquet + CSV)
    if len(df_acumulado) > len(df_consolidado):
        with st.spinner("Guardando consolidado actualizado en disco..."):
            guardar_datos(df_acumulado)
        st.success("💾 Consolidado guardado en `data/processed/`")
        st.cache_data.clear()   # Invalidar caché para que cargar_consolidado() use la versión nueva

    return df_acumulado


# ── Gráficos que responden preguntas de negocio ───────────────────────────────

def render_grafico_ibc_por_producto_departamento(df: pd.DataFrame) -> None:
    """
    Pregunta de negocio: ¿Cuál producto tiene mayor margen de intermediación
    por departamento?

    Muestra gráfico de barras agrupadas: IBC promedio × producto × departamento.
    """
    st.subheader("P1: ¿Qué producto tiene mayor margen de intermediación por departamento?")
    if df.empty or "departamento" not in df.columns:
        st.info("Sin datos suficientes para este gráfico.")
        return

    df_g = (
        df.groupby(["producto", "departamento"], observed=True)["ibc_pct"]
        .mean().round(2).reset_index()
        .rename(columns={"ibc_pct": "IBC Promedio (%)"})
    )
    fig = px.bar(
        df_g, x="producto", y="IBC Promedio (%)",
        color="departamento", barmode="group",
        text_auto=".1f",
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={"producto": "Producto", "departamento": "Departamento"},
    )
    fig.update_layout(legend_title="Departamento", xaxis_title="Producto",
                      yaxis_title="IBC Promedio (%)", height=400)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("IBC = ((Precio Mayorista − Precio Chacra) / Precio Chacra) × 100. "
               "Valores altos indican mayor margen de intermediación.")


def render_grafico_evolucion_precios(df: pd.DataFrame, modo_comparacion: str) -> None:
    """
    Pregunta de negocio: ¿Cómo evolucionan los precios en el tiempo
    según departamento o categoría?

    Parámetros:
        df               : DataFrame filtrado.
        modo_comparacion : 'Por Departamento' o 'Por Categoría'.
    """
    st.subheader("P2: ¿Cómo evolucionan los precios mayoristas en el tiempo?")
    if df.empty or "fecha" not in df.columns:
        st.info("Sin datos para serie temporal.")
        return

    color_col = "departamento" if modo_comparacion == "Por Departamento" else "categoria"
    if color_col not in df.columns:
        st.info(f"Columna '{color_col}' no disponible.")
        return

    df_ts = (
        df.groupby([pd.Grouper(key="fecha", freq="MS"), color_col], observed=True)
        ["precio_mayorista"].mean().round(2).reset_index()
    )
    fig = px.line(
        df_ts, x="fecha", y="precio_mayorista",
        color=color_col, markers=True,
        color_discrete_sequence=px.colors.qualitative.Plotly,
        labels={"fecha": "Fecha", "precio_mayorista": "Precio Mayorista (S/)",
                color_col: modo_comparacion.split()[-1]},
    )
    fig.update_layout(height=400, xaxis_title="Mes", yaxis_title="Precio Mayorista (S/)")
    st.plotly_chart(fig, use_container_width=True)


def render_grafico_comparacion_precios(df: pd.DataFrame) -> None:
    """
    Pregunta de negocio: ¿Cuál es la diferencia entre precio de chacra
    y precio mayorista por categoría?
    """
    st.subheader("P3: ¿Cuánto sube el precio del campo al mercado por categoría?")
    if df.empty:
        st.info("Sin datos para este gráfico.")
        return

    df_g = (
        df.groupby("categoria", observed=True)
        .agg(precio_chacra=("precio_chacra", "mean"),
             precio_mayorista=("precio_mayorista", "mean"))
        .round(2).reset_index()
        .sort_values("precio_mayorista", ascending=False)
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Precio Chacra",
        x=df_g["categoria"], y=df_g["precio_chacra"],
        marker_color="#2196F3", text=df_g["precio_chacra"].apply(lambda v: f"S/ {v:.2f}"),
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        name="Precio Mayorista",
        x=df_g["categoria"], y=df_g["precio_mayorista"],
        marker_color="#FF5722", text=df_g["precio_mayorista"].apply(lambda v: f"S/ {v:.2f}"),
        textposition="outside",
    ))
    fig.update_layout(
        barmode="group", height=420,
        xaxis_title="Categoría", yaxis_title="Precio Promedio (S/)",
        legend_title="Tipo de Precio",
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Un mayor gap entre barras indica mayor margen de intermediación en esa categoría.")


def render_grafico_ibc_por_categoria(df: pd.DataFrame) -> None:
    """
    Pregunta de negocio: ¿Qué categoría presenta mayor especulación potencial?
    Muestra boxplot de IBC por categoría para ver distribución y outliers.
    """
    st.subheader("P4: ¿Qué categoría presenta mayor variabilidad en el IBC?")
    if df.empty or "ibc_pct" not in df.columns:
        st.info("Sin datos para este gráfico.")
        return

    fig = px.box(
        df, x="categoria", y="ibc_pct",
        color="producto",
        color_discrete_sequence=px.colors.qualitative.Safe,
        labels={"categoria": "Categoría", "ibc_pct": "IBC (%)", "producto": "Producto"},
        points="outliers",
    )
    fig.update_layout(height=420, xaxis_tickangle=-30, xaxis_title="Categoría",
                      yaxis_title="IBC (%)")
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Puntos fuera de las cajas son outliers detectados por IQR. "
               "Cajas más altas indican mayor variabilidad en el margen.")


def render_alertas_especulacion(df: pd.DataFrame) -> None:
    """
    Pregunta de negocio: ¿Cuándo y dónde se detectó especulación (|Z-score| > 3)?
    Muestra tabla y scatter de alertas activas.
    """
    st.subheader("🚨 P5: ¿Cuándo y dónde se detectó especulación?")
    col_flag = "flag_outlier_zscore"
    if df.empty or col_flag not in df.columns:
        st.info("Sin columna de alertas en los datos. Verifica que el pipeline T2 haya corrido.")
        return

    df_alertas = df[df[col_flag] == True].copy()
    total_alertas = len(df_alertas)
    st.metric("Total de alertas activas", total_alertas)

    if df_alertas.empty:
        st.success("✅ No se detectaron alertas de especulación con los filtros actuales.")
        return

    cols_disp = [c for c in ["fecha", "departamento", "producto", "categoria",
                              "precio_chacra", "precio_mayorista", "ibc_pct",
                              "zscore_precio_mayorista"] if c in df_alertas.columns]
    st.dataframe(
        df_alertas[cols_disp].sort_values("zscore_precio_mayorista", ascending=False)
        .head(50).reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )

    if "fecha" in df_alertas.columns and "ibc_pct" in df_alertas.columns:
        fig = px.scatter(
            df_alertas, x="fecha", y="ibc_pct",
            color="producto" if "producto" in df_alertas.columns else None,
            size="ibc_pct", hover_data=cols_disp,
            color_discrete_sequence=px.colors.qualitative.Alphabet,
            labels={"fecha": "Fecha", "ibc_pct": "IBC (%)"},
        )
        fig.update_layout(height=350, title="Alertas de especulación en el tiempo")
        st.plotly_chart(fig, use_container_width=True)


def render_tabla_detalle(df: pd.DataFrame) -> None:
    """
    Muestra el DataFrame filtrado como tabla interactiva con búsqueda por texto.
    Parámetros: df (pd.DataFrame) dataset filtrado.
    """
    st.subheader("Tabla de datos filtrados")
    busqueda = st.text_input("Buscar en tabla (producto, categoría, departamento):",
                             placeholder="Ej: Papa, AREQUIPA, CHOCLO PARDO")
    df_vis = df.copy()
    if busqueda:
        mask = (
            df_vis.astype(str)
            .apply(lambda col: col.str.contains(busqueda, case=False, na=False))
            .any(axis=1)
        )
        df_vis = df_vis[mask]

    st.dataframe(df_vis.reset_index(drop=True), use_container_width=True, hide_index=True)
    st.caption(f"Mostrando **{len(df_vis)}** registro(s) de {len(df)} filtrados.")

    # Descarga del filtrado
    csv_bytes = df_vis.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "⬇️ Descargar CSV filtrado",
        data=csv_bytes,
        file_name="precios_filtrado.csv",
        mime="text/csv",
    )


# ══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA PRINCIPAL
# Flujo Streamlit: Datos → Lógica → Presentación
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """
    Orquesta el flujo completo de la Data App.
    Flujo: DATA LAYER → BUSINESS LAYER → UI LAYER
    """
    # ── Config de página ──────────────────────────────────────────────────────
    render_config_pagina()

    # ── DATA LAYER: carga del consolidado desde Parquet/CSV ──────────────────
    with st.spinner("Cargando datos consolidados..."):
        df_consolidado = cargar_consolidado()

    # ── UI LAYER: sección de carga de nuevos CSVs (integración de fuentes) ───
    # (Tema 5 §3.1.3 — integración con fuentes de datos dinámicas)
    df_activo = render_carga_csv(df_consolidado)

    # ── UI LAYER: sidebar con filtros interactivos ────────────────────────────
    # (Tema 5 §3.1.2 — componentes interactivos)
    if df_activo.empty:
        st.warning(
            "No hay datos disponibles. "
            "Ejecuta `python main.py` para generar el consolidado, "
            "o sube un CSV directamente arriba."
        )
        st.stop()

    filtros = render_sidebar(df_activo)

    # ── BUSINESS LAYER: aplicar filtros ──────────────────────────────────────
    df_filtrado = aplicar_filtros(df_activo, filtros)

    # ── UI LAYER: header con KPIs ─────────────────────────────────────────────
    render_header(df_activo, df_filtrado)

    if df_filtrado.empty:
        st.warning("Sin resultados con los filtros actuales. Ajusta el Panel de Control.")
        st.stop()

    # ── UI LAYER: modo de comparación (radio button) ──────────────────────────
    modo = st.radio(
        "🔀 Comparar evolución de precios:",
        options=["Por Departamento", "Por Categoría"],
        horizontal=True,
    )
    st.divider()

    # ── UI LAYER: gráficos que responden preguntas de negocio ─────────────────
    # P1 y P2 en dos columnas
    col_izq, col_der = st.columns(2)
    with col_izq:
        render_grafico_ibc_por_producto_departamento(df_filtrado)
    with col_der:
        render_grafico_evolucion_precios(df_filtrado, modo)

    st.divider()

    # P3 y P4 en dos columnas
    col_izq2, col_der2 = st.columns(2)
    with col_izq2:
        render_grafico_comparacion_precios(df_filtrado)
    with col_der2:
        render_grafico_ibc_por_categoria(df_filtrado)

    st.divider()

    # P5: alertas (ancho completo)
    render_alertas_especulacion(df_filtrado)

    st.divider()

    # Tabla de detalle con búsqueda y descarga
    render_tabla_detalle(df_filtrado)

    # Pie de página
    st.divider()
    st.caption(
        " Sistema de Monitoreo de Brechas · Canasta Básica Peruana · "
        "Lenguaje de Ciencia de Datos II — CIBERTEC · "
        "Arquitectura: Data Layer → Business Layer → UI Layer"
    )


if __name__ == "__main__":
    main()
