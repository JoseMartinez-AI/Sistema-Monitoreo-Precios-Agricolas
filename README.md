# 🌽 Sistema de Monitoreo de Brechas y Detección de Especulación

Pipeline de datos modular en Python para monitorear brechas de comercialización y detectar especulación en el mercado agrícola peruano, usando datos reales de **MIDAGRI** y **EMMSA**.

🔗 **[Ver demo en vivo →](https://sistema-monitoreo-precios-agricolas.streamlit.app/)**

---

## ¿Qué problema resuelve?

Existe una brecha entre lo que recibe el agricultor (**precio chacra**) y lo que paga el mercado (**precio mayorista**). Cuando esa diferencia supera rangos estadísticamente normales, puede indicar especulación por intermediarios.

El sistema calcula dos métricas clave:

```
IBC (%) = ((Precio Mayorista − Precio Chacra) / Precio Chacra) × 100
```
```
|Z-score| > 3  →  Alerta de posible especulación
```

Y responde 5 preguntas de negocio concretas:

| # | Pregunta |
|---|---|
| P1 | ¿Qué producto tiene mayor margen de intermediación por departamento? |
| P2 | ¿Cómo evolucionan los precios en el tiempo por departamento o categoría? |
| P3 | ¿Cuánto sube el precio del campo al mercado por categoría? |
| P4 | ¿Qué categoría presenta mayor variabilidad en el IBC? |
| P5 | ¿Cuándo y dónde se detectó especulación? |

---

## Arquitectura

El proyecto está organizado en 4 capas con responsabilidades separadas:

```
DATA LAYER       →  Extracción desde Power BI MIDAGRI y scraping EMMSA
BUSINESS LAYER   →  Limpieza, IBC, regla de negocio, Z-score
UI LAYER         →  Dashboard interactivo con Streamlit + Plotly
DEVOPS LAYER     →  venv, requirements.txt, .env
```

---

## Productos monitoreados

| Producto | Categorías | Departamentos |
|---|---|---|
| **Papa** | PAPA AMARILLA · PAPA BLANCA · PAPA YUNGAY | Lima · Arequipa · Cusco |
| **Cebolla** | CEBOLLA CABEZA BLANCA · CEBOLLA CABEZA ROJA | Lima · Arequipa · Cusco |
| **Choclo** | CHOCLO PARDO · CHOCLO (TIPO CUSCO) | Lima · Arequipa · Cusco |

---

## Estructura del proyecto

```
Sistema-Monitoreo-Precios-Agricolas/
│
├── app.py                    # Data App Streamlit
├── main.py                   # Orquestador del pipeline
├── requirements.txt
├── .env                      # Variables de entorno (no subir a git)
│
├── data/
│   ├── raw/
│   │   ├── datos_historicos/ # Snapshots semanales MIDAGRI
│   │   ├── datos_tiempo_real/# Precios en tiempo real EMMSA
│   │   ├── arequipa.csv      # Datos de muestra — Papa, Arequipa
│   │   ├── cusco.csv         # Datos de muestra — Choclo, Cusco
│   │   └── lima_extra.csv    # Datos de muestra — Cebolla, Lima
│   └── processed/
│       ├── reporte_precios_mayorista.csv
│       └── reporte_precios.parquet
│
├── logs/
│   ├── pipeline_precios.log
│   └── pipeline_precios_progress.json
│
└── src/
    ├── config.py             # Configuración centralizada
    ├── logger.py
    ├── extract/
    │   ├── api_utils.py      # Sesiones HTTP, retries, progreso persistente
    │   ├── midagri.py        # Descarga histórica Power BI MIDAGRI
    │   └── emmsa.py          # Scraping tiempo real EMMSA
    └── transform/
        ├── cleaner.py        # Limpieza, imputación, filtrado
        └── integrator.py     # IBC, regla de negocio, Z-score, Parquet
```

---

## Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/JoseMartinez-AI/Sistema-Monitoreo-Precios-Agricolas.git
cd Sistema-Monitoreo-Precios-Agricolas

# 2. Crear y activar entorno virtual
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux / Mac

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
# Crear el archivo .env con las URLs y credenciales del proyecto
# (ver la plantilla .env.example como referencia)
```

---

## Ejecución

| Comando | Descripción |
|---|---|
| `streamlit run app.py` | Lanza la Data App (funciona con los CSVs de muestra incluidos) |
| `python main.py` | Pipeline completo: extrae de MIDAGRI + EMMSA y transforma |
| `python main.py --solo-t2` | Solo transformación, usando CSVs ya descargados en disco |

> **Primera ejecución completa:** la descarga histórica desde MIDAGRI puede tardar 15–30 minutos. Las siguientes son instantáneas porque el sistema omite snapshots ya guardados.

---

## Data App

La app permite explorar los datos de forma interactiva sin escribir código:

- **Subir nuevos CSVs** — se validan, limpian e integran automáticamente al consolidado
- **Filtros en sidebar** — por producto, categoría, departamento, rango de IBC y fechas
- **Comparar** precios por departamento o por categoría con un solo click
- **Detectar alertas** de especulación activa (|Z-score| > 3)
- **Descargar** el resultado filtrado como CSV

### Formato de CSV para carga

Los archivos que subas deben tener al menos estas columnas:

| Columna | Ejemplo |
|---|---|
| `fecha` | 2024-06-15 |
| `departamento` | AREQUIPA / LIMA / CUSCO |
| `producto` | Papa / Cebolla / Choclo |
| `categoria` | PAPA AMARILLA / PAPA YUNGAY / CHOCLO PARDO / ... |
| `precio_chacra` | 2.85 |
| `precio_mayorista` | 3.65 |

Las columnas `ibc_pct`, `diferencia_en_soles` y `fuente` se calculan o asignan automáticamente.

---

## Reglas de negocio y gobernanza

| Regla | Criterio |
|---|---|
| Validez de precios | `precio_mayorista ≥ precio_chacra > 0` |
| Fechas | Se eliminan fechas futuras |
| Negativos | Precios negativos se tratan como nulos |
| Duplicados | Se retiene el registro más reciente por llave funcional |
| Imputación | Numéricos → mediana · Categóricos → moda |
| Outliers | \|Z-score\| > 3 por producto → alerta de especulación |

---

## Tecnologías

| Uso | Librería | Versión |
|---|---|---|
| Data App | Streamlit | 1.35.0 |
| Visualización | Plotly | 5.22.0 |
| Datos | Pandas | 2.2.2 |
| Numérico | NumPy | 1.26.4 |
| Serialización | PyArrow | 16.1.0 |
| HTTP / Scraping | Requests + BeautifulSoup4 | 2.32.3 / 4.12.3 |
| Entorno | python-dotenv | 1.0.1 |

---

## Autor

**José Martínez - Luis Cámaco - Lili Rojas** — Lenguaje de Ciencia de Datos II · CIBERTEC  
Proyecto integrador — Extracción, transformación, detección de anomalías y visualización sobre datos reales del mercado agrícola peruano.