"""Dashboard interactivo de empadronadores y municipalidades del Perú."""
from __future__ import annotations

import json
import unicodedata
from io import BytesIO
from pathlib import Path

import folium
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from streamlit_folium import st_folium

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
URL_MUNIS = (
    "https://docs.google.com/spreadsheets/d/"
    "1KjGs3bX6tFOq797VMFp7unbB_NguUaSGcirpsQZ8f_w/export?format=csv&gid=0"
)
URL_EMP = (
    "https://docs.google.com/spreadsheets/d/"
    "1Si4eEIQsxdo7J_1KT_D72cv5vrQLvEHU/export?format=csv&gid=1313518900"
)

# Base de datos SIG local (descargada en BD_SIG/) con fallback a URLs públicas.
BD_SIG = Path(__file__).parent / "BD_SIG"
GEO_REGIONS_LOCAL = BD_SIG / "peru_departamental.geojson"
GEO_PROVINCES_LOCAL = BD_SIG / "peru_provincial.geojson"
GEO_DISTRICTS_LOCAL = BD_SIG / "peru_distrital.geojson"

URL_GEO_REGIONS = (
    "https://raw.githubusercontent.com/juaneladio/peru-geojson/master/"
    "peru_departamental_simple.geojson"
)
URL_GEO_PROVINCES = (
    "https://raw.githubusercontent.com/juaneladio/peru-geojson/master/"
    "peru_provincial_simple.geojson"
)
URL_GEO_DISTRICTS = (
    "https://raw.githubusercontent.com/juaneladio/peru-geojson/master/"
    "peru_distrital_simple.geojson"
)

ESTADO_YA = {"HABILITADO", "CREADO"}
TODAS = "TODAS"
TODOS = "TODOS"

ENTIDAD_MUNI_KEY = "MUNICIPALIDAD"  # se usa para detectar si el filtro incluye munis

# ---------------------------------------------------------------------------
# Utilidades de normalización
# ---------------------------------------------------------------------------
def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )


def norm_key(s) -> str:
    if pd.isna(s):
        return ""
    s = str(s).replace("�", "").strip()
    return _strip_accents(s).upper()


def title_clean(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip().title()


def upper_clean(s) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip().upper()


def find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Busca una columna por nombre (insensible a mayúsculas/acentos/espacios)."""
    norm_map = {norm_key(c): c for c in df.columns}
    for cand in candidates:
        k = norm_key(cand)
        if k in norm_map:
            return norm_map[k]
    # fallback: contains
    for cand in candidates:
        k = norm_key(cand)
        for nc, orig in norm_map.items():
            if k in nc:
                return orig
    return None


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner="Cargando municipalidades…")
def load_munis() -> pd.DataFrame:
    # El Archivo 1 tiene un layout multi-bloque: la primera fila contiene
    # cabeceras internas y los datos del universo global están en las 3
    # primeras columnas (Distrito | Provincia | Departamento). Las demás
    # columnas son listas paralelas de otros sub-conjuntos que NO se usan.
    df = pd.read_csv(
        URL_MUNIS,
        header=None,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        encoding="utf-8",
    )
    out = df.iloc[1:, :3].copy()
    out.columns = ["Distrito", "Provincia", "Departamento"]
    # Drop any residual header-like row (e.g. "Distrito" repetido)
    out = out[out["Distrito"].str.strip().str.lower() != "distrito"]
    out["Distrito"] = out["Distrito"].map(title_clean)
    out["Provincia"] = out["Provincia"].map(title_clean)
    out["Departamento"] = out["Departamento"].map(title_clean)
    out = out[
        (out["Distrito"] != "") & (out["Provincia"] != "") & (out["Departamento"] != "")
    ].drop_duplicates(subset=["Distrito", "Provincia", "Departamento"])
    out["k_dist"] = out["Distrito"].map(norm_key)
    out["k_prov"] = out["Provincia"].map(norm_key)
    out["k_dep"] = out["Departamento"].map(norm_key)
    return out.reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner="Cargando empadronadores…")
def load_emp() -> pd.DataFrame:
    df = pd.read_csv(
        URL_EMP, dtype=str, keep_default_na=False, na_values=[""], encoding="utf-8"
    )
    df.columns = [c.strip() for c in df.columns]

    cols = {
        "ENTIDAD": find_col(df, "ENTIDAD"),
        "SEDE": find_col(df, "SEDE DONDE EMPADRONA", "SEDE"),
        "DISTRITO": find_col(df, "DISTRITO"),
        "PROVINCIA": find_col(df, "PROVINCIA"),
        "REGION": find_col(df, "REGION", "REGIÓN", "DEPARTAMENTO"),
        "DNI": find_col(df, "DNI"),
        "NOMBRE": find_col(df, "NOMBRE", "NOMBRES"),
        "CREA_USUARIO": find_col(df, "CREA USUARIO"),
        "CONDICION_CE": find_col(df, "CONDICION CE", "CONDICIÓN CE"),
        "SITUACION_2026": find_col(df, "SITUACION 2026", "SITUACIÓN 2026"),
        "BLOQUEADOS": find_col(df, "BLOQUEADOS", "BLOQUEADO"),
    }
    missing = [k for k, v in cols.items() if v is None]
    if missing:
        raise ValueError(
            f"Columnas faltantes en Archivo 2: {missing}. Disponibles: {list(df.columns)}"
        )

    out = pd.DataFrame(
        {
            "Entidad": df[cols["ENTIDAD"]].map(upper_clean),
            "Sede": df[cols["SEDE"]].map(lambda s: str(s).strip() if pd.notna(s) else ""),
            "Distrito": df[cols["DISTRITO"]].map(title_clean),
            "Provincia": df[cols["PROVINCIA"]].map(title_clean),
            "Region": df[cols["REGION"]].map(title_clean),
            "DNI": df[cols["DNI"]].map(lambda s: str(s).strip() if pd.notna(s) else ""),
            "Nombre": df[cols["NOMBRE"]].map(lambda s: str(s).strip() if pd.notna(s) else ""),
            "CreaUsuario": df[cols["CREA_USUARIO"]].map(upper_clean),
            "CondicionCE": df[cols["CONDICION_CE"]].map(upper_clean),
            "Situacion2026": df[cols["SITUACION_2026"]].map(upper_clean),
            "Bloqueados": df[cols["BLOQUEADOS"]].map(upper_clean),
        }
    )

    # Limpieza: descartar filas totalmente vacías (sin entidad ni DNI ni nombre)
    out = out[
        (out["Entidad"] != "")
        | (out["DNI"].astype(str).str.strip() != "")
        | (out["Nombre"].astype(str).str.strip() != "")
    ].reset_index(drop=True)

    # Estado lógico:
    #   YA   = Situacion2026 in {HABILITADO, CREADO}
    #   PEND = Situacion2026 vacío/null  (y NO bloqueado)
    #   BLOQ = Bloqueados con cualquier valor (prioridad)
    bloq = out["Bloqueados"].fillna("") != ""
    ya = out["Situacion2026"].isin(ESTADO_YA) & ~bloq
    pendiente = (out["Situacion2026"].fillna("") == "") & ~bloq
    out["Estado"] = "OTRO"
    out.loc[bloq, "Estado"] = "BLOQUEADO"
    out.loc[ya, "Estado"] = "YA"
    out.loc[pendiente, "Estado"] = "PENDIENTE"

    # Claves de merge
    out["k_dist"] = out["Distrito"].map(norm_key)
    out["k_prov"] = out["Provincia"].map(norm_key)
    out["k_dep"] = out["Region"].map(norm_key)
    return out.reset_index(drop=True)


@st.cache_data(ttl=86400, show_spinner="Cargando GeoJSON…")
def load_geojson(url: str, local: str | None = None) -> dict:
    """Carga un GeoJSON; prioriza ruta local de BD_SIG/ si existe."""
    if local:
        p = Path(local)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Centroides distritales y de regiones
# ---------------------------------------------------------------------------
def _collect_points(coords) -> list[tuple[float, float]]:
    pts: list[tuple[float, float]] = []

    def walk(o):
        if isinstance(o, list):
            if (
                len(o) >= 2
                and isinstance(o[0], (int, float))
                and isinstance(o[1], (int, float))
            ):
                pts.append((o[0], o[1]))
            else:
                for c in o:
                    walk(c)

    walk(coords)
    return pts


def _centroid(coords) -> tuple[float, float] | None:
    pts = _collect_points(coords)
    if not pts:
        return None
    lng = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    return lat, lng


DIST_CENT_COLS = ["k_dist", "k_prov", "k_dep", "lat", "lng"]
REG_CENT_COLS = ["k_dep", "lat", "lng"]


def _safe_geometry(feat: dict):
    """Devuelve geometry.coordinates o None (algunas features traen geometry=null)."""
    if not isinstance(feat, dict):
        return None
    geom = feat.get("geometry") or {}
    if not isinstance(geom, dict):
        return None
    return geom.get("coordinates")


def _bounds_from_coords(coords) -> tuple[float, float, float, float] | None:
    """Devuelve (min_lat, min_lng, max_lat, max_lng)."""
    pts = _collect_points(coords)
    if not pts:
        return None
    lats = [p[1] for p in pts]
    lngs = [p[0] for p in pts]
    return min(lats), min(lngs), max(lats), max(lngs)


@st.cache_data(ttl=86400)
def build_district_centroids() -> pd.DataFrame:
    """Devuelve un DataFrame con lat/lng aproximados por distrito."""
    gj = load_geojson(URL_GEO_DISTRICTS, str(GEO_DISTRICTS_LOCAL))
    rows = []
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        # juaneladio/peru-geojson district properties: NOMBDIST, NOMBPROV, NOMBDEP
        dist = props.get("NOMBDIST") or props.get("distrito") or props.get("name")
        prov = props.get("NOMBPROV") or props.get("provincia")
        dep = props.get("NOMBDEP") or props.get("departamento")
        if not (dist and prov and dep):
            continue
        coords = _safe_geometry(feat)
        if coords is None:
            continue
        c = _centroid(coords)
        if c is None:
            continue
        lat, lng = c
        rows.append(
            {
                "k_dist": norm_key(dist),
                "k_prov": norm_key(prov),
                "k_dep": norm_key(dep),
                "lat": lat,
                "lng": lng,
            }
        )
    df = pd.DataFrame(rows, columns=DIST_CENT_COLS)
    return df.drop_duplicates(subset=["k_dist", "k_prov", "k_dep"])


@st.cache_data(ttl=86400)
def build_region_centroids() -> pd.DataFrame:
    gj = load_geojson(URL_GEO_REGIONS, str(GEO_REGIONS_LOCAL))
    rows = []
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        dep = props.get("NOMBDEP") or props.get("name") or props.get("departamento")
        if not dep:
            continue
        coords = _safe_geometry(feat)
        if coords is None:
            continue
        c = _centroid(coords)
        if c is None:
            continue
        rows.append({"k_dep": norm_key(dep), "lat": c[0], "lng": c[1]})
    df = pd.DataFrame(rows, columns=REG_CENT_COLS)
    return df.drop_duplicates(subset=["k_dep"])


@st.cache_data(ttl=86400)
def build_region_bounds() -> dict[str, tuple[float, float, float, float]]:
    gj = load_geojson(URL_GEO_REGIONS, str(GEO_REGIONS_LOCAL))
    out: dict[str, tuple[float, float, float, float]] = {}
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        dep = props.get("NOMBDEP") or props.get("name") or props.get("departamento")
        if not dep:
            continue
        coords = _safe_geometry(feat)
        if coords is None:
            continue
        b = _bounds_from_coords(coords)
        if b:
            out[norm_key(dep)] = b
    return out


@st.cache_data(ttl=86400)
def build_province_bounds() -> dict[tuple[str, str], tuple[float, float, float, float]]:
    gj = load_geojson(URL_GEO_PROVINCES, str(GEO_PROVINCES_LOCAL))
    out: dict[tuple[str, str], tuple[float, float, float, float]] = {}
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        prov = props.get("NOMBPROV") or props.get("provincia")
        dep = props.get("NOMBDEP") or props.get("departamento")
        if not (prov and dep):
            continue
        coords = _safe_geometry(feat)
        if coords is None:
            continue
        b = _bounds_from_coords(coords)
        if b:
            out[(norm_key(dep), norm_key(prov))] = b
    return out


# ---------------------------------------------------------------------------
# Lógica de negocio: cobertura municipal + agregaciones
# ---------------------------------------------------------------------------
def cobertura_munis(munis: pd.DataFrame, emp: pd.DataFrame) -> pd.DataFrame:
    """Una fila por muni con conteos del Archivo 2 (left join)."""
    # Sólo cuentan empadronadores cuya ENTIDAD sea de tipo Municipalidad si
    # estamos midiendo cobertura municipal — en realidad la cobertura debe
    # contar cualquier empadronador relacionado a la muni, así que dejamos
    # todo. Si en el futuro quieres restringir, filtra emp aquí.
    counts = (
        emp.assign(
            ya=(emp["Estado"] == "YA").astype(int),
            pend=(emp["Estado"] == "PENDIENTE").astype(int),
            bloq=(emp["Estado"] == "BLOQUEADO").astype(int),
        )
        .groupby(["k_dist", "k_prov"], as_index=False)
        .agg(
            total_emp=("DNI", "size"),
            ya_emp=("ya", "sum"),
            pend_emp=("pend", "sum"),
            bloq_emp=("bloq", "sum"),
        )
    )
    out = munis.merge(counts, on=["k_dist", "k_prov"], how="left").fillna(
        {"total_emp": 0, "ya_emp": 0, "pend_emp": 0, "bloq_emp": 0}
    )
    for c in ["total_emp", "ya_emp", "pend_emp", "bloq_emp"]:
        out[c] = out[c].astype(int)
    out["tiene_emp"] = out["total_emp"] > 0
    out["tiene_ya"] = out["ya_emp"] > 0  # CE = al menos 1 ya empadronador
    return out


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Dashboard Empadronadores Perú",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Estilos personalizados ---
st.markdown(
    """
    <style>
        :root {
            --brand: #1565c0;
            --brand-2: #0d47a1;
            --bg-card: #ffffff;
            --bg-soft: #f5f7fb;
            --ok: #2e7d32;
            --warn: #fb8c00;
            --bad: #c62828;
            --muted: #6b7280;
        }
        html, body, [class*="css"]  {
            font-family: 'Inter', 'Segoe UI', -apple-system, sans-serif;
        }
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        h1, h2, h3 { letter-spacing: -0.01em; }
        .app-hero {
            background: linear-gradient(135deg, #1565c0 0%, #1e88e5 60%, #42a5f5 100%);
            color: #fff;
            padding: 1.4rem 1.6rem;
            border-radius: 16px;
            margin-bottom: 1rem;
            box-shadow: 0 8px 24px rgba(21,101,192,0.18);
        }
        .app-hero h1 { color: #fff; margin: 0 0 .25rem 0; font-size: 1.75rem; }
        .app-hero p { color: rgba(255,255,255,.92); margin: 0; font-size: .95rem; }
        div[data-testid="stMetric"] {
            background: var(--bg-card);
            padding: 14px 16px;
            border-radius: 14px;
            border: 1px solid #e5e7eb;
            box-shadow: 0 1px 3px rgba(15,23,42,.04);
            transition: transform .15s ease, box-shadow .15s ease;
        }
        div[data-testid="stMetric"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 18px rgba(15,23,42,.08);
        }
        div[data-testid="stMetricLabel"] { color: var(--muted); font-weight: 500; }
        div[data-testid="stMetricValue"] { color: #0f172a; font-weight: 700; }
        section[data-testid="stSidebar"] {
            background: var(--bg-soft);
            border-right: 1px solid #e5e7eb;
        }
        section[data-testid="stSidebar"] h2 { color: var(--brand-2); }
        .stTabs [data-baseweb="tab-list"] { gap: 6px; }
        .stTabs [data-baseweb="tab"] {
            background: var(--bg-soft);
            border-radius: 10px 10px 0 0;
            padding: 8px 14px;
        }
        .stTabs [aria-selected="true"] {
            background: var(--brand);
            color: #fff !important;
        }
        .stDownloadButton button, .stButton button {
            border-radius: 10px;
            border: 1px solid var(--brand);
            color: var(--brand);
            background: #fff;
            font-weight: 600;
        }
        .stDownloadButton button:hover, .stButton button:hover {
            background: var(--brand);
            color: #fff;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="app-hero">
        <h1>🗺️ Dashboard Empadronadores Perú</h1>
        <p>Cobertura municipal y estado de empadronadores · refresco cada 5 min</p>
    </div>
    """,
    unsafe_allow_html=True,
)

PLOTLY_TEMPLATE = "plotly_white"
PLOTLY_COLORWAY = ["#1565c0", "#2e7d32", "#fb8c00", "#c62828", "#6a1b9a", "#00838f"]

# --- Carga ---
try:
    munis = load_munis()
    emp = load_emp()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

# --- Sidebar / filtros ---
with st.sidebar:
    st.header("Filtros")

    entidades = sorted(emp["Entidad"].dropna().unique().tolist())
    entidad_sel = st.multiselect(
        "ENTIDAD",
        options=[TODAS] + entidades,
        default=[TODAS],
    )
    if not entidad_sel or TODAS in entidad_sel:
        entidad_sel = entidades

    departamentos = sorted(munis["Departamento"].unique().tolist())
    dep_sel = st.selectbox("Región / Departamento", [TODOS] + departamentos)

    if dep_sel == TODOS:
        provincias = sorted(munis["Provincia"].unique().tolist())
    else:
        provincias = sorted(
            munis.loc[munis["Departamento"] == dep_sel, "Provincia"].unique().tolist()
        )
    prov_sel = st.selectbox("Provincia", [TODOS] + provincias)

    cond_ce_opts = sorted(
        [v for v in emp["CondicionCE"].dropna().unique().tolist() if v]
    )
    cond_ce_sel = st.selectbox("CONDICION CE", [TODOS] + cond_ce_opts)

    sit_opts = ["Ya empadronador", "Pendiente", "Bloqueado"]
    sit_sel = st.selectbox("SITUACION 2026", [TODOS] + sit_opts)

# --- Aplicar filtros a empadronadores ---
emp_f = emp.copy()
emp_f = emp_f[emp_f["Entidad"].isin(entidad_sel)]
if dep_sel != TODOS:
    emp_f = emp_f[emp_f["k_dep"] == norm_key(dep_sel)]
if prov_sel != TODOS:
    emp_f = emp_f[emp_f["k_prov"] == norm_key(prov_sel)]
if cond_ce_sel != TODOS:
    emp_f = emp_f[emp_f["CondicionCE"] == cond_ce_sel]
if sit_sel != TODOS:
    map_sit = {"Ya empadronador": "YA", "Pendiente": "PENDIENTE", "Bloqueado": "BLOQUEADO"}
    emp_f = emp_f[emp_f["Estado"] == map_sit[sit_sel]]

# Munis filtradas (geográficamente; el Archivo 1 es el universo)
munis_f = munis.copy()
if dep_sel != TODOS:
    munis_f = munis_f[munis_f["k_dep"] == norm_key(dep_sel)]
if prov_sel != TODOS:
    munis_f = munis_f[munis_f["k_prov"] == norm_key(prov_sel)]

# Cobertura sobre TODO el universo (sin filtrar por entidad para los KPIs de cobertura)
cob_global = cobertura_munis(munis_f, emp[emp["Entidad"].isin(entidad_sel)])

# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------
st.subheader("📊 Indicadores")

col = st.columns(4)
col[0].metric("Total munis (Archivo 1)", f"{len(munis_f):,}")
col[1].metric("Con empadronador", f"{int(cob_global['tiene_emp'].sum()):,}")
col[2].metric("Sin empadronador", f"{int((~cob_global['tiene_emp']).sum()):,}")
cob_pct = (cob_global["tiene_emp"].mean() * 100) if len(cob_global) else 0
col[3].metric("% cobertura", f"{cob_pct:.1f}%")

col = st.columns(4)
col[0].metric("Personas registradas (filtro)", f"{len(emp_f):,}")
col[1].metric("Ya empadronadores ✅", f"{int((emp_f['Estado']=='YA').sum()):,}")
col[2].metric("Pendientes ❌", f"{int((emp_f['Estado']=='PENDIENTE').sum()):,}")
col[3].metric("Bloqueados 🚫", f"{int((emp_f['Estado']=='BLOQUEADO').sum()):,}")

st.divider()

# ---------------------------------------------------------------------------
# Mapa
# ---------------------------------------------------------------------------
st.subheader("🗺️ Mapa")

# Detección modo: A si la lista de entidades incluye "MUNICIPALIDAD..."
def is_mode_a(entidades_seleccionadas: list[str]) -> bool:
    return any(ENTIDAD_MUNI_KEY in e for e in entidades_seleccionadas)


mode_a = is_mode_a(entidad_sel)

# Cargar geometrías
try:
    geo_regions = load_geojson(URL_GEO_REGIONS, str(GEO_REGIONS_LOCAL))
    geo_provinces = load_geojson(URL_GEO_PROVINCES, str(GEO_PROVINCES_LOCAL))
    region_cent = build_region_centroids()
    dist_cent = build_district_centroids()
    region_bounds = build_region_bounds()
    province_bounds = build_province_bounds()
except Exception as e:
    st.warning(f"No se pudo cargar GeoJSON: {e}")
    geo_regions = None
    geo_provinces = None
    region_cent = pd.DataFrame(columns=REG_CENT_COLS)
    dist_cent = pd.DataFrame(columns=DIST_CENT_COLS)
    region_bounds = {}
    province_bounds = {}

m = folium.Map(location=[-9.19, -75.02], zoom_start=5, tiles="cartodbpositron")

# Resaltado: la región/provincia seleccionada se pinta con color vivo
sel_dep_key = norm_key(dep_sel) if dep_sel != TODOS else None
sel_prov_key = norm_key(prov_sel) if prov_sel != TODOS else None


def _style_region(feat):
    props = feat.get("properties", {}) or {}
    dep = props.get("NOMBDEP") or props.get("name") or props.get("departamento") or ""
    is_sel = sel_dep_key and norm_key(dep) == sel_dep_key
    return {
        "fillColor": "#1e88e5" if is_sel else "#f0f0f0",
        "color": "#1565c0" if is_sel else "#888",
        "weight": 2 if is_sel else 0.6,
        "fillOpacity": 0.35 if is_sel else 0.15,
    }


def _style_province(feat):
    props = feat.get("properties", {}) or {}
    prov = props.get("NOMBPROV") or props.get("provincia") or ""
    dep = props.get("NOMBDEP") or props.get("departamento") or ""
    is_sel = (
        sel_prov_key
        and norm_key(prov) == sel_prov_key
        and (not sel_dep_key or norm_key(dep) == sel_dep_key)
    )
    return {
        "fillColor": "#fb8c00" if is_sel else "#ffffff",
        "color": "#e65100" if is_sel else "#aaa",
        "weight": 2 if is_sel else 0.4,
        "fillOpacity": 0.45 if is_sel else 0.0,
    }


if geo_regions is not None:
    folium.GeoJson(
        geo_regions,
        name="Regiones Perú",
        style_function=_style_region,
        tooltip=folium.GeoJsonTooltip(
            fields=["NOMBDEP"], aliases=["Región:"], sticky=False
        ),
    ).add_to(m)

# Capa de provincias visible sólo cuando hay región o provincia seleccionada
if geo_provinces is not None and (sel_dep_key or sel_prov_key):
    folium.GeoJson(
        geo_provinces,
        name="Provincias",
        style_function=_style_province,
        tooltip=folium.GeoJsonTooltip(
            fields=["NOMBPROV", "NOMBDEP"],
            aliases=["Provincia:", "Región:"],
            sticky=False,
        ),
    ).add_to(m)

# Zoom automático a la selección
if sel_prov_key and sel_dep_key:
    b = province_bounds.get((sel_dep_key, sel_prov_key))
    if b:
        m.fit_bounds([[b[0], b[1]], [b[2], b[3]]])
elif sel_dep_key:
    b = region_bounds.get(sel_dep_key)
    if b:
        m.fit_bounds([[b[0], b[1]], [b[2], b[3]]])

def _color_pct(pct: float) -> str:
    if pct >= 60:
        return "#2e7d32"  # verde oscuro
    if pct >= 30:
        return "#81c784"  # verde claro
    if pct >= 10:
        return "#fb8c00"  # naranja
    return "#c62828"  # rojo


if mode_a:
    # MODO A: marcador por región
    # cobertura por región (sobre munis_f con entidades filtradas)
    cob_r = (
        cob_global.groupby(["Departamento", "k_dep"], as_index=False)
        .agg(
            total_munis=("Distrito", "size"),
            con_ya=("tiene_ya", "sum"),
            sin_emp=("tiene_emp", lambda s: int((~s).sum())),
        )
    )
    # Sumar empadronadores por región del df ya filtrado
    emp_por_region = (
        emp_f.assign(
            ya=(emp_f["Estado"] == "YA").astype(int),
            pend=(emp_f["Estado"] == "PENDIENTE").astype(int),
            bloq=(emp_f["Estado"] == "BLOQUEADO").astype(int),
        )
        .groupby("k_dep", as_index=False)
        .agg(
            total=("DNI", "size"),
            ya=("ya", "sum"),
            pend=("pend", "sum"),
            bloq=("bloq", "sum"),
        )
    )
    cob_r = cob_r.merge(emp_por_region, on="k_dep", how="left").fillna(0)
    cob_r = cob_r.merge(region_cent, on="k_dep", how="left")

    for _, r in cob_r.iterrows():
        if pd.isna(r.get("lat")):
            continue
        pct = (r["con_ya"] / r["total_munis"] * 100) if r["total_munis"] else 0
        color = _color_pct(pct)
        popup_html = (
            f"<b>{r['Departamento'].upper()}</b>: {int(r['con_ya'])} CE de "
            f"{int(r['total_munis'])} munis ({pct:.1f}%)<br>"
            f"Munis sin ningún empadronador: {int(r['sin_emp'])}<br>"
            f"Total empadronadores: {int(r['total'])}<br>"
            f"Ya empadronadores (HAB+CREADO): {int(r['ya'])}<br>"
            f"Pendientes: {int(r['pend'])}<br>"
            f"Bloqueados: {int(r['bloq'])}"
        )
        folium.CircleMarker(
            location=[r["lat"], r["lng"]],
            radius=8 + (pct / 8),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=f"{r['Departamento']}: {pct:.1f}%",
        ).add_to(m)
else:
    # MODO B: marcador por sede/distrito
    sede_group = (
        emp_f.assign(
            ya=(emp_f["Estado"] == "YA").astype(int),
            pend=(emp_f["Estado"] == "PENDIENTE").astype(int),
            bloq=(emp_f["Estado"] == "BLOQUEADO").astype(int),
        )
        .groupby(
            ["Sede", "Entidad", "Distrito", "Region", "k_dist", "k_prov", "k_dep"],
            as_index=False,
            dropna=False,
        )
        .agg(
            total=("DNI", "size"),
            ya=("ya", "sum"),
            pend=("pend", "sum"),
            bloq=("bloq", "sum"),
            cond_ce=("CondicionCE", lambda s: ", ".join(sorted({x for x in s if x}))),
        )
    )
    sede_group = sede_group.merge(dist_cent, on=["k_dist", "k_prov", "k_dep"], how="left")

    for _, r in sede_group.iterrows():
        if pd.isna(r.get("lat")):
            continue
        pct = (r["ya"] / r["total"] * 100) if r["total"] else 0
        color = _color_pct(pct)
        popup_html = (
            f"<b>{r['Sede']}</b><br>"
            f"Entidad: {r['Entidad']}<br>"
            f"Distrito: {r['Distrito']} · {r['Region']}<br>"
            f"Total empadronadores: {int(r['total'])}<br>"
            f"Ya empadronadores: {int(r['ya'])}<br>"
            f"Pendientes: {int(r['pend'])}<br>"
            f"Bloqueados: {int(r['bloq'])}<br>"
            f"CONDICION CE: {r['cond_ce'] or '-'}"
        )
        folium.CircleMarker(
            location=[r["lat"], r["lng"]],
            radius=5 + min(int(r["total"]), 12),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=f"{r['Sede']} ({int(r['total'])})",
        ).add_to(m)

st_folium(m, height=540, use_container_width=True, returned_objects=[])

st.divider()

# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------
st.subheader("📈 Análisis")

g1, g2 = st.columns(2)

with g1:
    top_reg = (
        emp_f[emp_f["Estado"] == "YA"]
        .groupby("Region", as_index=False)
        .size()
        .rename(columns={"size": "Activos"})
        .sort_values("Activos", ascending=True)
        .tail(10)
    )
    if not top_reg.empty:
        fig = px.bar(
            top_reg,
            x="Activos",
            y="Region",
            orientation="h",
            title="Top 10 regiones por empadronadores activos (HAB+CREADO)",
        )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=40, b=10),
            template=PLOTLY_TEMPLATE,
            colorway=PLOTLY_COLORWAY,
            font=dict(family="Inter, Segoe UI, sans-serif"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos para Top 10 regiones con el filtro actual.")

with g2:
    estado_dist = (
        emp_f["Estado"]
        .replace({"YA": "Ya empadronador", "PENDIENTE": "Pendiente", "BLOQUEADO": "Bloqueado"})
        .value_counts()
        .reset_index()
    )
    estado_dist.columns = ["Estado", "n"]
    if not estado_dist.empty:
        fig = px.pie(
            estado_dist,
            values="n",
            names="Estado",
            hole=0.5,
            title="Distribución de estados",
            color="Estado",
            color_discrete_map={
                "Ya empadronador": "#2e7d32",
                "Pendiente": "#fb8c00",
                "Bloqueado": "#c62828",
            },
        )
        fig.update_layout(
            height=380,
            margin=dict(l=10, r=10, t=40, b=10),
            template=PLOTLY_TEMPLATE,
            colorway=PLOTLY_COLORWAY,
            font=dict(family="Inter, Segoe UI, sans-serif"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos para el donut con el filtro actual.")

g3, g4 = st.columns(2)

with g3:
    ent_dist = (
        emp_f.groupby("Entidad", as_index=False)
        .size()
        .rename(columns={"size": "n"})
        .sort_values("n", ascending=True)
        .tail(15)
    )
    if not ent_dist.empty:
        fig = px.bar(
            ent_dist,
            x="n",
            y="Entidad",
            orientation="h",
            title="Personas registradas por ENTIDAD (top 15)",
        )
        fig.update_layout(
            height=420,
            margin=dict(l=10, r=10, t=40, b=10),
            template=PLOTLY_TEMPLATE,
            colorway=PLOTLY_COLORWAY,
            font=dict(family="Inter, Segoe UI, sans-serif"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sin datos por ENTIDAD.")

with g4:
    cob_dep = (
        cob_global.groupby("Departamento", as_index=False)
        .agg(con=("tiene_emp", "sum"), total=("Distrito", "size"))
    )
    cob_dep["sin"] = cob_dep["total"] - cob_dep["con"]
    cob_dep = cob_dep.sort_values("total", ascending=True).tail(20)
    fig = px.bar(
        cob_dep,
        x=["con", "sin"],
        y="Departamento",
        orientation="h",
        title="Munis con vs sin empadronador por Departamento",
        labels={"value": "Munis", "variable": ""},
        color_discrete_map={"con": "#2e7d32", "sin": "#c62828"},
    )
    fig.update_layout(
        barmode="stack",
        height=420,
        margin=dict(l=10, r=10, t=40, b=10),
        template=PLOTLY_TEMPLATE,
        colorway=PLOTLY_COLORWAY,
        font=dict(family="Inter, Segoe UI, sans-serif"),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Tablas y descargas
# ---------------------------------------------------------------------------
st.subheader("📋 Datos")

tab1, tab2 = st.tabs(["Municipalidades", "Empadronadores"])

with tab1:
    tabla_m = (
        cob_global[
            [
                "Distrito",
                "Provincia",
                "Departamento",
                "tiene_emp",
                "ya_emp",
                "pend_emp",
                "bloq_emp",
            ]
        ]
        .rename(
            columns={
                "tiene_emp": "Con empadronador",
                "ya_emp": "Ya empadronadores",
                "pend_emp": "Pendientes",
                "bloq_emp": "Bloqueados",
            }
        )
        .assign(
            **{
                "Con empadronador": lambda d: d["Con empadronador"].map(
                    {True: "SI", False: "NO"}
                )
            }
        )
        .sort_values(["Departamento", "Provincia", "Distrito"])
        .reset_index(drop=True)
    )
    st.dataframe(tabla_m, use_container_width=True, height=420)
    st.download_button(
        "Descargar CSV — Municipalidades",
        data=tabla_m.to_csv(index=False).encode("utf-8-sig"),
        file_name="municipalidades_cobertura.csv",
        mime="text/csv",
    )

with tab2:
    tabla_e = (
        emp_f.assign(
            **{
                "SITUACION 2026": emp_f["Estado"].replace(
                    {
                        "YA": "Ya empadronador",
                        "PENDIENTE": "Pendiente",
                        "BLOQUEADO": "Bloqueado",
                        "OTRO": emp_f["Situacion2026"],
                    }
                ),
                "Bloqueado": (emp_f["Estado"] == "BLOQUEADO").map({True: "SI", False: "NO"}),
            }
        )[
            [
                "Nombre",
                "DNI",
                "Entidad",
                "Sede",
                "Distrito",
                "Region",
                "CondicionCE",
                "SITUACION 2026",
                "Bloqueado",
            ]
        ]
        .rename(
            columns={
                "Region": "Región",
                "CondicionCE": "CONDICION CE",
            }
        )
        .reset_index(drop=True)
    )
    st.dataframe(tabla_e, use_container_width=True, height=420)
    st.download_button(
        "Descargar CSV — Empadronadores",
        data=tabla_e.to_csv(index=False).encode("utf-8-sig"),
        file_name="empadronadores.csv",
        mime="text/csv",
    )

with st.expander("ℹ️ Lógica de SITUACION 2026"):
    st.markdown(
        """
- **YA empadronador** ✅ = `SITUACION 2026` ∈ {`HABILITADO`, `CREADO`}
- **Pendiente** ❌ = `SITUACION 2026` vacío/nulo *y* sin valor en `BLOQUEADOS`
- **Bloqueado** 🚫 = `BLOQUEADOS` con cualquier valor (tiene prioridad sobre los anteriores)

Universo de municipalidades = **Archivo 1** (lista global). Las munis con
empadronadores son aquellas con match en **Archivo 2** por `Distrito + Provincia`.
"""
    )
