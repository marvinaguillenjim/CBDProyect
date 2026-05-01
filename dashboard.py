"""
dashboard.py  –  Dashboard Streamlit: Censo de Conductores de España
Resultados procesados con MapReduce (Python Streaming, compatible con Hadoop)

Arquitectura AWS propuesta:
  S3 (datos raw)  →  Lambda (trigger)  →  EMR / local pipeline  →  S3 (JSON resultado)  →  Streamlit
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Censo de Conductores · España",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# ESTILOS
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Space Grotesk', sans-serif;
}
.metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #0f3460;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    text-align: center;
    color: white;
}
.metric-card .value {
    font-size: 2rem;
    font-weight: 700;
    color: #e94560;
    font-family: 'JetBrains Mono', monospace;
}
.metric-card .label {
    font-size: 0.85rem;
    color: #a0aec0;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 0.3rem;
}
.badge {
    background: #e94560;
    color: white;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 700;
}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# NOMBRES DE PROVINCIAS
# ──────────────────────────────────────────────────────────────────────────────
PROVINCIAS = {
    "01": "Álava", "02": "Albacete", "03": "Alicante", "04": "Almería",
    "05": "Ávila", "06": "Badajoz", "07": "Baleares", "08": "Barcelona",
    "09": "Burgos", "10": "Cáceres", "11": "Cádiz", "12": "Castellón",
    "13": "Ciudad Real", "14": "Córdoba", "15": "A Coruña", "16": "Cuenca",
    "17": "Girona", "18": "Granada", "19": "Guadalajara", "20": "Gipuzkoa",
    "21": "Huelva", "22": "Huesca", "23": "Jaén", "24": "León",
    "25": "Lleida", "26": "La Rioja", "27": "Lugo", "28": "Madrid",
    "29": "Málaga", "30": "Murcia", "31": "Navarra", "32": "Ourense",
    "33": "Asturias", "34": "Palencia", "35": "Las Palmas", "36": "Pontevedra",
    "37": "Salamanca", "38": "S.C. Tenerife", "39": "Cantabria", "40": "Segovia",
    "41": "Sevilla", "42": "Soria", "43": "Tarragona", "44": "Teruel",
    "45": "Toledo", "46": "Valencia", "47": "Valladolid", "48": "Vizcaya",
    "49": "Zamora", "50": "Zaragoza", "51": "Ceuta", "52": "Melilla",
}

PERMISOS_DESC = {
    "A": "Motocicletas (A)", "A1": "Motocicletas ligeras (A1)",
    "A2": "Motocicletas medias (A2)", "AM": "Ciclomotores (AM)",
    "B": "Turismos (B)", "B+E": "Turismo + remolque (B+E)",
    "C": "Camiones (C)", "C+E": "Camión + remolque (C+E)",
    "C1": "Camión ligero (C1)", "C1+E": "C1 + remolque",
    "D": "Autobús (D)", "D+E": "Autobús + remolque (D+E)",
    "D1": "Minibús (D1)", "D1+E": "D1 + remolque",
    "LCC": "Licencia ciclomotor (LCC)", "BTP": "Transporte público (BTP)",
}

# ──────────────────────────────────────────────────────────────────────────────
# CARGA DE DATOS (salida del MapReduce)
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data
def cargar_resultados_mapreduce():
    """
    Carga el JSON generado por el pipeline MapReduce.
    En producción AWS este fichero vendría de S3:
      import boto3, json
      s3 = boto3.client('s3')
      obj = s3.get_object(Bucket='mi-bucket', Key='output/resultados_mapreduce.json')
      return json.loads(obj['Body'].read())
    """
    rutas = [
        "resultados_mapreduce.json",
        "/home/claude/resultados_mapreduce.json",
    ]
    for ruta in rutas:
        if os.path.exists(ruta):
            with open(ruta, encoding="utf-8") as f:
                return json.load(f)
    st.error("❌ No se encontró 'resultados_mapreduce.json'. Ejecuta primero run_mapreduce.py")
    st.stop()

@st.cache_data
def preparar_dataframes(data: dict):
    """Convierte el JSON de resultados en DataFrames de Pandas."""

    # ── Por provincia ──────────────────────────────────────────
    df_prov = (
        pd.DataFrame(list(data["provincia"].items()), columns=["cod", "conductores"])
        .assign(provincia=lambda d: d["cod"].map(PROVINCIAS).fillna(d["cod"]))
        .sort_values("conductores", ascending=False)
    )

    # ── Por sexo ───────────────────────────────────────────────
    df_sexo = pd.DataFrame(
        [{"sexo": "Mujer" if k == "M" else "Hombre", "conductores": v}
         for k, v in data["sexo"].items()]
    )

    # ── Por permiso ────────────────────────────────────────────
    df_permiso = (
        pd.DataFrame(list(data["permiso"].items()), columns=["permiso", "conductores"])
        .assign(descripcion=lambda d: d["permiso"].map(PERMISOS_DESC).fillna(d["permiso"]))
        .sort_values("conductores", ascending=False)
    )

    # ── Por año (antigüedad) ───────────────────────────────────
    df_anio = (
        pd.DataFrame(list(data["anio"].items()), columns=["anio", "conductores"])
        .assign(es_anterior=lambda d: d["anio"].str.startswith("Anterior"))
        .sort_values("anio")
    )

    # ── Provincia × Sexo ───────────────────────────────────────
    rows = []
    for clave, val in data["provincia_sexo"].items():
        cod, sexo_cod = clave.rsplit("_", 1)
        rows.append({
            "cod": cod,
            "provincia": PROVINCIAS.get(cod, cod),
            "sexo": "Mujer" if sexo_cod == "M" else "Hombre",
            "conductores": val,
        })
    df_prov_sexo = pd.DataFrame(rows)

    # ── Permiso × Sexo ─────────────────────────────────────────
    rows2 = []
    for clave, val in data["permiso_sexo"].items():
        parts = clave.rsplit("_", 1)
        if len(parts) == 2:
            perm, sexo_cod = parts
            rows2.append({
                "permiso": perm,
                "sexo": "Mujer" if sexo_cod == "M" else "Hombre",
                "conductores": val,
            })
    df_perm_sexo = pd.DataFrame(rows2)

    return df_prov, df_sexo, df_permiso, df_anio, df_prov_sexo, df_perm_sexo

# ──────────────────────────────────────────────────────────────────────────────
# CARGA
# ──────────────────────────────────────────────────────────────────────────────
raw = cargar_resultados_mapreduce()
df_prov, df_sexo, df_permiso, df_anio, df_prov_sexo, df_perm_sexo = preparar_dataframes(raw)

total = df_prov["conductores"].sum()
n_provincias = len(df_prov)
permiso_top = df_permiso.iloc[0]["permiso"]
prov_top = df_prov.iloc[0]["provincia"]

# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🚗 Filtros")

    # Filtro de provincias
    provincias_opciones = sorted(df_prov["provincia"].tolist())
    provincias_sel = st.multiselect(
        "Provincias",
        options=provincias_opciones,
        default=provincias_opciones[:10],
        help="Filtra las provincias mostradas en los gráficos de barras"
    )

    # Filtro de permisos
    permisos_opciones = sorted(df_permiso["permiso"].tolist())
    permisos_sel = st.multiselect(
        "Clases de permiso",
        options=permisos_opciones,
        default=permisos_opciones,
    )

    st.divider()
    st.markdown("""
    <small>
    <span class="badge">MapReduce</span> Datos procesados con<br>
    Python Streaming · compatible Hadoop<br><br>
    <b>Fuente:</b> DGT – Censo de Conductores<br>
    Enero 2026
    </small>
    """, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# TÍTULO
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("# 🚗 Censo de Conductores de España")
st.markdown(
    "Análisis procesado con **MapReduce** (Python Streaming) · "
    "Arquitectura desplegable en **AWS Lambda + S3**"
)

# ──────────────────────────────────────────────────────────────────────────────
# MÉTRICAS
# ──────────────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{total/1e6:.1f}M</div>
        <div class="label">Total conductores</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{n_provincias}</div>
        <div class="label">Provincias</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{permiso_top}</div>
        <div class="label">Permiso más común</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{prov_top[:8]}</div>
        <div class="label">Provincia líder</div>
    </div>""", unsafe_allow_html=True)

st.divider()

# ──────────────────────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Por Provincia",
    "🚦 Por Permiso",
    "👥 Por Sexo",
    "📈 Evolución Temporal",
    "🗂️ Tabla de Datos",
])

COLORES = px.colors.qualitative.Set2

# ── TAB 1: Por provincia ──────────────────────────────────────────────────────
with tab1:
    st.subheader("Distribución por Provincia")

    df_f = df_prov[df_prov["provincia"].isin(provincias_sel)] if provincias_sel else df_prov

    col_a, col_b = st.columns([2, 1])
    with col_a:
        fig = px.bar(
            df_f.sort_values("conductores", ascending=True),
            x="conductores", y="provincia",
            orientation="h",
            color="conductores",
            color_continuous_scale="Reds",
            labels={"conductores": "Nº Conductores", "provincia": "Provincia"},
            title="Conductores por provincia",
            height=max(400, len(df_f) * 22),
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#333",
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("**Top 10 provincias**")
        df_top10 = df_prov.head(10)[["provincia", "conductores"]].copy()
        df_top10["conductores"] = df_top10["conductores"].apply(lambda x: f"{x:,}")
        st.dataframe(df_top10, use_container_width=True, hide_index=True)

    # Hombres vs Mujeres por provincia
    st.subheader("Distribución Hombre / Mujer por Provincia")
    df_ps_f = df_prov_sexo[df_prov_sexo["provincia"].isin(provincias_sel)] if provincias_sel else df_prov_sexo
    fig2 = px.bar(
        df_ps_f.sort_values("conductores", ascending=False),
        x="provincia", y="conductores", color="sexo",
        barmode="group",
        color_discrete_map={"Hombre": "#0f3460", "Mujer": "#e94560"},
        labels={"conductores": "Nº Conductores", "provincia": "Provincia"},
        height=450,
    )
    fig2.update_xaxes(tickangle=45)
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)

# ── TAB 2: Por permiso ────────────────────────────────────────────────────────
with tab2:
    st.subheader("Distribución por Clase de Permiso")

    df_pm_f = df_permiso[df_permiso["permiso"].isin(permisos_sel)] if permisos_sel else df_permiso

    col_a, col_b = st.columns(2)
    with col_a:
        fig = px.bar(
            df_pm_f.sort_values("conductores", ascending=False),
            x="permiso", y="conductores",
            color="conductores",
            color_continuous_scale="Blues",
            text="conductores",
            labels={"conductores": "Nº Conductores", "permiso": "Clase de permiso"},
            title="Conductores por clase de permiso",
        )
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        fig_pie = px.pie(
            df_pm_f,
            names="permiso", values="conductores",
            title="Cuota de cada permiso",
            color_discrete_sequence=px.colors.qualitative.Set3,
            hole=0.4,
        )
        fig_pie.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pie, use_container_width=True)

    # Permiso × Sexo
    st.subheader("Permiso por Sexo")
    df_ps2_f = df_perm_sexo[df_perm_sexo["permiso"].isin(permisos_sel)] if permisos_sel else df_perm_sexo
    fig3 = px.bar(
        df_ps2_f, x="permiso", y="conductores", color="sexo",
        barmode="stack",
        color_discrete_map={"Hombre": "#0f3460", "Mujer": "#e94560"},
        title="Distribución de cada permiso por sexo",
        labels={"conductores": "Nº Conductores"},
    )
    fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig3, use_container_width=True)

# ── TAB 3: Por sexo ───────────────────────────────────────────────────────────
with tab3:
    st.subheader("Distribución por Sexo")

    col_a, col_b = st.columns(2)
    with col_a:
        fig = px.pie(
            df_sexo, names="sexo", values="conductores",
            color_discrete_map={"Hombre": "#0f3460", "Mujer": "#e94560"},
            title="Conductores totales por sexo",
            hole=0.45,
        )
        for i, row in df_sexo.iterrows():
            pct = row["conductores"] / total * 100
            fig.add_annotation(
                text=f"{row['sexo']}<br><b>{pct:.1f}%</b>",
                x=0.5, y=0.5, showarrow=False, font_size=14
            )
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("**Detalle numérico**")
        df_sexo_disp = df_sexo.copy()
        df_sexo_disp["conductores"] = df_sexo_disp["conductores"].apply(lambda x: f"{x:,}")
        df_sexo_disp["porcentaje"] = df_sexo.apply(
            lambda r: f"{r['conductores']/total*100:.2f}%"
            if isinstance(r["conductores"], (int, float))
            else f"{int(r['conductores'].replace(',',''))/total*100:.2f}%",
            axis=1
        )
        # Recalcular para mostrar bien
        df_sexo_disp2 = df_sexo.copy()
        df_sexo_disp2["pct"] = (df_sexo_disp2["conductores"] / total * 100).round(2)
        df_sexo_disp2["conductores_fmt"] = df_sexo_disp2["conductores"].apply(lambda x: f"{x:,}")
        df_sexo_disp2 = df_sexo_disp2[["sexo", "conductores_fmt", "pct"]]
        df_sexo_disp2.columns = ["Sexo", "Conductores", "% del total"]
        st.dataframe(df_sexo_disp2, use_container_width=True, hide_index=True)

# ── TAB 4: Evolución temporal ─────────────────────────────────────────────────
with tab4:
    st.subheader("Obtención del Permiso por Año (Antigüedad)")

    df_anio_plot = df_anio.copy()

    # Separar "Anterior_2014" para destacarlo
    df_recientes = df_anio_plot[~df_anio_plot["es_anterior"]].copy()
    df_anterior  = df_anio_plot[df_anio_plot["es_anterior"]].copy()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_recientes["anio"],
        y=df_recientes["conductores"],
        name="Permisos obtenidos",
        marker_color="#0f3460",
    ))
    if not df_anterior.empty:
        fig.add_trace(go.Bar(
            x=df_anterior["anio"],
            y=df_anterior["conductores"],
            name="Anterior a 2014",
            marker_color="#e94560",
        ))
    fig.update_layout(
        title="Conductores según año de obtención del permiso",
        xaxis_title="Año",
        yaxis_title="Nº Conductores",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        barmode="group",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Tendencia (sin "Anterior_2014")
    st.markdown("**Tendencia de nuevos permisos (2014-2026)**")
    fig2 = px.line(
        df_recientes,
        x="anio", y="conductores",
        markers=True,
        labels={"anio": "Año", "conductores": "Nº Conductores"},
        color_discrete_sequence=["#e94560"],
    )
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)

# ── TAB 5: Tabla ──────────────────────────────────────────────────────────────
with tab5:
    st.subheader("Datos agregados por MapReduce")

    vista = st.selectbox(
        "Selecciona dimensión",
        ["Provincia", "Permiso", "Sexo", "Año", "Provincia × Sexo", "Permiso × Sexo"]
    )

    mapa_vistas = {
        "Provincia":         df_prov.rename(columns={"cod": "Código", "provincia": "Provincia", "conductores": "Conductores"}),
        "Permiso":           df_permiso.rename(columns={"permiso": "Permiso", "descripcion": "Descripción", "conductores": "Conductores"}),
        "Sexo":              df_sexo.rename(columns={"sexo": "Sexo", "conductores": "Conductores"}),
        "Año":               df_anio[["anio", "conductores"]].rename(columns={"anio": "Año", "conductores": "Conductores"}),
        "Provincia × Sexo":  df_prov_sexo.rename(columns={"cod": "Código", "provincia": "Provincia", "sexo": "Sexo", "conductores": "Conductores"}),
        "Permiso × Sexo":    df_perm_sexo.rename(columns={"permiso": "Permiso", "sexo": "Sexo", "conductores": "Conductores"}),
    }

    df_tabla = mapa_vistas[vista].copy()
    if "Conductores" in df_tabla.columns:
        df_tabla["Conductores"] = df_tabla["Conductores"].apply(lambda x: f"{x:,}")

    st.dataframe(df_tabla, use_container_width=True, hide_index=True)

    st.download_button(
        label="📥 Descargar CSV",
        data=mapa_vistas[vista].to_csv(index=False).encode("utf-8"),
        file_name=f"censo_conductores_{vista.replace(' ', '_').lower()}.csv",
        mime="text/csv",
    )
