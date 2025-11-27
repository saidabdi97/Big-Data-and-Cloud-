import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path


st.set_page_config(page_title="JobTech Dashboard", layout="wide")

# UI-tema (behålls exakt som du hade det)
st.markdown("""
    <style>
    .block-container {padding-top: 1rem;}
    h1, h2, h3 {color: #F4F4F4;}
    body {background-color: #0E1117; color: #E0E0E0;}
    </style>
""", unsafe_allow_html=True)

st.title("JobTech Dashboard – Datapipeline")
st.caption("Visar data från dlt → DuckDB → dbt")


# -------------------------------------------
# Ladda data från DuckDB
# -------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "job_ads_pipeline.duckdb"
SCHEMA = "job_ads_dataset"

@st.cache_data
def load_data():
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"SET schema='{SCHEMA}'")

    query = """
    SELECT
        f.job_id,
        f.title,
        f.transformed_at,
        o.occupation_field,
        l.municipality,
        e.employer
    FROM fact_job_ads f
    LEFT JOIN dim_occupation o ON f.occupation_key = o.occupation_key
    LEFT JOIN dim_location   l ON f.location_key   = l.location_key
    LEFT JOIN dim_employer   e ON f.employer_key   = e.employer_key
    """

    df = con.execute(query).fetch_df()
    df["transformed_at"] = pd.to_datetime(df["transformed_at"], errors="coerce")
    return df


# ---------------------------------------------------------
# Start
# ---------------------------------------------------------

df = load_data()

if df.empty:
    st.error("Inga data i DuckDB. Kör dlt + dbt först.")
    st.stop()

st.success(f"{len(df)} annonser laddade från DuckDB/dbt.")


# ---------------------------------------------------------
# VISUALISERINGAR (samma stil som du hade)
# ---------------------------------------------------------

# 1 – Yrkesområden
st.subheader("1. Fördelning per yrkesområde")
fig1 = px.bar(
    df.groupby("occupation_field").size().reset_index(name="Antal"),
    x="occupation_field", y="Antal", color="occupation_field",
    text="Antal", title="Antal jobb per yrkesområde", template="plotly_dark"
)
fig1.update_traces(textposition='outside')
st.plotly_chart(fig1, use_container_width=True)


# 2 – Kommuner
st.subheader("2. Topp 10 kommuner med flest jobb")
kommun_df = df["municipality"].value_counts().head(10).reset_index()
kommun_df.columns = ["Kommun", "Antal"]
fig2 = px.bar(
    kommun_df, x="Kommun", y="Antal", color="Kommun",
    text="Antal", title="Kommuner med flest lediga jobb", template="plotly_dark"
)
fig2.update_traces(textposition='outside')
st.plotly_chart(fig2, use_container_width=True)


# 3 – Arbetsgivare
st.subheader("3. Största arbetsgivare")
employer_df = df["employer"].value_counts().head(10).reset_index()
employer_df.columns = ["Arbetsgivare", "Antal"]
fig3 = px.bar(
    employer_df, y="Arbetsgivare", x="Antal", orientation="h",
    text="Antal", title="Topp 10 största arbetsgivare", template="plotly_dark",
    color="Antal", color_continuous_scale="Blues"
)
fig3.update_traces(textposition='outside')
st.plotly_chart(fig3, use_container_width=True)


# 4 – Kommun × yrkesområde
st.subheader("4. Jobb per kommun och yrkesområde (topp 20)")
combo_df = (
    df.groupby(["occupation_field", "municipality"])
    .size()
    .reset_index(name="Antal")
    .sort_values("Antal", ascending=False)
    .head(20)
)
fig4 = px.bar(
    combo_df, x="municipality", y="Antal", color="occupation_field",
    title="Topp 20 kombinationer av kommun & yrkesområde", template="plotly_dark"
)
st.plotly_chart(fig4, use_container_width=True)


# 5 – Rådata
st.subheader("5. Rådata från datalagret")
st.dataframe(df, use_container_width=True)
