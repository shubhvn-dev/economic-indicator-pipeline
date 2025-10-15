from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st

# Page configuration
st.set_page_config(page_title="Economic Indicators Dashboard", page_icon="📊", layout="wide")


# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect("/opt/airflow/data/gold/economic_indicators.duckdb", read_only=True)


@st.cache_data
def load_indicators():
    conn = get_connection()
    return conn.execute("SELECT * FROM dim_indicators ORDER BY indicator_id").fetchdf()


@st.cache_data
def load_observations(indicator_ids):
    conn = get_connection()
    query = f"""
        SELECT 
            f.indicator_id,
            i.indicator_name,
            f.date_key,
            f.observation_value,
            f.yoy_change_pct,
            t.year,
            t.quarter,
            t.month
        FROM fct_observations f
        JOIN dim_indicators i ON f.indicator_id = i.indicator_id
        JOIN dim_time t ON f.date_key = t.date_key
        WHERE f.indicator_id IN ({','.join([f"'{id}'" for id in indicator_ids])})
        ORDER BY f.date_key
    """
    return conn.execute(query).fetchdf()


# Title
st.title("📊 Economic Indicators Dashboard")
st.markdown("Real-time analysis of key US economic indicators from FRED")

# Sidebar filters
st.sidebar.header("Filters")

indicators_df = load_indicators()
selected_indicators = st.sidebar.multiselect(
    "Select Indicators",
    options=indicators_df["indicator_id"].tolist(),
    default=["GDP", "UNRATE", "CPIAUCSL"],
    format_func=lambda x: indicators_df[indicators_df["indicator_id"] == x]["indicator_name"].iloc[
        0
    ],
)

if not selected_indicators:
    st.warning("Please select at least one indicator")
    st.stop()

# Load data
data = load_observations(selected_indicators)

# Date range filter
min_date = data["date_key"].min()
max_date = data["date_key"].max()
date_range = st.sidebar.date_input(
    "Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date
)

# Filter data by date range
if len(date_range) == 2:
    data = data[
        (data["date_key"] >= pd.Timestamp(date_range[0]))
        & (data["date_key"] <= pd.Timestamp(date_range[1]))
    ]

# Key Metrics Row
st.header("Key Metrics")
cols = st.columns(len(selected_indicators))

for idx, indicator_id in enumerate(selected_indicators):
    indicator_data = data[data["indicator_id"] == indicator_id]
    latest_value = indicator_data.iloc[-1]["observation_value"]
    latest_yoy = indicator_data.iloc[-1]["yoy_change_pct"]
    indicator_name = indicator_data.iloc[-1]["indicator_name"]

    with cols[idx]:
        st.metric(
            label=indicator_name,
            value=f"{latest_value:.2f}",
            delta=f"{latest_yoy:.2f}% YoY" if pd.notna(latest_yoy) else "N/A",
        )

# Time Series Chart
st.header("Time Series Analysis")

fig = go.Figure()
for indicator_id in selected_indicators:
    indicator_data = data[data["indicator_id"] == indicator_id]
    fig.add_trace(
        go.Scatter(
            x=indicator_data["date_key"],
            y=indicator_data["observation_value"],
            name=indicator_data.iloc[0]["indicator_name"],
            mode="lines+markers",
        )
    )

fig.update_layout(
    title="Economic Indicators Over Time",
    xaxis_title="Date",
    yaxis_title="Value",
    hovermode="x unified",
    height=500,
)
st.plotly_chart(fig, use_container_width=True)

# Year-over-Year Changes
st.header("Year-over-Year Changes")

fig_yoy = go.Figure()
for indicator_id in selected_indicators:
    indicator_data = data[data["indicator_id"] == indicator_id]
    indicator_data_clean = indicator_data.dropna(subset=["yoy_change_pct"])

    fig_yoy.add_trace(
        go.Scatter(
            x=indicator_data_clean["date_key"],
            y=indicator_data_clean["yoy_change_pct"],
            name=indicator_data_clean.iloc[0]["indicator_name"],
            mode="lines",
        )
    )

fig_yoy.update_layout(
    title="Year-over-Year Percentage Change",
    xaxis_title="Date",
    yaxis_title="YoY Change (%)",
    hovermode="x unified",
    height=400,
)
fig_yoy.add_hline(y=0, line_dash="dash", line_color="gray")
st.plotly_chart(fig_yoy, use_container_width=True)

# Correlation Analysis
if len(selected_indicators) > 1:
    st.header("Correlation Analysis")

    try:
        # Deduplicate by taking the most recent observation for each date/indicator
        deduped_data = (
            data.sort_values("date_key")
            .groupby(["date_key", "indicator_name"], as_index=False)
            .last()
        )

        # Create pivot table
        pivot_data = deduped_data.pivot(
            index="date_key", columns="indicator_name", values="observation_value"
        )
        correlation_matrix = pivot_data.corr()

        fig_corr = px.imshow(
            correlation_matrix,
            text_auto=".2f",
            aspect="auto",
            color_continuous_scale="RdBu_r",
            title="Correlation Matrix",
        )
        st.plotly_chart(fig_corr, use_container_width=True)
    except Exception as e:
        st.error(f"Could not generate correlation matrix: {str(e)}")

# Data Table
st.header("Raw Data")
st.dataframe(
    data[["indicator_name", "date_key", "observation_value", "yoy_change_pct"]]
    .sort_values("date_key", ascending=False)
    .head(50),
    use_container_width=True,
)

# Download button
csv = data.to_csv(index=False)
st.download_button(
    label="Download Data as CSV",
    data=csv,
    file_name=f"economic_indicators_{datetime.now().strftime('%Y%m%d')}.csv",
    mime="text/csv",
)

# Footer
st.markdown("---")
st.markdown("Data Source: Federal Reserve Economic Data (FRED) | Updated: Daily")
