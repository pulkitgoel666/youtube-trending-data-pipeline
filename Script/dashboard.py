import streamlit as st
import duckdb
import plotly.express as px

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="YouTube Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("📊 YouTube Analytics Dashboard")
st.markdown("---")

# -----------------------------
# Connect DuckDB
# -----------------------------
con = duckdb.connect()

# Read Parquet files
channel_df = con.execute("""
SELECT *
FROM read_parquet('channel-analysis.parquet')
""").fetchdf()

daily_df = con.execute("""
SELECT *
FROM read_parquet('daily-analysis.parquet')
""").fetchdf()

# -----------------------------
# KPI Cards
# -----------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Channels",
        len(channel_df)
    )

with col2:
    st.metric(
        "Total Videos",
        f"{channel_df['total_videos'].sum():,}"
    )

with col3:
    st.metric(
        "Total Views",
        f"{channel_df['total_views'].sum():,}"
    )

with col4:
    st.metric(
        "Total Likes",
        f"{channel_df['total_likes'].sum():,}"
    )

st.markdown("---")

# -----------------------------
# Top Channels
# -----------------------------
st.subheader("🏆 Top 10 Channels by Views")

top_channels = (
    channel_df
    .sort_values("total_views", ascending=False)
    .head(10)
)

fig = px.bar(
    top_channels,
    x="channel_title",
    y="total_views",
    color="total_views",
    text="total_views"
)

fig.update_layout(
    xaxis_title="Channel",
    yaxis_title="Views",
    height=500
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Likes vs Comments
# -----------------------------
st.subheader("👍 Likes vs 💬 Comments")

fig2 = px.scatter(
    channel_df,
    x="total_likes",
    y="total_comments",
    size="total_views",
    hover_name="channel_title",
    color="total_views"
)

st.plotly_chart(fig2, use_container_width=True)

# -----------------------------
# Daily Views
# -----------------------------
st.subheader("📅 Daily Views")

daily_df["date"] = (
    daily_df["year"].astype(str)
    + "-"
    + daily_df["month"].astype(str)
    + "-"
    + daily_df["day"].astype(str)
)

fig3 = px.line(
    daily_df.sort_values(["year", "month", "day"]),
    x="date",
    y="total_views",
    markers=True
)

fig3.update_layout(
    xaxis_title="Date",
    yaxis_title="Views",
    height=500
)

st.plotly_chart(fig3, use_container_width=True)

# -----------------------------
# Daily Videos
# -----------------------------
st.subheader("🎥 Videos Uploaded Per Day")

fig4 = px.bar(
    daily_df.sort_values(["year", "month", "day"]),
    x="date",
    y="total_videos",
    color="total_videos"
)

st.plotly_chart(fig4, use_container_width=True)

# -----------------------------
# Channel Table
# -----------------------------
st.subheader("📋 Channel Analytics")

st.dataframe(
    channel_df.sort_values(
        "total_views",
        ascending=False
    ),
    use_container_width=True
)
