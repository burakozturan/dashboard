import os
import json
import pandas as pd
import streamlit as st
from datetime import datetime
import matplotlib.pyplot as plt
import altair as alt
from collections import defaultdict

# ----------------------
# CONFIG
# ----------------------
st.set_page_config(page_title="Media Platform Dashboard", layout="wide")

# ---- PATHS ---- #
topic_data_root = "/Users/burak/microsoft"
sentiment_data_root = "/Users/burak/microsoft/output_entities_sentiment_structured"

platform_folders = {
    "TikTok": "tiktok",
    "Twitter": "twitter",
    "YouTube": "codes/youtube",
    "Bluesky": "bluesky"
}
date_cols = {
    "TikTok": "create_time",
    "Twitter": "date",
    "YouTube": "publishedAt",
    "Bluesky": "created_at"
}

# ----------------------
# Load Topic Distribution CSV
# ----------------------
def load_topic_csv(outlet, platform):
    # Normalize platform name (case-insensitive lookup)
    platform_lookup = {k.lower(): v for k, v in platform_folders.items()}
    platform_key = platform.lower()

    if platform_key not in platform_lookup:
        return pd.DataFrame()  # Platform not recognized

    folder_path = os.path.join(topic_data_root, platform_lookup[platform_key])

    try:
        matched_file = next(
            (f for f in os.listdir(folder_path) if f.startswith(outlet) and f.endswith("_with_sections.csv")),
            None
        )
        if not matched_file:
            return pd.DataFrame()

        df = pd.read_csv(os.path.join(folder_path, matched_file))
        date_col_lookup = {k.lower(): v for k, v in date_cols.items()}
        date_col = date_col_lookup.get(platform_key)

        if not date_col or date_col not in df.columns:
            return pd.DataFrame()

        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[date_col] = df[date_col].dt.tz_localize(None)
        df = df.rename(columns={date_col: "timestamp"})
        return df

    except Exception as e:
        print(f"Error loading topic CSV for {outlet}, {platform}: {e}")
        return pd.DataFrame()


# ----------------------
# Load Sentiment Data
# ----------------------
@st.cache_data
def load_sentiment_csvs(data_folder):
    dataframes = defaultdict(dict)
    date_ranges = defaultdict(dict)
    outlet_platform_map = defaultdict(list)

    for outlet_name in os.listdir(data_folder):
        outlet_path = os.path.join(data_folder, outlet_name)
        if os.path.isdir(outlet_path):
            for file in os.listdir(outlet_path):
                if file.endswith(".csv") and "entities_sentiment_by_date" in file:
                    platform = file.replace("_entities_sentiment_by_date.csv", "")
                    if platform.startswith(outlet_name + "_"):
                        platform = platform[len(outlet_name) + 1:]
                    filepath = os.path.join(outlet_path, file)
                    try:
                        df = pd.read_csv(filepath)
                        df.columns = df.columns.str.lower()
                        if "created_at" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["created_at"], errors="coerce")
                        elif "date" in df.columns:
                            df["timestamp"] = pd.to_datetime(df["date"], errors="coerce")
                        else:
                            continue
                        df = df.dropna(subset=["timestamp"])
                        if df.empty:
                            continue
                        min_date = df["timestamp"].min()
                        max_date = df["timestamp"].max()
                        dataframes[outlet_name][platform] = df
                        date_ranges[outlet_name][platform] = (min_date, max_date)
                        outlet_platform_map[outlet_name].append(platform)
                    except Exception as e:
                        print(f"Error reading {filepath}: {e}")
    return dataframes, date_ranges, sorted(outlet_platform_map.keys()), outlet_platform_map

dataframes, file_date_ranges, outlets, outlet_platform_map = load_sentiment_csvs(sentiment_data_root)

# ----------------------
# Sidebar Filters
# ----------------------
st.sidebar.header("Select First Media Outlet")
outlet1 = st.sidebar.selectbox("Media Outlet 1", outlets)
platforms1 = outlet_platform_map[outlet1]
platform1 = st.sidebar.selectbox("Platform for Outlet 1", sorted(platforms1))

st.sidebar.header("Select Second Media Outlet")
outlet2 = st.sidebar.selectbox("Media Outlet 2", outlets)
platforms2 = outlet_platform_map[outlet2]
platform2 = st.sidebar.selectbox("Platform for Outlet 2", sorted(platforms2))

selected_combinations = [(outlet1, platform1), (outlet2, platform2)]

# ----------------------
# Topic Distribution Charts
# ----------------------
st.subheader("Topical Distribution by Section")

col1, col2 = st.columns(2)
filtered_counts = []

# Step 1: DATE FILTERING
for idx, (outlet, platform) in enumerate(selected_combinations):
    df_topic = load_topic_csv(outlet, platform)
    column = col1 if idx == 0 else col2

    if df_topic.empty or "section" not in df_topic.columns:
        column.warning(f"No topic data found for {outlet} on {platform}.")
        filtered_counts.append(None)
        continue

    min_date = df_topic["timestamp"].min()
    max_date = df_topic["timestamp"].max()

    with column:
        st.markdown(f"**{outlet} – {platform}**")
        selected_range = st.slider(
            f"Select Date Range for {outlet} on {platform}",
            min_value=min_date.date(),
            max_value=max_date.date(),
            value=(min_date.date(), max_date.date()),
            key=f"slider_date_{idx}_{outlet}_{platform}"
        )

        mask = (df_topic["timestamp"].dt.date >= selected_range[0]) & (df_topic["timestamp"].dt.date <= selected_range[1])
        df_filtered = df_topic[mask]

        if df_filtered.empty:
            st.info(f"No data in selected range for {outlet} on {platform}")
            filtered_counts.append(None)
        else:
            section_counts = df_filtered["merged_section"].value_counts(normalize=True) * 100
            filtered_counts.append(section_counts)

# Step 2: THRESHOLD FILTERING + PIE CHARTS
threshold = st.slider("Minimum % to Display in Pie Charts", min_value=0, max_value=20, value=2)

for idx, counts in enumerate(filtered_counts):
    if counts is None:
        continue
    counts = counts[counts >= threshold]
    with (col1 if idx == 0 else col2):
        fig, ax = plt.subplots()
        ax.pie(counts, labels=counts.index, autopct="%1.1f%%", startangle=140)
        ax.axis("equal")
        st.pyplot(fig)

  





# ----------------------
# Entity-Level Comparison (Original Dashboard 2 Content)
# ----------------------

entity_stopwords = {
    "unknown",
    "n/a",
    "none",
    "please provide the text",
    "i'm sorry",
    "example.com",
    "#cnn#",
    'a year',
    'ABC News'
    '@ABC News',
    'BBCNews',
    'MSNBC',
     'www.nbcnew.com/politics/tru',
     '@msnbc.com',
     "n't",
     '@NYTimes',
     'the Reuters World News'
    # Add your custom garbage/low-quality entities here
}


comparison_data = []

for outlet, platform in selected_combinations:
    if outlet in dataframes and platform in dataframes[outlet]:
        df = dataframes[outlet][platform]
        min_date, max_date = file_date_ranges[outlet][platform]
        df_filtered = df[(df["timestamp"] >= min_date) & (df["timestamp"] <= max_date)].copy()
        df_filtered["source"] = f"{outlet}_{platform}"
        comparison_data.append(df_filtered)
    else:
        st.warning(f"No data for {outlet} on {platform}.")

if comparison_data:
    merged_df = pd.concat(comparison_data, ignore_index=True)

    st.subheader("Entity % Share Comparison for Top Entities")
    if "entity" in merged_df.columns:
        min_date = merged_df["timestamp"].min().date()
        max_date = merged_df["timestamp"].max().date()
        date_range = st.slider("Filter by Date", min_value=min_date, max_value=max_date, value=(min_date, max_date))
        mask = (merged_df["timestamp"].dt.date >= date_range[0]) & (merged_df["timestamp"].dt.date <= date_range[1])
        filtered_df = merged_df[mask].dropna(subset=["entity"])
        filtered_df["entity"] = filtered_df["entity"].astype(str).str.strip()
        filtered_df = filtered_df[~filtered_df["entity"].str.lower().isin({e.lower() for e in entity_stopwords})]

        grouped = filtered_df.groupby(["source", "entity"]).size().reset_index(name="count")
        totals = filtered_df.groupby("source").size().reset_index(name="total")
        merged = pd.merge(grouped, totals, on="source")
        merged["percent"] = (merged["count"] / merged["total"]) * 100

        top1 = merged[merged["source"] == f"{outlet1}_{platform1}"].nlargest(10, "count")["entity"].tolist()
        top2 = merged[merged["source"] == f"{outlet2}_{platform2}"].nlargest(10, "count")["entity"].tolist()
        top_entities = list(set(top1 + top2))
        merged = merged[merged["entity"].isin(top_entities)]

        for entity in top_entities:
            for source in [f"{outlet1}_{platform1}", f"{outlet2}_{platform2}"]:
                if not ((merged["source"] == source) & (merged["entity"] == entity)).any():
                    merged = pd.concat([merged, pd.DataFrame({
                        "source": [source], "entity": [entity], "count": [0], "total": [1], "percent": [0.0]
                    })], ignore_index=True)

        chart = alt.Chart(merged).mark_bar().encode(
            x=alt.X("entity:N", title="Entity", axis=alt.Axis(labelAngle=-90), sort=top_entities),
            y=alt.Y("percent:Q", title="% Share of Mentions"),
            color=alt.Color("source:N", title="Source"),
            tooltip=["source", "entity", "percent"]
        ).properties(height=400)

        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No 'entity' column found in datasets.")
