import json
from pathlib import Path

import pandas as pd


OUTPUT_DIR = Path("output")
RESULT_PATH = OUTPUT_DIR / "result.json"


def load_cached_insights(path: Path = RESULT_PATH):
    if not path.exists():
        raise SystemExit(
            f"Cached MCP 결과가 없습니다: {path}\n"
            "먼저 python main.py 를 실행하여 result.json 을 생성하세요."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        insights = payload.get("insights")
    else:
        insights = payload
    if not isinstance(insights, list):
        raise SystemExit(f"result.json 형식이 잘못되었습니다: {path}")
    return insights


def resolve_dominant_group(demographics):
    if isinstance(demographics, str):
        return demographics
    if not isinstance(demographics, dict):
        return None

    flattened = {}
    for key, value in demographics.items():
        if isinstance(value, (int, float)):
            flattened[key] = float(value)
        elif isinstance(value, dict):
            for sub_key, sub_val in value.items():
                if isinstance(sub_val, (int, float)):
                    flattened[f"{key}.{sub_key}"] = float(sub_val)
    if not flattened:
        return None
    dominant_key = max(flattened.items(), key=lambda x: x[1])[0]
    return dominant_key


def insights_to_dataframe(insights):
    rows = []
    for rec in insights:
        metrics = rec.get("metrics", {})
        sales_metric = metrics.get("sales", {})
        analysis = rec.get("analysis", {})
        impact = analysis.get("impact", {})
        trend = analysis.get("trend", {}) if isinstance(analysis, dict) else {}

        demographics = rec.get("population", {}).get("demographics")
        dominant_demo = resolve_dominant_group(demographics)

        rows.append(
            {
                "spatial": rec["spatial"],
                "time": rec["time"],
                "source": rec.get("source"),
                "sales": sales_metric.get("current"),
                "baseline": sales_metric.get("baseline"),
                "impact_score": impact.get("impact_score"),
                "impact_level": impact.get("classification"),
                "foot_traffic": rec["population"].get("foot_traffic"),
                "trend_direction": trend.get("direction"),
                "dominant_demo": dominant_demo,
                "narrative": rec.get("narrative"),
            }
        )
    return pd.DataFrame(rows)


def create_charts(df):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib 미설치: 시각화를 건너뜁니다.")
        return []

    OUTPUT_DIR.mkdir(exist_ok=True)
    created = []

    # Impact bar chart
    top_df = df.dropna(subset=["impact_score"]).copy()
    if not top_df.empty:
        top_df["label"] = top_df["spatial"] + "@" + top_df["time"].astype(str)
        top = top_df.nlargest(20, "impact_score", keep="all")
        plt.figure(figsize=(10, 6))
        plt.barh(top["label"], top["impact_score"], color="coral")
        plt.xlabel("Impact Score")
        plt.ylabel("Spatial@Time")
        plt.title("Top Impact Scores")
        plt.tight_layout()
        impact_path = OUTPUT_DIR / "impact_scores.png"
        plt.savefig(impact_path)
        plt.close()
        created.append(impact_path)

    # Traffic vs Sales scatter
    scatter_df = df.dropna(subset=["foot_traffic", "sales"])
    if not scatter_df.empty:
        plt.figure(figsize=(8, 6))
        colors = scatter_df["impact_level"].map(
            {"high": "red", "moderate": "orange", "low": "green"}
        ).fillna("gray")
        plt.scatter(
            scatter_df["foot_traffic"],
            scatter_df["sales"],
            c=colors,
            alpha=0.6,
        )
        plt.xlabel("Foot Traffic")
        plt.ylabel("Sales")
        plt.title("Foot Traffic vs Sales")
        plt.tight_layout()
        scatter_path = OUTPUT_DIR / "traffic_vs_sales.png"
        plt.savefig(scatter_path)
        plt.close()
        created.append(scatter_path)

    return created


def run_streamlit_dashboard(df):
    try:
        import streamlit as st
        from streamlit.runtime.scriptrunner import get_script_run_ctx
    except ImportError:
        return False

    ctx = get_script_run_ctx()
    if ctx is None:
        return False

    st.set_page_config(page_title="MCP Insights Dashboard", layout="wide")
    st.title("MCP Insights Dashboard")
    st.caption("도메인별 정규화 데이터를 기반으로 산출된 인사이트를 시각화합니다.")

    df = df.copy()
    df["time_dt"] = pd.to_datetime(df["time"], errors="coerce")

    spatial_options = sorted(df["spatial"].dropna().unique())
    default_spatial = spatial_options[: min(5, len(spatial_options))]
    selected_spatial = st.sidebar.multiselect(
        "지역 필터",
        options=spatial_options,
        default=default_spatial,
    )
    source_options = sorted(df["source"].dropna().unique())
    selected_sources = st.sidebar.multiselect(
        "도메인 소스",
        options=source_options,
        default=source_options,
    )

    if df["time_dt"].notna().any():
        min_time = df["time_dt"].min()
        max_time = df["time_dt"].max()
    else:
        min_time = max_time = None

    if min_time and max_time and min_time != max_time:
        time_range = st.sidebar.slider(
            "기간 선택",
            min_value=min_time.to_pydatetime(),
            max_value=max_time.to_pydatetime(),
            value=(min_time.to_pydatetime(), max_time.to_pydatetime()),
        )
    else:
        time_range = None

    min_score = float(df["impact_score"].min() if df["impact_score"].notna().any() else 0)
    max_score = float(df["impact_score"].max() if df["impact_score"].notna().any() else 1)
    impact_threshold = st.sidebar.slider(
        "Impact Score 하한",
        min_value=min_score,
        max_value=max_score if max_score > min_score else min_score + 1,
        value=min_score,
        step=0.01 if max_score - min_score < 1 else 0.1,
    )

    filtered = df.copy()
    if selected_spatial:
        filtered = filtered[filtered["spatial"].isin(selected_spatial)]
    if selected_sources:
        filtered = filtered[filtered["source"].isin(selected_sources)]
    filtered = filtered[filtered["impact_score"].fillna(min_score - 1) >= impact_threshold]
    if time_range and filtered["time_dt"].notna().any():
        start, end = time_range
        filtered = filtered[
            (filtered["time_dt"] >= pd.Timestamp(start))
            & (filtered["time_dt"] <= pd.Timestamp(end))
        ]

    col1, col2, col3 = st.columns(3)
    col1.metric("총 인사이트 수", len(df))
    col2.metric("필터 적용 인사이트", len(filtered))
    high_count = (filtered["impact_level"] == "high").sum()
    col3.metric("High Impact 개수", int(high_count))

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Impact Score Top 10")
        top = filtered.dropna(subset=["impact_score"]).nlargest(10, "impact_score")
        if top.empty:
            st.info("표시할 데이터가 없습니다.")
        else:
            chart_data = top.set_index("time")[["impact_score"]]
            st.bar_chart(chart_data)

    with chart_cols[1]:
        st.subheader("Foot Traffic vs Sales")
        scatter = filtered.dropna(subset=["foot_traffic", "sales"])
        if scatter.empty:
            st.info("표시할 데이터가 없습니다.")
        else:
            st.scatter_chart(
                scatter,
                x="foot_traffic",
                y="sales",
                color="impact_level",
            )

    st.subheader("시간 경향")
    time_series = filtered.dropna(subset=["time_dt"]).copy()
    if time_series.empty:
        st.info("시간 정보를 가진 데이터가 부족합니다.")
    else:
        grouped = (
            time_series.groupby("time_dt")[["sales", "foot_traffic", "impact_score"]]
            .mean()
            .rename(columns={"sales": "avg_sales", "foot_traffic": "avg_traffic"})
        )
        st.line_chart(grouped)

    st.subheader("도메인별 비중")
    domain_counts = filtered["source"].value_counts()
    if domain_counts.empty:
        st.info("도메인 데이터가 없습니다.")
    else:
        st.bar_chart(domain_counts)

    st.subheader("세부 인사이트")
    st.dataframe(
        filtered[
            [
                "spatial",
                "time",
                "source",
                "impact_level",
                "impact_score",
                "sales",
                "baseline",
                "foot_traffic",
                "trend_direction",
                "dominant_demo",
                "narrative",
            ]
        ]
    )
    st.download_button(
        "인사이트 JSON 다운로드",
        data=filtered.to_json(orient="records", force_ascii=False),
        file_name="insights_filtered.json",
        mime="application/json",
    )
    return True


def export_outputs(df):
    OUTPUT_DIR.mkdir(exist_ok=True)
    out_json = OUTPUT_DIR / "insights.json"
    export_df = df.drop(columns=["time_dt"], errors="ignore")
    export_df.to_json(out_json, orient="records", force_ascii=False)
    print(f"인사이트 JSON 저장: {out_json}")


def main_cli(df):
    print(f"총 {len(df)}개의 인사이트를 DataFrame으로 변환했습니다.")
    chart_paths = create_charts(df)
    if chart_paths:
        for path in chart_paths:
            print(f"차트를 생성했습니다: {path}")
    else:
        print("생성할 차트가 없습니다.")
    export_outputs(df)


def main():
    insights = load_cached_insights()
    df = insights_to_dataframe(insights)
    if run_streamlit_dashboard(df):
        return
    main_cli(df)


if __name__ == "__main__":
    main()
