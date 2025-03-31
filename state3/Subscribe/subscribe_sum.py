
import urllib.parse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import sqlalchemy
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    return engine

# ============= 读取每日订阅宽表数据 =============
def read_subscribe_data(tag, engine):
    table_name = f"tbl_report_subscribe_metrics_{tag}"
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        return df[df["variation"].notnull()].copy()
    except Exception as e:
        print(f"❌ 数据读取失败: {e}")
        return None

# ============= 贝叶斯分析核心逻辑 =============
def bayesian_subscribe_analysis(df, tag, n_samples=10000):
    df = df[df["experiment_user_count"] > 0].copy()
    if df.empty:
        print("❌ 没有有效数据可用于分析")
        return None

    grouped = df.groupby("variation", as_index=False).agg({
        "experiment_user_count": "sum",
        "new_subscribe_events": "sum",
        "subscribe_arpu": "mean"
    })

    control = grouped[grouped["variation"] == "0"]
    if control.empty:
        print("❌ 没有找到对照组 variation=0")
        return None

    control = control.iloc[0]
    alpha_c = control["new_subscribe_events"] + 1
    beta_c = control["experiment_user_count"] - control["new_subscribe_events"] + 1
    rate_samples_c = np.random.beta(alpha_c, beta_c, n_samples)
    arpu_samples_c = np.random.normal(loc=control["subscribe_arpu"], scale=control["subscribe_arpu"] / 5, size=n_samples)

    results = []

    for _, row in grouped[grouped["variation"] != "0"].iterrows():
        var = row["variation"]
        alpha_e = row["new_subscribe_events"] + 1
        beta_e = row["experiment_user_count"] - row["new_subscribe_events"] + 1
        rate_samples_e = np.random.beta(alpha_e, beta_e, n_samples)
        rate_win = np.mean(rate_samples_e > rate_samples_c)

        arpu_samples_e = np.random.normal(loc=row["subscribe_arpu"], scale=row["subscribe_arpu"] / 5, size=n_samples)
        arpu_win = np.mean(arpu_samples_e > arpu_samples_c)

        results.append({
            "variation": var,
            "control_users": int(control["experiment_user_count"]),
            "control_new_subscribe_rate": round(alpha_c / (alpha_c + beta_c), 6),
            "control_subscribe_arpu": round(control["subscribe_arpu"], 6),
            "exp_users": int(row["experiment_user_count"]),
            "exp_new_subscribe_rate": round(alpha_e / (alpha_e + beta_e), 6),
            "exp_subscribe_arpu": round(row["subscribe_arpu"], 6),
            "subscribe_rate_uplift": round((row["new_subscribe_events"] / row["experiment_user_count"] - control["new_subscribe_events"] / control["experiment_user_count"]) / (control["new_subscribe_events"] / control["experiment_user_count"]), 6),
            "subscribe_rate_chance_to_win": round(rate_win, 6),
            "subscribe_arpu_uplift": round((row["subscribe_arpu"] - control["subscribe_arpu"]) / control["subscribe_arpu"], 6),
            "subscribe_arpu_chance_to_win": round(arpu_win, 6),
            "experiment_tag": tag
        })

    return pd.DataFrame(results)


# ============= 写入分析结果 =============
def write_results(df_result, tag, engine):
    table_name = f"tbl_report_subscribe_{tag}_bayes"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        control_users INT,
        control_new_subscribe_rate DOUBLE,
        control_subscribe_arpu DOUBLE,
        exp_users INT,
        exp_new_subscribe_rate DOUBLE,
        exp_subscribe_arpu DOUBLE,
        subscribe_rate_uplift DOUBLE,
        subscribe_rate_chance_to_win DOUBLE,
        subscribe_arpu_uplift DOUBLE,
        subscribe_arpu_chance_to_win DOUBLE,
        experiment_tag VARCHAR(255)
    ) ENGINE=OLAP
    DUPLICATE KEY(variation)
    DISTRIBUTED BY HASH(variation) BUCKETS 10
    PROPERTIES ("replication_num" = "3");
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SET query_timeout = 30000"))
            conn.execute(text(create_table_query))
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))

        df_result.to_sql(table_name, con=engine, if_exists="append", index=False, method='multi', chunksize=500)
        print(f"📊 订阅贝叶斯分析结果已写入表 {table_name}")
        print(df_result)
    except Exception as e:
        print(f"❌ 写入失败: {e}")

# ============= 主流程 =============
def main(tag):
    engine = get_db_connection()
    df = read_subscribe_data(tag, engine)
    if df is None or df.empty:
        print("❌ 无订阅数据")
        return
    result_df = bayesian_subscribe_analysis(df, tag)
    if result_df is not None:
        write_results(result_df, tag, engine)

if __name__ == "__main__":
    main("trans_pt")