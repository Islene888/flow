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

# ============= 读取广告数据 =============
def read_ad_data(tag, engine):
    table_name = f"tbl_report_ad_{tag}"
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        return df[df["variation"].notnull()].copy()
    except Exception as e:
        print(f"❌ 数据读取失败: {e}")
        return None

# ============= 贝叶斯胜率分析 =============
def bayesian_ad_analysis(df, tag, n_samples=10000):
    control = df[df["variation"] == "0"]
    if control.empty:
        print("❌ 未找到对照组 variation=0")
        return None

    control = control.iloc[0]
    control_users = control["total_active_users"]
    control_exp_users = control["ad_exposure_users"]
    control_ad_arpu = control["ad_arpu"]
    control_exp_rate = control["ad_exposure_rate"]

    # 贝塔分布用于曝光率
    alpha_c = control_exp_users + 1
    beta_c = control_users - control_exp_users + 1
    exp_rate_samples_c = np.random.beta(alpha_c, beta_c, n_samples)

    # 正态分布用于 ARPU
    arpu_samples_c = np.random.normal(loc=control_ad_arpu, scale=control_ad_arpu / 5, size=n_samples)

    results = []

    for _, row in df[df["variation"] != "0"].iterrows():
        var = row["variation"]
        users = row["total_active_users"]
        exp_users = row["ad_exposure_users"]
        arpu = row["ad_arpu"]
        exp_rate = row["ad_exposure_rate"]

        # 曝光率贝叶斯胜率
        alpha_e = exp_users + 1
        beta_e = users - exp_users + 1
        exp_rate_samples_e = np.random.beta(alpha_e, beta_e, n_samples)
        exp_rate_win = np.mean(exp_rate_samples_e > exp_rate_samples_c)

        # ARPU 胜率
        arpu_samples_e = np.random.normal(loc=arpu, scale=arpu / 5, size=n_samples)
        arpu_win = np.mean(arpu_samples_e > arpu_samples_c)

        results.append({
            "variation": var,
            "control_users": int(control_users),
            "control_ad_exposure_rate": round(control_exp_rate, 6),
            "control_ad_arpu": round(control_ad_arpu, 6),
            "exp_users": int(users),
            "exp_ad_exposure_rate": round(exp_rate, 6),
            "exp_ad_arpu": round(arpu, 6),
            "exposure_rate_uplift": round((exp_rate - control_exp_rate) / control_exp_rate, 6) if control_exp_rate > 0 else 0,
            "exposure_rate_chance_to_win": round(exp_rate_win, 6),
            "ad_arpu_uplift": round((arpu - control_ad_arpu) / control_ad_arpu, 6) if control_ad_arpu > 0 else 0,
            "ad_arpu_chance_to_win": round(arpu_win, 6),
            "experiment_tag": tag
        })

    return pd.DataFrame(results)

# ============= 写入结果表 =============
def write_results(df_result, tag, engine):
    table_name = f"tbl_report_ad_{tag}_bayes"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        control_users INT,
        control_ad_exposure_rate DOUBLE,
        control_ad_arpu DOUBLE,
        exp_users INT,
        exp_ad_exposure_rate DOUBLE,
        exp_ad_arpu DOUBLE,
        exposure_rate_uplift DOUBLE,
        exposure_rate_chance_to_win DOUBLE,
        ad_arpu_uplift DOUBLE,
        ad_arpu_chance_to_win DOUBLE,
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
        print(f"📊 广告贝叶斯分析结果已写入表 {table_name}")
        print(df_result)
    except Exception as e:
        print(f"❌ 写入表失败: {e}")

# ============= 主流程 =============
def main(tag):
    engine = get_db_connection()
    df = read_ad_data(tag, engine)
    if df is None or df.empty:
        print("❌ 没有读取到数据")
        return
    result_df = bayesian_ad_analysis(df, tag)
    if result_df is not None:
        write_results(result_df, tag, engine)

if __name__ == "__main__":
    main("trans_es")
