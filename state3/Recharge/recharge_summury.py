import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    return engine

# ============= 读取充值指标表 =============
def read_recharge_data(tag, engine):
    table_name = f"tbl_report_recharge_{tag}"
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        return df[df["variation"].notnull()].copy()
    except Exception as e:
        print(f"❌ 数据读取失败: {e}")
        return None

# ============= 贝叶斯胜率计算 =============
def bayesian_analysis(df, tag, n_samples=10000):
    control = df[df["variation"] == "0"]
    if control.empty:
        print("❌ 未找到对照组 variation=0")
        return

    control = control.iloc[0]
    control_users = control["total_active_users"]
    control_conv = control_users * control["recharge_conversion_rate"]
    control_revenue = control["total_recharge_revenue"]
    control_arpu = control["recharge_ARPU"]

    alpha_c = control_conv + 1
    beta_c = control_users - control_conv + 1
    conversion_samples_c = np.random.beta(alpha_c, beta_c, n_samples)

    # 假设 ARPU 为正态分布
    arpu_samples_c = np.random.normal(loc=control_arpu, scale=control_arpu / 5, size=n_samples)

    results = []

    for _, row in df[df["variation"] != "0"].iterrows():
        var = row["variation"]
        users = row["total_active_users"]
        conv = users * row["recharge_conversion_rate"]
        revenue = row["total_recharge_revenue"]
        arpu = row["recharge_ARPU"]

        # 转化率
        alpha_e = conv + 1
        beta_e = users - conv + 1
        conversion_samples_e = np.random.beta(alpha_e, beta_e, n_samples)
        conv_chance_to_win = np.mean(conversion_samples_e > conversion_samples_c)

        # ARPU
        arpu_samples_e = np.random.normal(loc=arpu, scale=arpu / 5, size=n_samples)
        arpu_chance_to_win = np.mean(arpu_samples_e > arpu_samples_c)

        results.append({
            "variation": var,
            "control_users": int(control_users),
            "control_conversion_rate": round(control["recharge_conversion_rate"], 6),
            "control_ARPU": round(control_arpu, 6),
            "exp_users": int(users),
            "exp_conversion_rate": round(row["recharge_conversion_rate"], 6),
            "exp_ARPU": round(arpu, 6),
            "conversion_uplift": round((row["recharge_conversion_rate"] - control["recharge_conversion_rate"]) / control["recharge_conversion_rate"], 6) if control["recharge_conversion_rate"] > 0 else 0,
            "conversion_chance_to_win": round(conv_chance_to_win, 6),
            "ARPU_uplift": round((arpu - control_arpu) / control_arpu, 6) if control_arpu > 0 else 0,
            "ARPU_chance_to_win": round(arpu_chance_to_win, 6),
            "experiment_tag": tag
        })

    return pd.DataFrame(results)

# ============= 写入结果表 =============
def write_results_to_db(result_df, tag, engine):
    table_name = f"tbl_report_recharge_{tag}_bayes"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        control_users INT,
        control_conversion_rate DOUBLE,
        control_ARPU DOUBLE,
        exp_users INT,
        exp_conversion_rate DOUBLE,
        exp_ARPU DOUBLE,
        conversion_uplift DOUBLE,
        conversion_chance_to_win DOUBLE,
        ARPU_uplift DOUBLE,
        ARPU_chance_to_win DOUBLE,
        experiment_tag VARCHAR(255)
    ) ENGINE=OLAP
    DUPLICATE KEY(variation)
    DISTRIBUTED BY HASH(variation) BUCKETS 10
    PROPERTIES ("replication_num" = "3");
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SET query_timeout = 30000;"))
            conn.execute(text(create_table_query))
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        result_df.to_sql(table_name, con=engine, if_exists="append", index=False, method='multi', chunksize=500)
        print(f"📊 贝叶斯胜率结果已写入 {table_name}")
        print(result_df)
    except Exception as e:
        print(f"❌ 写入失败: {e}")

# ============= 主流程 =============
def main(tag):
    engine = get_db_connection()
    df = read_recharge_data(tag, engine)
    if df is None or df.empty:
        print("❌ 没有读取到数据")
        return
    result_df = bayesian_analysis(df, tag)
    if result_df is not None:
        write_results_to_db(result_df, tag, engine)

if __name__ == "__main__":
    main("trans_es")
