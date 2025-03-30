import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import warnings

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    return engine


# ============= 从宽表提取数据 =============
def extract_data_from_db(tag, engine):
    query = f"SELECT * FROM tbl_wide_user_retention_active_{tag};"
    try:
        df = pd.read_sql(query, engine)
        if "new_users" in df.columns:
            df.rename(columns={"new_users": "users"}, inplace=True)
        return df.fillna(0)
    except Exception as e:
        print(f"数据提取失败: {e}")
        return None


# ============= 计算留存率及置信区间 =============
def calculate_retention(df):
    days = {"d1": 1, "d3": 3, "d7": 7, "d15": 15}
    results = []
    df = df[df["users"] > 0].copy()
    for _, row in df.iterrows():
        dt = row["dt"]
        try:
            variation = int(row["variation"])
        except:
            variation = row["variation"]
        users = row["users"]
        cov = row["coverage_ratio"] if "coverage_ratio" in row else None
        for day_key, day in days.items():
            if day_key not in row:
                continue
            retained = row[day_key]
            retention_rate = retained / users if users > 0 else 0
            se = np.sqrt(retention_rate * (1 - retention_rate) / users) if users > 0 else 0
            ci_lower = max(0, retention_rate - 1.96 * se)
            ci_upper = min(1, retention_rate + 1.96 * se)
            results.append({
                "dt": dt,
                "variation": variation,
                "day": day,
                "users": int(users),
                "retained": int(retained),
                "retention_rate": retention_rate,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "coverage_ratio": cov
            })
    return pd.DataFrame(results)


# ============= 贝叶斯 uplift + chance to win（按天） =============
def calculate_uplift_and_chance_to_win(result_df, n_samples=10000):
    control_df = result_df[result_df["variation"] == 0]
    experiment_df = result_df[result_df["variation"] != 0]
    comparison_results = []

    for day in result_df["day"].unique():
        for dt in result_df["dt"].unique():
            control_rows = control_df[(control_df["day"] == day) & (control_df["dt"] == dt)]
            if control_rows.empty:
                continue
            control_row = control_rows.iloc[0]
            alpha_c = control_row["retained"] + 1
            beta_c = control_row["users"] - control_row["retained"] + 1
            samples_c = np.random.beta(alpha_c, beta_c, n_samples)

            for variation in experiment_df["variation"].unique():
                exp_rows = experiment_df[
                    (experiment_df["day"] == day) &
                    (experiment_df["variation"] == variation) &
                    (experiment_df["dt"] == dt)
                    ]
                if exp_rows.empty:
                    continue
                exp_row = exp_rows.iloc[0]
                alpha_e = exp_row["retained"] + 1
                beta_e = exp_row["users"] - exp_row["retained"] + 1
                samples_e = np.random.beta(alpha_e, beta_e, n_samples)

                mean_c = samples_c.mean()
                mean_e = samples_e.mean()
                if mean_c != 0:
                    uplift = (mean_e - mean_c) / mean_c
                else:
                    uplift = 0

                uplift_samples = (samples_e - samples_c) / (mean_c if mean_c != 0 else 1)
                uplift_ci_lower = np.percentile(uplift_samples, 2.5)
                uplift_ci_upper = np.percentile(uplift_samples, 97.5)
                chance_to_win = np.mean(samples_e > samples_c)

                comparison_results.append({
                    "dt": dt,
                    "day": day,
                    "variation": variation,
                    "uplift": uplift,
                    "uplift_ci_lower": uplift_ci_lower,
                    "uplift_ci_upper": uplift_ci_upper,
                    "chance_to_win": chance_to_win
                })

    return pd.DataFrame(comparison_results)


# ============= 生成最终报告宽表（按天） =============
def generate_report(tag):
    engine = get_db_connection()
    df = extract_data_from_db(tag, engine)
    if df is None:
        return None, None

    retention_df = calculate_retention(df)

    # 排除首尾 dt（如不需要，可注释此逻辑）
    # unique_dates = sorted(retention_df["dt"].unique())
    # if len(unique_dates) > 2:
    #     retention_df = retention_df[~retention_df["dt"].isin([unique_dates[0], unique_dates[-1]])]

    uplift_df = calculate_uplift_and_chance_to_win(retention_df)

    # 对照组 day=1
    control_day1 = retention_df[(retention_df["variation"] == 0) & (retention_df["day"] == 1)]
    control_day1 = control_day1.rename(columns={
        "users": "对照组人数",
        "retention_rate": "对照组留存率",
        "ci_lower": "对照组_ci_lower",
        "ci_upper": "对照组_ci_upper"
    })[["dt", "对照组人数", "对照组留存率", "对照组_ci_lower", "对照组_ci_upper"]]

    # 实验组分日留存率
    exp_ret_pivot = retention_df[retention_df["variation"] != 0].pivot(
        index=["dt", "variation"],
        columns="day",
        values="retention_rate"
    ).reset_index().rename(columns={
        1: "d1留存率",
        3: "d3留存率",
        7: "d7留存率",
        15: "d15留存率"
    })

    # 实验组 day=1
    exp_day1 = retention_df[(retention_df["variation"] != 0) & (retention_df["day"] == 1)]
    exp_day1 = exp_day1.rename(columns={
        "users": "实验组人数",
        "retention_rate": "实验组留存率",
        "ci_lower": "exp_ci_lower",
        "ci_upper": "exp_ci_upper"
    })[["dt", "variation", "实验组人数", "实验组留存率", "exp_ci_lower", "exp_ci_upper", "coverage_ratio"]]

    # day=1 的 uplift & chance_to_win
    exp_uplift = uplift_df[(uplift_df["variation"] != 0) & (uplift_df["day"] == 1)]
    exp_uplift = exp_uplift[["dt", "variation", "uplift", "uplift_ci_lower", "uplift_ci_upper", "chance_to_win"]]

    # 合并
    exp_all = pd.merge(exp_ret_pivot, exp_day1, on=["dt", "variation"], how="left")
    exp_all = pd.merge(exp_all, exp_uplift, on=["dt", "variation"], how="left")
    final_df = pd.merge(exp_all, control_day1, on="dt", how="left")

    final_df = final_df.rename(columns={"coverage_ratio": "覆盖占比"})

    final_final = final_df[[
        "dt", "variation", "对照组人数", "对照组留存率", "实验组人数", "实验组留存率",
        "d1留存率", "d3留存率", "d7留存率", "d15留存率", "覆盖占比",
        "exp_ci_lower", "exp_ci_upper",
        "uplift", "uplift_ci_lower", "uplift_ci_upper", "chance_to_win"
    ]]
    return final_final, retention_df


# ============= 创建报告表结构 =============
def create_report_table(engine, tag):
    table_name = f"tbl_report_user_retention_active_{tag}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        dt DATE,
        variation INT,
        对照组人数 INT,
        对照组留存率 DOUBLE,
        实验组人数 INT,
        实验组留存率 DOUBLE,
        d1留存率 DOUBLE,
        d3留存率 DOUBLE,
        d7留存率 DOUBLE,
        d15留存率 DOUBLE,
        覆盖占比 DOUBLE,
        exp_ci_lower DOUBLE,
        exp_ci_upper DOUBLE,
        uplift DOUBLE,
        uplift_ci_lower DOUBLE,
        uplift_ci_upper DOUBLE,
        chance_to_win DOUBLE
    ) ENGINE=OLAP
    DUPLICATE KEY(dt, variation)
    DISTRIBUTED BY HASH(dt) BUCKETS 10
    PROPERTIES (
        "replication_num" = "3"
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SET query_timeout = 30000;"))
            conn.execute(text(create_table_query))
        print(f"表 {table_name} 已成功创建！")
    except SQLAlchemyError as e:
        print(f"表格创建失败: {e}")


# ============= 加载结果入库（按天） =============
def load_analysis_results(final_df, engine, table_name):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        print(f"✅ 表 {table_name} 已成功清空原有数据！")
        final_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=500,
            dtype={
                'dt': sqlalchemy.Date(),
                'variation': sqlalchemy.Integer(),
                '对照组人数': sqlalchemy.Integer(),
                '对照组留存率': sqlalchemy.Float(),
                '实验组人数': sqlalchemy.Integer(),
                '实验组留存率': sqlalchemy.Float(),
                'd1留存率': sqlalchemy.Float(),
                'd3留存率': sqlalchemy.Float(),
                'd7留存率': sqlalchemy.Float(),
                'd15留存率': sqlalchemy.Float(),
                '覆盖占比': sqlalchemy.Float(),
                'exp_ci_lower': sqlalchemy.Float(),
                'exp_ci_upper': sqlalchemy.Float(),
                'uplift': sqlalchemy.Float(),
                'uplift_ci_lower': sqlalchemy.Float(),
                'uplift_ci_upper': sqlalchemy.Float(),
                'chance_to_win': sqlalchemy.Float()
            }
        )
        print(f"数据已成功写入 {table_name} 中！")
    except SQLAlchemyError as e:
        print(f"数据库插入失败: {e}")
    except Exception as e:
        print(f"其他错误: {e}")


# ============= 主流程 =============
def main(tag):
    engine = get_db_connection()

    # 1) 创建“按天”报告表
    create_report_table(engine, tag)
    table_name = f"tbl_report_user_retention_active_{tag}"

    # 2) 生成“按天”报告 + 留存明细
    final_report, retention_df = generate_report(tag)
    if final_report is None or retention_df is None:
        print("生成报告失败。")
        return

    # 3) 写入“按天”报告结果
    load_analysis_results(final_report, engine, table_name)


if __name__ == "__main__":
    tag = "trans_es"  # 修改为你的 tag
    main(tag)
