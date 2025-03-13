import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
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
    query = f"SELECT * FROM tbl_wide_user_retention_{tag};"
    try:
        df = pd.read_sql(query, engine)
        # 将 new_users 重命名为 users，方便后续计算；coverage_ratio 字段直接保留
        if "new_users" in df.columns:
            df.rename(columns={"new_users": "users"}, inplace=True)
        return df.fillna(0)
    except Exception as e:
        print(f"数据提取失败: {e}")
        return None


# ============= 计算留存率及置信区间 =============
def calculate_retention(df):
    # 定义列名与对应天数映射
    days = {"d1": 1, "d3": 3, "d7": 7, "d15": 15}
    results = []
    df = df[df["users"] > 0].copy()
    for _, row in df.iterrows():
        dt = row["dt"]
        # 尝试将 variation 转为 int（对照组应为 0 ）
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
                "day": day,  # 数值：1, 3, 7, 15
                "users": int(users),
                "retained": int(retained),
                "retention_rate": retention_rate,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "coverage_ratio": cov  # 保留覆盖占比字段（数值型）
            })
    result_df = pd.DataFrame(results)
    return result_df


# ============= 计算 uplift 与统计检验 =============
def calculate_uplift_and_significance(result_df):
    # 对照组：variation==0（数值型）
    control_df = result_df[result_df["variation"] == 0]
    experiment_df = result_df[result_df["variation"] != 0]
    comparison_results = []
    for day in result_df["day"].unique():
        for dt in result_df["dt"].unique():
            control_rows = control_df[(control_df["day"] == day) & (control_df["dt"] == dt)]
            if control_rows.empty:
                continue
            control_row = control_rows.iloc[0]
            r_control = control_row["retention_rate"]
            N_control = control_row["users"]
            se_control = np.sqrt(r_control * (1 - r_control) / N_control) if N_control > 0 else 0
            for variation in experiment_df["variation"].unique():
                exp_rows = experiment_df[(experiment_df["day"] == day) &
                                         (experiment_df["variation"] == variation) &
                                         (experiment_df["dt"] == dt)]
                if exp_rows.empty:
                    continue
                exp_row = exp_rows.iloc[0]
                r_exp = exp_row["retention_rate"]
                N_exp = exp_row["users"]
                se_exp = np.sqrt(r_exp * (1 - r_exp) / N_exp) if N_exp > 0 else 0
                uplift = (r_exp - r_control) / r_control if r_control > 0 else np.nan
                se_uplift = np.sqrt(se_control ** 2 + se_exp ** 2) if r_control > 0 and r_exp > 0 else np.nan
                z = (r_exp - r_control) / se_uplift if se_uplift > 0 else np.nan
                p = 2 * (1 - stats.norm.cdf(abs(z))) if not np.isnan(z) else np.nan
                comparison_results.append({
                    "dt": dt,
                    "day": day,
                    "variation": variation,
                    "uplift": uplift,
                    "uplift_ci_lower": uplift - 1.96 * se_uplift if not np.isnan(uplift) and not np.isnan(
                        se_uplift) else np.nan,
                    "uplift_ci_upper": uplift + 1.96 * se_uplift if not np.isnan(uplift) and not np.isnan(
                        se_uplift) else np.nan,
                    "z": z,
                    "p": p
                })
    return pd.DataFrame(comparison_results)


# ============= 生成最终报告宽表 =============
def generate_report(tag):
    engine = get_db_connection()
    df = extract_data_from_db(tag, engine)
    if df is None:
        return None

    # 计算留存率（数值型），保留覆盖占比字段
    retention_df = calculate_retention(df)

    # --- 新增逻辑：去掉实验的第一天和最后一天的数据 ---
    unique_dates = sorted(retention_df["dt"].unique())
    if len(unique_dates) > 2:
        retention_df = retention_df[~retention_df["dt"].isin([unique_dates[0], unique_dates[-1]])]

    # 基于过滤后的 retention_df 计算 uplift
    uplift_df = calculate_uplift_and_significance(retention_df)

    # —— 对照组数据（variation == 0），取 day==1 的记录 ——
    control_day1 = retention_df[(retention_df["variation"] == 0) & (retention_df["day"] == 1)]
    control_day1 = control_day1.rename(columns={
        "users": "对照组人数",
        "retention_rate": "对照组留存率",
        "ci_lower": "对照组_ci_lower",
        "ci_upper": "对照组_ci_upper"
    })[["dt", "对照组人数", "对照组留存率", "对照组_ci_lower", "对照组_ci_upper"]]

    # —— 实验组数据（variation ≠ 0） ——
    # 1. 将实验组各天留存率 pivot 成宽格式，便于展示 d1, d3, d7, d15 的留存率
    exp_ret_pivot = retention_df[retention_df["variation"] != 0].pivot(
        index=["dt", "variation"],
        columns="day",
        values="retention_rate"
    ).reset_index()
    exp_ret_pivot = exp_ret_pivot.rename(columns={
        1: "d1留存率",
        3: "d3留存率",
        7: "d7留存率",
        15: "d15留存率"
    })
    # 2. 取实验组 day==1 的记录，获取当天实验组人数、实验组留存率以及覆盖占比
    exp_day1 = retention_df[(retention_df["variation"] != 0) & (retention_df["day"] == 1)]
    exp_day1 = exp_day1.rename(columns={
        "users": "实验组人数",
        "retention_rate": "实验组留存率",
        "ci_lower": "exp_ci_lower",
        "ci_upper": "exp_ci_upper"
    })[["dt", "variation", "实验组人数", "实验组留存率", "exp_ci_lower", "exp_ci_upper", "coverage_ratio"]]
    # 3. 取实验组 uplift 数据（仅取 day==1 的记录）
    exp_uplift = uplift_df[(uplift_df["variation"] != 0) & (uplift_df["day"] == 1)]
    exp_uplift = exp_uplift[["dt", "variation", "uplift", "uplift_ci_lower", "uplift_ci_upper", "z", "p"]]

    # 合并实验组数据：以 dt 与 variation 为键，先合并 pivot 数据和 exp_day1，再合并 uplift 数据和对照组数据
    exp_all = pd.merge(exp_ret_pivot, exp_day1, on=["dt", "variation"], how="left")
    exp_all = pd.merge(exp_all, exp_uplift, on=["dt", "variation"], how="left")
    final_df = pd.merge(exp_all, control_day1, on="dt", how="left")

    # 重命名 coverage_ratio 为 覆盖占比（保留数值，不做百分比字符串格式化）
    final_df = final_df.rename(columns={"coverage_ratio": "覆盖占比"})

    # 最终选择字段及顺序（全部保持数值型）
    final_final = final_df[[
        "dt",
        "variation",
        "对照组人数",
        "对照组留存率",
        "实验组人数",
        "实验组留存率",
        "d1留存率",
        "d3留存率",
        "d7留存率",
        "d15留存率",
        "覆盖占比",
        "exp_ci_lower",
        "exp_ci_upper",
        "uplift",
        "uplift_ci_lower",
        "uplift_ci_upper",
        "z",
        "p"
    ]]
    return final_final


# ============= 创建报告表（数据库表结构） =============
def create_report_table(engine, tag):
    table_name = f"tbl_report_user_retention_{tag}"
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
        z DOUBLE,
        p DOUBLE
    )
    ENGINE=OLAP
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


# ============= 数据加载，将最终报告写入数仓 =============
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
                'z': sqlalchemy.Float(),
                'p': sqlalchemy.Float()
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
    create_report_table(engine, tag)
    table_name = f"tbl_report_user_retention_{tag}"
    final_report = generate_report(tag)
    if final_report is None:
        print("生成报告失败。")
        return
    load_analysis_results(final_report, engine, table_name)


if __name__ == "__main__":
    tag = "backend"  # 根据实际标签修改
    main(tag)
