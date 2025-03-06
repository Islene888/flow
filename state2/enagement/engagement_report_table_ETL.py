# retention_analysis.py
import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy

# 设置数据库连接
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    return engine

# 运行 SQL 查询，获取数据
def extract_data_from_db(tag, engine):
    query = f"""
    SELECT 
        dt, 
        variation, 
        SUM(users) AS users,
        SUM(d1) AS d1,
        SUM(d2) AS d2,
        SUM(d3) AS d3,
        SUM(d4) AS d4,
        SUM(d5) AS d5,
        SUM(d6) AS d6,
        SUM(d7) AS d7,
        SUM(d8) AS d8,
        SUM(d9) AS d9,
        SUM(d10) AS d10,
        SUM(d11) AS d11,
        SUM(d12) AS d12,
        SUM(d13) AS d13,
        SUM(d14) AS d14,
        SUM(d15) AS d15
    FROM tbl_wide_user_retention_{tag}
    GROUP BY dt, variation
    ORDER BY dt ASC, CAST(variation AS UNSIGNED) ASC;
    """
    try:
        df = pd.read_sql(query, engine)
        print(f"✅ 数据从表 'tbl_wide_user_retention_{tag}' 成功提取！")
        return df.fillna(0)  # 处理空值
    except Exception as e:
        print(f"🚨 数据提取失败: {e}")
        return None

# 计算留存率（修复1：添加四位小数处理）
def calculate_retention(df):
    df = df[df["users"] > 0].copy()

    days = [f"d{i}" for i in range(1,16)]
    day_map = {f"d{i}": i for i in range(1, 16)}
    results = []

    for _, row in df.iterrows():
        dt = row["dt"]
        variation = row["variation"]
        users = row["users"]

        for day in days:
            day_num = day_map[day]
            retained = row[day]
            retention_rate = retained / users if users > 0 else 0
            se = np.sqrt((retention_rate * (1 - retention_rate)) / users) if users > 0 else 0

            ci_lower = max(0, retention_rate - 1.96 * se)
            ci_upper = min(1, retention_rate + 1.96 * se)

            # 添加四舍五入到四位小数
            results.append({
                "dt": dt,
                "day": day_num,
                "variation": str(variation),
                "users": int(users),
                "retained": int(retained),
                "retention_rate": round(retention_rate, 4),
                "ci_lower": round(ci_lower, 4),
                "ci_upper": round(ci_upper, 4)
            })

    return pd.DataFrame(results)

# 计算增长率（修复2：统一四位小数处理）
def calculate_uplift_and_significance(result_df):
    control_df = result_df[result_df["variation"] == "0"]
    experiment_df = result_df[result_df["variation"] != "0"]
    comparison_results = []

    for day in range(1, 16):
        for dt in result_df["dt"].unique():
            control_row = control_df[(control_df["day"] == day) & (control_df["dt"] == dt)]
            if control_row.empty:
                continue

            control_rate = control_row["retention_rate"].values[0]
            control_users = control_row["users"].values[0]
            control_se = np.sqrt((control_rate * (1 - control_rate)) / control_users) if control_users > 0 else 0

            for variation in experiment_df["variation"].unique():
                exp_row = experiment_df[
                    (experiment_df["day"] == day) &
                    (experiment_df["variation"] == variation) &
                    (experiment_df["dt"] == dt)
                ]
                if exp_row.empty:
                    continue

                exp_rate = exp_row["retention_rate"].values[0]
                exp_users = exp_row["users"].values[0]
                exp_se = np.sqrt((exp_rate * (1 - exp_rate)) / exp_users) if exp_users > 0 else 0

                # 计算逻辑并四舍五入
                uplift = (exp_rate - control_rate) / control_rate if control_rate > 0 else np.nan
                uplift_se = np.sqrt((control_se**2)/(control_rate**2) + (exp_se**2)/(exp_rate**2)) if (control_rate > 0 and exp_rate > 0) else 0
                uplift_lower = uplift - 1.96 * uplift_se if not np.isnan(uplift) else np.nan
                uplift_upper = uplift + 1.96 * uplift_se if not np.isnan(uplift) else np.nan

                z_score = (exp_rate - control_rate) / np.sqrt(control_se**2 + exp_se**2) if (control_se**2 + exp_se**2) > 0 else np.nan
                p_value = 2 * (1 - stats.norm.cdf(abs(z_score))) if not np.isnan(z_score) else np.nan

                comparison_results.append({
                    "dt": dt,
                    "day": day,
                    "variation": variation,
                    "control_rate": round(control_rate, 4),
                    "exp_rate": round(exp_rate, 4),
                    "uplift": round(uplift, 4) if not np.isnan(uplift) else np.nan,
                    "uplift_ci_lower": round(uplift_lower, 4) if not np.isnan(uplift_lower) else np.nan,
                    "uplift_ci_upper": round(uplift_upper, 4) if not np.isnan(uplift_upper) else np.nan,
                    "z_score": round(z_score, 4) if not np.isnan(z_score) else np.nan,
                    "p_value": round(p_value, 4) if not np.isnan(p_value) else np.nan
                })

    return pd.DataFrame(comparison_results)

# 创建报告表（保持不变）
def create_report_table(engine, tag):
    table_name2 = f"tbl_report_user_retention_{tag}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name2} (
        dt DATE,
        day INT,
        variation VARCHAR(200),
        users INT,
        retained INT,
        retention_rate DOUBLE,
        ci_lower DOUBLE,
        ci_upper DOUBLE,
        control_rate DOUBLE,
        exp_rate DOUBLE,
        uplift DOUBLE,
        uplift_ci_lower DOUBLE,
        uplift_ci_upper DOUBLE,
        z_score DOUBLE,
        p_value DOUBLE,
        retention_rate_baseline DOUBLE
    )
    ENGINE=OLAP
    DUPLICATE KEY(dt, day, variation)
    DISTRIBUTED BY HASH(dt) BUCKETS 10
    PROPERTIES (
        "replication_num" = "3"
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SET query_timeout = 30000;"))
            conn.execute(text(create_table_query))
        print(f"✅ report表 {table_name2} 已成功创建！")
    except SQLAlchemyError as e:
        print(f"🚨 表格创建失败: {e}")

# 数据加载（修复3：最终统一四舍五入）
def load_analysis_results(final_df, engine, table_name2):
    try:
        # 最终统一四舍五入（确保覆盖所有数值列）
        decimal_columns = [
            'retention_rate', 'ci_lower', 'ci_upper',
            'control_rate', 'exp_rate', 'uplift',
            'uplift_ci_lower', 'uplift_ci_upper',
            'z_score', 'p_value', 'retention_rate_baseline'
        ]
        final_df[decimal_columns] = final_df[decimal_columns].round(4)

        # 强制类型转换
        final_df = final_df.astype({
            'day': 'int32',
            'users': 'int32',
            'retained': 'int32',
            'variation': 'str'
        })

        # 清空原表
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name2}"))

        # 批量插入
        final_df.to_sql(
            name=table_name2,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500,
            dtype={
                'dt': sqlalchemy.Date(),
                'day': sqlalchemy.Integer(),
                'variation': sqlalchemy.String(200),
                'users': sqlalchemy.Integer(),
                'retained': sqlalchemy.Integer(),
                'retention_rate': sqlalchemy.Double(),
                'ci_lower': sqlalchemy.Double(),
                'ci_upper': sqlalchemy.Double(),
                'control_rate': sqlalchemy.Double(),
                'exp_rate': sqlalchemy.Double(),
                'uplift': sqlalchemy.Double(),
                'uplift_ci_lower': sqlalchemy.Double(),
                'uplift_ci_upper': sqlalchemy.Double(),
                'z_score': sqlalchemy.Double(),
                'p_value': sqlalchemy.Double(),
                'retention_rate_baseline': sqlalchemy.Double()
            }
        )
        print(f"✅ report表数据已成功写入 {table_name2} 中！")
    except SQLAlchemyError as e:
        print(f"🚨 数据库插入失败: {e}")
    except Exception as e:
        print(f"🚨 其他错误: {e}")

# 主流程（修复4：最终数据校验）
def main(tag):
    engine = get_db_connection()
    create_report_table(engine, tag)
    table_name2 = f"tbl_report_user_retention_{tag}"

    if (df := extract_data_from_db(tag, engine)) is None:
        return

    result_df = calculate_retention(df)
    comparison_df = calculate_uplift_and_significance(result_df)
    control_df = result_df[result_df["variation"] == "0"]
    baseline_df = control_df[["dt", "day", "retention_rate"]].rename(
        columns={"retention_rate": "retention_rate_baseline"}
    )

    # 合并数据
    final_df = pd.merge(
        result_df,
        comparison_df,
        on=["dt", "day", "variation"],
        how="left"
    ).merge(
        baseline_df,
        on=["dt", "day"],
        how="left"
    )

    # 最终字段处理
    final_df = final_df[[
        'dt', 'day', 'variation', 'users', 'retained',
        'retention_rate', 'ci_lower', 'ci_upper', 'control_rate',
        'exp_rate', 'uplift', 'uplift_ci_lower', 'uplift_ci_upper',
        'z_score', 'p_value', 'retention_rate_baseline'
    ]]



    load_analysis_results(final_df, engine, table_name2)

