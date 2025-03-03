# retention_analysis.py
import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
from sqlalchemy.exc import SQLAlchemyError

# 设置数据库连接
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@18.188.196.105:9030/flow_ab_test"
    engine = create_engine(DATABASE_URL)
    return engine


# 运行 SQL 查询，获取数据
def extract_data_from_db(tag, engine):

    query = f"""
    SELECT * FROM tbl_wide_user_retention_{tag}
    ORDER BY dt ASC, CAST(variation AS UNSIGNED) ASC;
    """
    try:
        df = pd.read_sql(query, engine)
        print(f"✅ 数据从表 'tbl_wide_user_retention_{tag}' 成功提取！")
        return df
    except Exception as e:
        print(f"🚨 数据提取失败: {e}")
        return None

# 计算留存率
def calculate_retention(df):
    days = ["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10", "d11", "d12", "d13", "d14", "d15"]
    day_map = {f"d{i}": str(i) for i in range(1, 16)}
    results = []

    for _, row in df.iterrows():
        dt = row["dt"]
        variation = row["variation"]
        users = row["users"]

        if users > 0:
            for day in days:
                day_num = day_map[day]
                retained = row[day]
                retention_rate = retained / users if users > 0 else 0
                se = np.sqrt((retention_rate * (1 - retention_rate)) / users) if users > 0 else 0

                ci_lower = retention_rate - 1.96 * se
                ci_upper = retention_rate + 1.96 * se

                results.append({
                    "dt": dt,
                    "day": day_num,
                    "variation": variation,
                    "users": users,
                    "retained": retained,
                    "retention_rate": round(retention_rate, 4),
                    "ci_lower": round(ci_lower, 4),
                    "ci_upper": round(ci_upper, 4)
                })

    result_df = pd.DataFrame(results)
    return result_df


# 计算对照组与实验组的增长率和置信区间
def calculate_uplift_and_significance(result_df):
    control_df = result_df[result_df["variation"] == "0"]
    experiment_df = result_df[result_df["variation"] != "0"]

    comparison_results = []
    for day in range(1, 16):
        for dt in result_df["dt"].unique():
            control_row = control_df[(control_df["day"] == str(day)) & (control_df["dt"] == dt)]
            if control_row.empty:
                continue

            control_rate = control_row["retention_rate"].values[0]
            control_se = np.sqrt((control_rate * (1 - control_rate)) / control_row["users"].values[0]) if \
                control_row["users"].values[0] > 0 else 0

            for variation in experiment_df["variation"].unique():
                exp_row = experiment_df[
                    (experiment_df["day"] == str(day)) & (experiment_df["variation"] == variation) & (
                            experiment_df["dt"] == dt)]
                if exp_row.empty:
                    continue

                exp_rate = exp_row["retention_rate"].values[0]
                exp_se = np.sqrt((exp_rate * (1 - exp_rate)) / exp_row["users"].values[0]) if exp_row["users"].values[
                                                                                                  0] > 0 else 0

                if control_rate == 0 or exp_rate == 0:
                    uplift, uplift_se, z_score, p_value, uplift_lower, uplift_upper = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
                else:
                    uplift = (exp_rate - control_rate) / control_rate
                    uplift_se = np.sqrt((control_se / control_rate) ** 2 + (
                            exp_se / exp_rate) ** 2) if control_rate > 0 and exp_rate > 0 else 0

                    uplift_lower = uplift - 1.96 * uplift_se
                    uplift_upper = uplift + 1.96 * uplift_se

                    z_score = (exp_rate - control_rate) / np.sqrt(control_se ** 2 + exp_se ** 2) if (
                                                                                                            control_se ** 2 + exp_se ** 2) > 0 else np.nan
                    p_value = 2 * (1 - stats.norm.cdf(abs(z_score))) if not np.isnan(z_score) else np.nan

                comparison_results.append({
                    "dt": dt,
                    "day": str(day),
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


# 创建报告表
def create_report_table(engine, tag):
    table_name2 = f"tbl_report_user_retention_{tag}"  # 生成表名

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name2} (
        dt DATE,
        day INT,
        variation VARCHAR(255),
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
    );
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
        print(f"✅ report表 {table_name2} 已成功创建！")
    except SQLAlchemyError as e:
        print(f"🚨 宽表数据库表格创建失败: {e}")


# 合并数据，插入数据库
def load_analysis_results(final_df, engine, table_name2):
    try:
        final_df.to_sql(
            name=table_name2,  # 使用动态表名
            con=engine,
            if_exists='append',
            index=False,
            method='multi',  # 批量插入提升性能
            chunksize=500  # 根据需要调整批次大小
        )
        print(f"✅ report表数据已成功写入 {table_name2} 中！")
    except SQLAlchemyError as e:
        print(f"🚨 数据库插入失败: {e}")


# 主流程
def main(tag):
    # 获取数据库连接
    engine = get_db_connection()

    # 创建报告表
    table_name2 = f"tbl_report_user_retention_{tag}"  # 生成表名
    create_report_table(engine, tag)

    # 提取数据
    df = extract_data_from_db(tag, engine)
    if df is None:
        return

    # 计算留存率
    result_df = calculate_retention(df)

    # 计算对照组与实验组的增长率及置信区间
    comparison_df = calculate_uplift_and_significance(result_df)

    # 创建 baseline_retention_rate
    control_df = result_df[result_df["variation"] == "0"]
    baseline_retention_rate = control_df[["dt", "day", "retention_rate"]].drop_duplicates()


    if comparison_df.empty:
        print("⚠️ 警告：计算出的 comparison_df 为空，请检查原始数据中是否存在对照组 (variation == '0') 的数据。")
        # 根据业务需求，可以选择直接使用 result_df 或者构造一个空的 comparison_df
        # 这里我们构造一个具有相同列的空 DataFrame
        comparison_df = pd.DataFrame(columns=["dt", "day", "variation", "control_rate", "exp_rate",
                                              "uplift", "uplift_ci_lower", "uplift_ci_upper", "z_score", "p_value"])

    # 合并 baseline_retention_rate 到原始结果中
    final_df = pd.merge(result_df, comparison_df, on=["dt", "day", "variation"], how="left")
    final_df = pd.merge(final_df, baseline_retention_rate, on=["dt", "day"], how="left", suffixes=("", "_baseline"))

    # 插入分析结果到数据库
    load_analysis_results(final_df, engine, table_name2)
