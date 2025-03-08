import sys
import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy


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
        print(f"✅ 数据从表 'tbl_wide_user_retention_{tag}' 成功提取！")
        # 如果存在 new_users 列，则重命名为 users 以统一字段名称
        if "new_users" in df.columns:
            df.rename(columns={"new_users": "users"}, inplace=True)
        return df.fillna(0)
    except Exception as e:
        print(f"🚨 数据提取失败: {e}")
        return None


# ============= 计算滚动留存率及置信区间 =============
def calculate_retention(df):
    """
    计算每个日期、分组（variation）下的各天滚动留存率及其置信区间。
    滚动留存率计算公式：
      滚动留存率 = (在注册日后第 n 天及之后任意一天登录的用户数 / 注册日新增用户总数)
    标准误 SE 的计算公式：
      SE = sqrt(滚动留存率 * (1 - 滚动留存率) / users)
    置信区间：
      (滚动留存率 - 1.96*SE, 滚动留存率 + 1.96*SE)

    其中各指标定义：
      - d1：次日滚动留存率
      - d3：3日滚动留存率
      - d7：7日滚动留存率
      - d15：15日滚动留存率
    """
    df = df[df["users"] > 0].copy()
    days = ["d1", "d3", "d7", "d15"]
    day_map = {"d1": 1, "d3": 3, "d7": 7, "d15": 15}
    results = []
    for _, row in df.iterrows():
        dt = row["dt"]
        variation = str(row["variation"])
        users = row["users"]
        for day in days:
            # 如果该行中没有对应的字段，则跳过
            if day not in row:
                continue
            day_num = day_map[day]
            # 注意：此处的数值已为滚动留存人数，即在注册日后第 n 天及之后任意一天登录的用户数
            retained = row[day]
            retention_rate = retained / users if users > 0 else 0
            se = np.sqrt(retention_rate * (1 - retention_rate) / users) if users > 0 else 0
            ci_lower = max(0, retention_rate - 1.96 * se)
            ci_upper = min(1, retention_rate + 1.96 * se)
            results.append({
                "dt": dt,
                "variation": variation,
                "users": int(users),
                "day": day_num,
                "retention_rate": round(retention_rate, 4),
                "ci_lower": round(ci_lower, 4),
                "ci_upper": round(ci_upper, 4)
            })
    return pd.DataFrame(results)


# ============= 计算 uplift 与统计检验 =============
def calculate_uplift_and_significance(result_df):
    """
    针对每个日期和天数，比较实验组与对照组的滚动留存率差异。
    计算公式：
      uplift = (r_exp - r_control) / r_control
      z = (r_exp - r_control) / sqrt(SE_control^2 + SE_exp^2)
      p = 2*(1 - Φ(|z|))
    这里采用 day==1 的数据作为代表（即次日滚动留存率）。
    """
    control_df = result_df[result_df["variation"] == "0"]
    experiment_df = result_df[result_df["variation"] != "0"]
    comparison_results = []
    for day in result_df["day"].unique():
        for dt in result_df["dt"].unique():
            control_row = control_df[(control_df["day"] == day) & (control_df["dt"] == dt)]
            if control_row.empty:
                continue
            r_control = control_row["retention_rate"].values[0]
            N_control = control_row["users"].values[0]
            se_control = np.sqrt(r_control * (1 - r_control) / N_control) if N_control > 0 else 0
            for variation in experiment_df["variation"].unique():
                exp_row = experiment_df[
                    (experiment_df["day"] == day) &
                    (experiment_df["variation"] == variation) &
                    (experiment_df["dt"] == dt)
                    ]
                if exp_row.empty:
                    continue
                r_exp = exp_row["retention_rate"].values[0]
                N_exp = exp_row["users"].values[0]
                se_exp = np.sqrt(r_exp * (1 - r_exp) / N_exp) if N_exp > 0 else 0
                uplift = (r_exp - r_control) / r_control if r_control > 0 else np.nan
                # 计算 uplift 的标准误（简单估算）
                se_uplift = np.sqrt((se_control ** 2) / (r_control ** 2) + (se_exp ** 2) / (
                            r_exp ** 2)) if r_control > 0 and r_exp > 0 else np.nan
                uplift_lower = uplift - 1.96 * se_uplift if not np.isnan(uplift) and not np.isnan(se_uplift) else np.nan
                uplift_upper = uplift + 1.96 * se_uplift if not np.isnan(uplift) and not np.isnan(se_uplift) else np.nan
                z = (r_exp - r_control) / np.sqrt(se_control ** 2 + se_exp ** 2) if (
                                                                                                se_control ** 2 + se_exp ** 2) > 0 else np.nan
                p = 2 * (1 - stats.norm.cdf(abs(z))) if not np.isnan(z) else np.nan
                comparison_results.append({
                    "dt": dt,
                    "day": day,
                    "variation": variation,
                    "control_rate": round(r_control, 4),
                    "exp_rate": round(r_exp, 4),
                    "uplift": round(uplift, 4) if not np.isnan(uplift) else np.nan,
                    "uplift_ci_lower": round(uplift_lower, 4) if not np.isnan(uplift_lower) else np.nan,
                    "uplift_ci_upper": round(uplift_upper, 4) if not np.isnan(uplift_upper) else np.nan,
                    "z": round(z, 4) if not np.isnan(z) else np.nan,
                    "p": round(p, 4) if not np.isnan(p) else np.nan
                })
    return pd.DataFrame(comparison_results)


# ============= 生成最终报告宽表 =============
def generate_report(tag):
    engine = get_db_connection()
    df = extract_data_from_db(tag, engine)
    if df is None:
        return

    # 计算滚动留存率长表（包含 d1, d3, d7, d15）及各自置信区间
    retention_df = calculate_retention(df)
    # 计算 uplift 与统计检验（此处基于 day==1 的数据计算，用于 uplift 等）
    uplift_df = calculate_uplift_and_significance(retention_df)

    # —— 对照组数据（variation == "0"） ——
    # 取对照组 day==1 的记录，此时 users 即为注册当日新增用户数，
    # 对照组滚动留存率 = d1 / users, 置信区间来源于 retention_df 中的 ci_lower 与 ci_upper
    control_day1 = retention_df[(retention_df["variation"] == "0") & (retention_df["day"] == 1)]
    control_day1 = control_day1.rename(columns={
        "users": "对照组人数",
        "retention_rate": "对照组滚动留存率",
        "ci_lower": "对照组_ci_lower",
        "ci_upper": "对照组_ci_upper"
    })[["dt", "对照组人数", "对照组滚动留存率", "对照组_ci_lower", "对照组_ci_upper"]]

    # —— 实验组数据（variation ≠ "0"） ——
    # 1. 将实验组各天滚动留存率 pivot 成宽格式（不包含人数），便于展示 d1, d3, d7, d15 的滚动留存率
    exp_ret_pivot = retention_df[retention_df["variation"] != "0"].pivot(index=["dt", "variation"], columns="day",
                                                                         values="retention_rate").reset_index()
    exp_ret_pivot = exp_ret_pivot.rename(columns={
        1: "d1(滚动留存率)",
        3: "d3(滚动留存率)",
        7: "d7(滚动留存率)",
        15: "d15(滚动留存率)"
    })
    # 2. 取实验组 day==1 的记录，获取当天实验组人数和实验组滚动留存率（即 d1 滚动留存率）
    exp_day1 = retention_df[(retention_df["variation"] != "0") & (retention_df["day"] == 1)]
    exp_day1 = exp_day1.rename(columns={
        "users": "实验组人数",
        "retention_rate": "实验组滚动留存率",
        "ci_lower": "exp_ci_lower",
        "ci_upper": "exp_ci_upper"
    })[["dt", "variation", "实验组人数", "实验组滚动留存率", "exp_ci_lower", "exp_ci_upper"]]
    # 3. 取实验组 uplift 数据（仅取 day==1 的记录）
    exp_uplift = uplift_df[(uplift_df["variation"] != "0") & (uplift_df["day"] == 1)]
    exp_uplift = exp_uplift.rename(columns={
        "uplift": "uplift",
        "uplift_ci_lower": "uplift_ci_lower",
        "uplift_ci_upper": "uplift_ci_upper",
        "z": "z",
        "p": "p"
    })[["dt", "variation", "uplift", "uplift_ci_lower", "uplift_ci_upper", "z", "p"]]

    # 合并实验组数据：以 dt 与 variation 为键，先合并 pivot 数据和 exp_day1，确保“实验组人数”取自 day==1 的记录
    exp_all = pd.merge(exp_ret_pivot, exp_day1, on=["dt", "variation"], how="left")
    # 合并 uplift 数据（按 dt, variation）
    exp_all = pd.merge(exp_all, exp_uplift, on=["dt", "variation"], how="left")

    # 合并对照组数据（按 dt），得到最终对照组与实验组数据的对比
    final_df = pd.merge(exp_all, control_day1, on="dt", how="left")

    # 生成显示字段：
    # 对照组置信区间：格式 "(下, 上)"
    final_df["对照组置信区间"] = "(" + final_df["对照组_ci_lower"].astype(str) + ", " + final_df[
        "对照组_ci_upper"].astype(str) + ")"
    # 实验组滚动留存率的置信区间：格式 "(下, 上)"
    final_df["实验组滚动留存率的置信区间"] = "(" + final_df["exp_ci_lower"].astype(str) + ", " + final_df[
        "exp_ci_upper"].astype(str) + ")"
    # uplift(置信区间)：格式 "(下, 上)"
    final_df["uplift(置信区间)"] = "(" + final_df["uplift_ci_lower"].astype(str) + ", " + final_df[
        "uplift_ci_upper"].astype(str) + ")"
    # (z,p)：将 z 和 p 值合并为一个字段，格式 "(z, p)"
    final_df["(z,p)"] = "(" + final_df["z"].astype(str) + ", " + final_df["p"].astype(str) + ")"

    # 最终选择字段及顺序：
    # dt, variation, 对照组人数, 对照组滚动留存率, 实验组人数, 实验组滚动留存率,
    # d1(滚动留存率), d3(滚动留存率), d7(滚动留存率), d15(滚动留存率),
    # 实验组滚动留存率的置信区间, uplift, uplift(置信区间), (z,p)
    final_final = final_df[[
        "dt",
        "variation",
        "对照组人数",
        "对照组滚动留存率",
        "实验组人数",
        "实验组滚动留存率",
        "d1(滚动留存率)",
        "d3(滚动留存率)",
        "d7(滚动留存率)",
        "d15(滚动留存率)",
        "实验组滚动留存率的置信区间",
        "uplift",
        "uplift(置信区间)",
        "(z,p)"
    ]]

    # 格式化滚动留存率字段为百分比（乘以 100 保留两位小数）
    for col in ["对照组滚动留存率", "实验组滚动留存率", "d1(滚动留存率)", "d3(滚动留存率)", "d7(滚动留存率)",
                "d15(滚动留存率)"]:
        final_final[col] = final_final[col].apply(lambda x: f"{x * 100:.2f}%" if pd.notnull(x) and x != 0 else "/")
    # uplift 转换为百分比格式
    final_final["uplift"] = final_final["uplift"].apply(
        lambda x: f"{x * 100:.2f}%" if pd.notnull(x) and x != 0 else "/")

