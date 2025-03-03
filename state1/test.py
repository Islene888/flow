import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
from sqlalchemy.exc import SQLAlchemyError

# ✅ 解决 Matplotlib 中文乱码
matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False

# 对密码进行 URL 编码
password = urllib.parse.quote_plus("flowgpt@2024.com")

# 构造数据库连接 URL
DATABASE_URL = f"mysql+pymysql://bigdata:{password}@18.188.196.105:9030/flow_test"

# 创建数据库连接
engine = create_engine(DATABASE_URL)

# SQL 查询（创建新表 - 用户留存率结果表）
create_table_query = """
CREATE TABLE IF NOT EXISTS tbl_user_engagement_filtered2 (
    dt DATE,
    variation VARCHAR(255),
    users INT,
    d1 INT,
    d2 INT,
    d3 INT,
    d4 INT,
    d5 INT,
    d6 INT,
    d7 INT,
    d8 INT,
    d9 INT,
    d10 INT,
    d11 INT,
    d12 INT,
    d13 INT,
    d14 INT,
    d15 INT,
    d16 INT
);

CREATE TABLE IF NOT EXISTS tbl_new_retention_results2 (
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
    uplift DOUBLE,  -- 添加 uplift 列
    uplift_ci_lower DOUBLE,  -- 添加 uplift_ci_lower 列
    uplift_ci_upper DOUBLE,  -- 添加 uplift_ci_upper 列
    z_score DOUBLE,  -- 添加 z_score 列
    p_value DOUBLE,  -- 添加 p_value 列
    retention_rate_baseline DOUBLE
);
"""

# 执行查询并创建表
with engine.connect() as conn:
    conn.execute(text(create_table_query))

# 执行插入查询
insert_query = """
INSERT INTO tbl_user_engagement_filtered2 (dt, variation, users, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15, d16)
SELECT 
    u.first_visit_date AS dt, 
    e.variation, 
    COUNT(DISTINCT u.user_id) AS users,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 1 DAY) THEN a.user_id END) AS d1,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 2 DAY) THEN a.user_id END) AS d2,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 3 DAY) THEN a.user_id END) AS d3,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 4 DAY) THEN a.user_id END) AS d4,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 5 DAY) THEN a.user_id END) AS d5,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 6 DAY) THEN a.user_id END) AS d6,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 7 DAY) THEN a.user_id END) AS d7,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 8 DAY) THEN a.user_id END) AS d8,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 9 DAY) THEN a.user_id END) AS d9,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 10 DAY) THEN a.user_id END) AS d10,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 11 DAY) THEN a.user_id END) AS d11,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 12 DAY) THEN a.user_id END) AS d12,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 13 DAY) THEN a.user_id END) AS d13,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 14 DAY) THEN a.user_id END) AS d14,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 15 DAY) THEN a.user_id END) AS d15,
    COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL 16 DAY) THEN a.user_id END) AS d16
FROM
    (SELECT
        user_id,
        DATE(first_visit_date) AS first_visit_date
    FROM
        flow_wide_info.tbl_wide_user_first_visit_app_info
    WHERE
        first_visit_date BETWEEN '2024-12-17' AND '2025-01-02') u
LEFT JOIN
    (SELECT
        u.user_id,
        u.first_visit_date,
        DATE(FROM_UNIXTIME(a.ingest_timestamp / 1000, '%Y-%m-%d')) AS active_date
    FROM
        flow_wide_info.tbl_wide_user_first_visit_app_info u
    JOIN
        flow_wide_info.tbl_wide_backend_detail_hi a ON u.user_id = a.user_id
    WHERE
        a.event_name = 'Chat_LLM'
        AND a.device_type = 'MOBILE'
        AND DATE(FROM_UNIXTIME(a.ingest_timestamp / 1000, '%Y-%m-%d')) BETWEEN u.first_visit_date AND '2025-01-02') a
ON u.user_id = a.user_id
LEFT JOIN
    (SELECT
        user_id,
        CAST(variation_id AS CHAR) AS variation
    FROM
        flow_wide_info.tbl_wide_experiment_assignment_hi
    WHERE
        experiment_id = 'mobile-non-claude-11'
        AND timestamp_assigned BETWEEN '2024-12-17 12:00:00' AND '2025-01-02 12:00:00') e
ON u.user_id = e.user_id
GROUP BY
    u.first_visit_date, e.variation
ORDER BY 
    u.first_visit_date;
"""

# 执行查询并插入数据
with engine.connect() as conn:
    conn.execute(text(insert_query))

print("SQL query executed successfully.")

# 运行 SQL 查询，获取数据
query = """
SELECT * FROM tbl_user_engagement_filtered2
ORDER BY dt ASC, CAST(variation AS UNSIGNED) ASC;
"""
df = pd.read_sql(query, engine)

# 确保 `variation` 是字符串类型
df["variation"] = df["variation"].astype(str)

# 计算每天的留存率
days = ["d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "d10", "d11", "d12", "d13", "d14", "d15"]

# 替换 "d1", "d2", ..., "d15" 为对应的阿拉伯数字 1, 2, ..., 15
day_map = {f"d{i}": str(i) for i in range(1, 16)}

results = []

for _, row in df.iterrows():
    dt = row["dt"]
    variation = row["variation"]
    users = row["users"]  # 该实验组的新增用户数

    if users > 0:
        for day in days:
            # 替换 'd1', 'd2', ..., 'd15' 为数字
            day_num = day_map[day]
            retained = row[day]
            retention_rate = retained / users if users > 0 else 0
            se = np.sqrt((retention_rate * (1 - retention_rate)) / users) if users > 0 else 0  # 标准误

            ci_lower = retention_rate - 1.96 * se
            ci_upper = retention_rate + 1.96 * se

            results.append({
                "dt": dt,
                "day": day_num,  # 使用数字
                "variation": variation,
                "users": users,
                "retained": retained,
                "retention_rate": round(retention_rate, 4),
                "ci_lower": round(ci_lower, 4),
                "ci_upper": round(ci_upper, 4)
            })

# 转换为 DataFrame
result_df = pd.DataFrame(results)

# 计算实验组 vs 对照组的增长率及置信区间
control_df = result_df[result_df["variation"] == "0"]
experiment_df = result_df[result_df["variation"] != "0"]

comparison_results = []
for day in range(1, 16):  # 使用数字 1 到 15
    for dt in result_df["dt"].unique():
        control_row = control_df[(control_df["day"] == str(day)) & (control_df["dt"] == dt)]
        if control_row.empty:
            continue

        control_rate = control_row["retention_rate"].values[0]
        control_se = np.sqrt((control_rate * (1 - control_rate)) / control_row["users"].values[0]) if \
        control_row["users"].values[0] > 0 else 0

        for variation in experiment_df["variation"].unique():
            exp_row = experiment_df[
                (experiment_df["day"] == str(day)) & (experiment_df["variation"] == variation) & (experiment_df["dt"] == dt)]
            if exp_row.empty:
                continue

            exp_rate = exp_row["retention_rate"].values[0]
            exp_se = np.sqrt((exp_rate * (1 - exp_rate)) / exp_row["users"].values[0]) if exp_row["users"].values[
                                                                                              0] > 0 else 0

            if control_rate == 0 or exp_rate == 0:
                uplift, uplift_se, z_score, p_value, uplift_lower, uplift_upper = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
            else:
                uplift = (exp_rate - control_rate) / control_rate
                uplift_se = np.sqrt((control_se / control_rate) ** 2 + (exp_se / exp_rate) ** 2) if control_rate > 0 and exp_rate > 0 else 0

                uplift_lower = uplift - 1.96 * uplift_se
                uplift_upper = uplift + 1.96 * uplift_se

                z_score = (exp_rate - control_rate) / np.sqrt(control_se ** 2 + exp_se ** 2) if (control_se ** 2 + exp_se ** 2) > 0 else np.nan
                p_value = 2 * (1 - stats.norm.cdf(abs(z_score))) if not np.isnan(z_score) else np.nan

            comparison_results.append({
                "dt": dt,
                "day": str(day),  # 使用数字
                "variation": variation,
                "control_rate": round(control_rate, 4),
                "exp_rate": round(exp_rate, 4),
                "uplift": round(uplift, 4) if not np.isnan(uplift) else np.nan,
                "uplift_ci_lower": round(uplift_lower, 4) if not np.isnan(uplift_lower) else np.nan,
                "uplift_ci_upper": round(uplift_upper, 4) if not np.isnan(uplift_upper) else np.nan,
                "z_score": round(z_score, 4) if not np.isnan(z_score) else np.nan,
                "p_value": round(p_value, 4) if not np.isnan(p_value) else np.nan
            })

# 转换为 DataFrame 并保存
comparison_df = pd.DataFrame(comparison_results)

# 合并两个 DataFrame，添加比较结果到原结果中
final_df = pd.merge(result_df, comparison_df, on=["dt", "day", "variation"], how="left")

# 创建一个新的 DataFrame，仅包含 variation 为 "0" 的行（即对照组）
control_df = final_df[final_df["variation"] == "0"]

# 创建一个新的列 'baseline_retention_rate'，将每个日期和天数（day）对应的对照组留存率添加到所有变体中
baseline_retention_rate = control_df[["dt", "day", "retention_rate"]].drop_duplicates()

# 合并 baseline_retention_rate 到原始结果中，按照 'dt' 和 'day' 进行合并
final_df = pd.merge(final_df, baseline_retention_rate, on=["dt", "day"], how="left", suffixes=("", "_baseline"))

# 处理 'N/A' 和类型转换
final_df = final_df.replace("N/A", np.nan)

# 转换 'N/A' 为 None，并进行类型转换
final_df = final_df.where(pd.notnull(final_df), None)

# 处理数据类型
numeric_cols = ["uplift", "uplift_ci_lower", "uplift_ci_upper", "p_value"]
for col in numeric_cols:
    final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

# 保存最终结果到 CSV（可选）
final_df.to_csv("final_retention_results2.csv", index=False)
print("✅ A/B 测试最终结果已保存！")

# 可视化（可选）
plt.figure(figsize=(12, 6))
sns.lineplot(data=final_df, x="day", y="retention_rate", hue="variation", marker="o")
plt.axhline(control_df["retention_rate"].mean(), linestyle="--", color="black", linewidth=2, label="Control Baseline")
plt.title("实验组 vs 对照组的留存率")
plt.xlabel("天数")
plt.ylabel("留存率")
plt.legend()
plt.show()

# 将 final_df 插入到数据库中的 tbl_new_retention_results (使用 append 模式)
try:
    final_df.to_sql(
        name='tbl_new_retention_results2',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',  # 批量插入提升性能
        chunksize=500  # 根据需要调整批次大小
    )
    print("✅ 数据已成功插入到数据库！")
except SQLAlchemyError as e:
    print(f"🚨 数据库插入失败: {e}")
