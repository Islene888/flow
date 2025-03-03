import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
from sqlalchemy.exc import SQLAlchemyError
import re
from datetime import datetime

# ✅ 解决 Matplotlib 中文乱码
matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False

# 对密码进行 URL 编码
password = urllib.parse.quote_plus("flowgpt@2024.com")

# 构造数据库连接 URL
DATABASE_URL = f"mysql+pymysql://bigdata:{password}@18.188.196.105:9030/flow_test"

# 创建数据库连接
engine = create_engine(DATABASE_URL)


# 函数：提取实验的日期范围并计算天数
def get_experiment_params():
    query = """
    SELECT experiment_name, date_created, date_updated, control_group_key, variations
    FROM `tbl_experiment_data`
    WHERE experiment_name IS NOT NULL
    """
    experiment_data = pd.read_sql(query, engine)

    if experiment_data.empty:
        raise ValueError("No experiments found in `tbl_experiment_data`")

    experiment_params = []
    for _, row in experiment_data.iterrows():
        experiment_name = row['experiment_name']
        date_created = row['date_created']
        date_updated = row['date_updated']
        control_group_key = row['control_group_key']
        variations = row['variations']

        # 打印每个实验的基本信息，方便调试
        print(f"Processing experiment: {experiment_name}")
        print(f"Date Created: {date_created}, Date Updated: {date_updated}")

        # 处理毫秒部分
        start_date = datetime.strptime(str(date_created).split('.')[0], '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(str(date_updated).split('.')[0], '%Y-%m-%d %H:%M:%S')

        experiment_days = (end_date - start_date).days + 1

        experiment_params.append({
            "experiment_name": experiment_name,
            "start_date": start_date,
            "end_date": end_date,
            "experiment_days": experiment_days,
            "control_group_key": control_group_key,
            "variations": variations
        })

    return experiment_params


# 函数：根据实验参数动态创建表并插入数据
def create_and_insert_table(experiment_name, start_date, end_date, experiment_days):
    # 动态生成天数列
    days_columns = [f"d{i}" for i in range(1, experiment_days + 1)]

    # 动态生成表名
    table_name_filtered = f"tbl_user_engagement_filtered_{experiment_name}"
    print(table_name_filtered)

    table_name_results = f"tbl_new_retention_results_{experiment_name}"
    print(table_name_results)

    # 打印生成的表名
    print(f"Creating/Verifying tables: {table_name_filtered}, {table_name_results}")

    # SQL 查询：创建新表
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name_filtered}` (
        dt DATE,
        variations VARCHAR(255),
        users INT,
        {', '.join([f'{day} INT' for day in days_columns])}
    );

    CREATE TABLE IF NOT EXISTS `{table_name_results}` (
        dt DATE,
        day INT,
        variations VARCHAR(255),
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

    # 执行创建表操作
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
        print(f"✅ Tables created or verified for {experiment_name}.")
    except SQLAlchemyError as e:
        print(f"🚨 SQL Error: {e}")
        return

    insert_query = f"""
        INSERT INTO `{table_name_filtered}` (dt, variation, users, {', '.join(days_columns)})
        SELECT 
            u.first_visit_date AS dt, 
            e.variation,  -- 注意这里使用的是 variation
            COUNT(DISTINCT u.user_id) AS users,
            {', '.join([f'COUNT(DISTINCT CASE WHEN a.active_date = DATE_ADD(u.first_visit_date, INTERVAL {i} DAY) THEN a.user_id END) AS d{i}' for i in range(1, experiment_days + 1)])}
        FROM
            (SELECT
                user_id,
                DATE(first_visit_date) AS first_visit_date
            FROM
                flow_wide_info.tbl_wide_user_first_visit_app_info
            WHERE
                first_visit_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}') u
        LEFT JOIN
            (SELECT
                u.user_id,
                u.first_visit_date,
                DATE(FROM_UNIXTIME(a.ingest_timestamp / 1000, '%%Y-%%m-%%d')) AS active_date
            FROM
                flow_wide_info.tbl_wide_user_first_visit_app_info u
            JOIN
                flow_wide_info.tbl_wide_backend_detail_hi a ON u.user_id = a.user_id
            WHERE
                a.event_name = 'Chat_LLM'
                AND a.device_type = 'MOBILE'
                AND DATE(FROM_UNIXTIME(a.ingest_timestamp / 1000, '%%Y-%%m-%%d')) BETWEEN u.first_visit_date AND '{end_date.strftime('%Y-%m-%d')}') a
        ON u.user_id = a.user_id
        LEFT JOIN
            (SELECT
                user_id,
                CAST(variation_id AS CHAR) AS variation  -- 注意这里用的是 variation
            FROM
                flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE
                experiment_name = '{experiment_name}'
                AND timestamp_assigned BETWEEN '{start_date.strftime('%Y-%m-%d')} 12:00:00' AND '{end_date.strftime('%Y-%m-%d')} 12:00:00') e
        ON u.user_id = e.user_id
        GROUP BY
            u.first_visit_date, e.variation
        ORDER BY 
            u.first_visit_date;
    """

    # 执行插入操作
    try:
        with engine.connect() as conn:
            conn.execute(text(insert_query))
        print(f"✅ Data inserted for {experiment_name}.")
    except SQLAlchemyError as e:
        print(f"🚨 SQL Error during data insertion: {e}")


# 获取实验参数
experiment_params = get_experiment_params()

# 遍历每个实验，运行计算
for params in experiment_params:
    experiment_name = params["experiment_name"]
    start_date = params["start_date"]
    end_date = params["end_date"]
    experiment_days = params["experiment_days"]

    # 打印实验参数
    print(f"Running for experiment: {experiment_name}")
    print(f"Start Date: {start_date}, End Date: {end_date}, Days: {experiment_days}")

    # 创建表并插入数据
    create_and_insert_table(experiment_name, start_date, end_date, experiment_days)

    # 使用正确的表名
    table_name_filtered = f"tbl_user_engagement_filtered_{experiment_name}"

    # 动态生成查询语句
    query = f"""
    SELECT * FROM `{table_name_filtered}`
    ORDER BY dt ASC, CAST(variation AS UNSIGNED) ASC;
    """

    print(f"Running query: {query}")

    df = pd.read_sql(query, engine)

    # 打印数据预览
    print(f"Data retrieved: {df.head()}")

    # 将 NaN 替换为 SQL 中的 NULL
    df = df.where(pd.notnull(df), None)

    if 'dt' not in df.columns:
        df['dt'] = pd.to_datetime(df['first_visit_date'])

    df["variations"] = df["variations"].astype(str)

    # 动态生成留存率计算过程
    days_columns = [f"d{i}" for i in range(1, experiment_days + 1)]
    day_map = {f"d{i}": str(i) for i in range(1, experiment_days + 1)}

    results = []

    for _, row in df.iterrows():
        dt = row["dt"]
        variations = row["variations"]
        users = row["users"]

        if users > 0:
            for day in days_columns:
                day_num = day_map[day]
                retained = row[day]

                # 计算留存率
                retention_rate = np.float64(retained) / np.float64(users) if users > 0 else 0

                # 计算标准误差（标准误）
                se = np.sqrt((retention_rate * (1 - retention_rate)) / np.float64(users)) if users > 0 else 0

                # 计算95%置信区间
                ci_lower = retention_rate - 1.96 * se
                ci_upper = retention_rate + 1.96 * se

                # 将结果添加到结果列表
                results.append({
                    "dt": dt,
                    "day": int(day_num),
                    "variations": variations,
                    "users": users,
                    "retained": retained,
                    "retention_rate": retention_rate,
                    "ci_lower": ci_lower,
                    "ci_upper": ci_upper
                })

        # 将结果转换为 DataFrame
        results_df = pd.DataFrame(results)

        # 计算更多的统计数据
        for variations in results_df["variations"].unique():
            variation_data = results_df[results_df["variations"] == variations]

            # 计算对照组的基准留存率（假设第一个 variation 作为对照组）
            if variations == results_df["variations"].unique()[0]:
                baseline_retention_rate = variation_data["retention_rate"].mean()
            else:
                baseline_retention_rate = results_df[results_df["variations"] == results_df["variations"].unique()[0]][
                    "retention_rate"].mean()

            # 计算提升（Uplift）
            results_df["uplift"] = results_df["retention_rate"] - baseline_retention_rate
            results_df["uplift_ci_lower"] = results_df["ci_lower"] - baseline_retention_rate
            results_df["uplift_ci_upper"] = results_df["ci_upper"] - baseline_retention_rate

            # 计算 Z 分数和 P 值
            results_df["z_score"] = (results_df["retention_rate"] - baseline_retention_rate) / np.sqrt(
                (baseline_retention_rate * (1 - baseline_retention_rate)) / results_df["users"])
            results_df["p_value"] = stats.norm.sf(abs(results_df["z_score"])) * 2  # 双尾检验

        # 将结果写入结果表
        insert_results_query = f"""
            INSERT INTO `{experiment_name}_new_retention_results` 
            (dt, day, variations, users, retained, retention_rate, ci_lower, ci_upper, control_rate, exp_rate, uplift, uplift_ci_lower, uplift_ci_upper, z_score, p_value, retention_rate_baseline)
            VALUES 
        """

        # 将 DataFrame 中的 NaN 替换为 SQL 中的 NULL（即 None）
        df = df.replace({np.nan: None})

        # 对计算的结果，也进行同样的处理
        results_df = results_df.replace({np.nan: None})

        # 在插入数据库之前，确保没有 NaN 值
        insert_values = []
        for _, row in results_df.iterrows():
            insert_values.append(
                f"('{row['dt']}', {row['day']}, '{row['variations']}', {row['users']}, {row['retained']}, {row['retention_rate']}, {row['ci_lower']}, {row['ci_upper']}, {row['retention_rate_baseline']}, {row['retention_rate']}, {row['uplift']}, {row['uplift_ci_lower']}, {row['uplift_ci_upper']}, {row['z_score']}, {row['p_value']}, {row['retention_rate_baseline']})"
            )

        # 处理插入数据时的 None 为 NULL
        insert_values = [value.replace('None', 'NULL') for value in insert_values]

        # 构建完整的插入查询
        insert_values_query = insert_results_query + ", ".join(insert_values)

        # 执行插入操作
        try:
            with engine.connect() as conn:
                conn.execute(text(insert_values_query))
            print(f"✅ Results inserted for {experiment_name}.")
        except SQLAlchemyError as e:
            print(f"🚨 SQL Error during result insertion: {e}")
