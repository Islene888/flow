import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime

# 使用与 LTV 代码一致的模块
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


# ============= 数据库连接 =============
def get_db_connection():
    """
    建立并返回数据库连接引擎。
    """
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine


# ============= 插入 ARPU 明细数据 =============
def insert_arpu_data(tag):
    """
    插入 ARPU 明细数据：
      - 获取实验信息，统计实验期间每天每个 variation 的活跃用户和收入，
        计算 ARPU 指标（总收入/活跃用户数）；
      - 在写入前创建目标表（如果不存在）并清空数据。

    修改后：
      - 利用 WITH 子查询一次性统计实验期间每天数据，再聚合计算各 variation 的总活跃用户、总收入及 ARPU，
        避免了原先的日期循环和批次查询，使逻辑更清晰。
    """
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    # 获取实验参数
    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_date} 至 {end_date}")

    engine = get_db_connection()
    table_name = f"tbl_report_arpu_{tag}"

    # 创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        total_active_users INT,
        total_revenue DOUBLE,
        ARPU DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建并清空数据。")

        # 利用 WITH 子查询一次性统计实验期间每天数据，并按 variation 聚合计算 ARPU
        insert_query = f"""
        INSERT INTO {table_name} (variation_id, total_active_users, total_revenue, ARPU, experiment_tag)
        WITH 
        exp AS (
          SELECT user_id, variation_id, event_date
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        daily_active AS (
          SELECT event_date, variation_id, COUNT(DISTINCT user_id) AS active_users
          FROM exp
          GROUP BY event_date, variation_id
        ),
        daily_revenue AS (
          SELECT e.event_date, e.variation_id, SUM(p.revenue) AS revenue
          FROM flow_event_info.tbl_app_event_all_purchase p
          JOIN exp e ON p.user_id = e.user_id AND p.event_date = e.event_date
          WHERE p.type IN ('subscription', 'currency')
          GROUP BY e.event_date, e.variation_id
        )
        SELECT 
          da.variation_id,
          SUM(da.active_users) AS total_active_users,
          SUM(dr.revenue) AS total_revenue,
          ROUND(SUM(dr.revenue)/SUM(da.active_users), 4) AS ARPU,
          '{tag}' AS experiment_tag
        FROM daily_active da
        LEFT JOIN daily_revenue dr 
          ON da.event_date = dr.event_date AND da.variation_id = dr.variation_id
        GROUP BY da.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"✅ ARPU 明细数据已插入到表 {table_name}")
    return table_name


# ============= 汇总并覆盖目标表 =============
def overwrite_arpu_table_with_summary(tag):
    """
    将插入的 ARPU 明细数据进行汇总（按 variation_id 聚合），
    重新计算 ARPU 指标，然后覆盖写入目标表，同时添加 experiment_tag 字段。
    """
    print(f"🚀 开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_arpu_{tag}"
    engine = get_db_connection()

    summary_query = f"""
    SELECT 
        variation_id,
        SUM(total_active_users) AS total_active_users,
        SUM(total_revenue) AS total_revenue,
        ROUND(SUM(total_revenue)/SUM(total_active_users), 4) AS ARPU,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation_id != 'null'
    GROUP BY variation_id;
    """
    summary_df = pd.read_sql(text(summary_query), engine)

    # 重新创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        total_active_users INT,
        total_revenue DOUBLE,
        ARPU DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        print(f"✅ 目标表 {table_name} 已重新清空。")

        # 将汇总结果逐行写入目标表
        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation_id, total_active_users, total_revenue, ARPU, experiment_tag)
            VALUES ('{row['variation_id']}', {row['total_active_users']}, {row['total_revenue']}, {row['ARPU']}, '{row['experiment_tag']}');
            """
            conn.execute(text(insert_query))
    print(f"✅ 汇总数据已覆盖到表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_arpu_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    overwrite_arpu_table_with_summary(tag)
    print("🚀 主流程执行完毕。")


if __name__ == "__main__":
    main("backend")
