import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime

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


# ============= 插入 ARPPU 明细数据 =============
def insert_arppu_data(tag):
    """
    计算 ARPPU 并写入数仓：
      - 计算总收入、付费用户数
      - 计算 ARPPU（总收入 / 付费用户数）
      - 写入 `tbl_report_arppu_{tag}`
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
    table_name = f"tbl_report_arppu_{tag}"

    # **创建目标表（如果不存在）并清空数据**
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        paying_users INT,
        total_revenue DOUBLE,
        ARPPU DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建并清空数据。")

        # **计算 ARPPU 并写入数据**
        insert_query = f"""
        INSERT INTO {table_name} (variation_id, paying_users, total_revenue, ARPPU, experiment_tag)
        WITH 
        exp AS (
          SELECT user_id, variation_id
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        revenue AS (
          SELECT 
            e.variation_id,
            SUM(p.revenue) AS total_revenue
          FROM flow_event_info.tbl_app_event_all_purchase p
          JOIN exp e ON p.user_id = e.user_id
          WHERE p.type IN ('subscription', 'currency')
          GROUP BY e.variation_id
        ),
        paid AS (
          SELECT 
            e.variation_id,
            COUNT(DISTINCT p.user_id) AS paying_users
          FROM flow_event_info.tbl_app_event_all_purchase p
          JOIN exp e ON p.user_id = e.user_id
          WHERE p.type IN ('subscription', 'currency')
          GROUP BY e.variation_id
        )
        SELECT
          paid.variation_id,
          paid.paying_users,
          revenue.total_revenue,
          ROUND(revenue.total_revenue / paid.paying_users, 4) AS ARPPU,
          '{tag}' AS experiment_tag
        FROM paid
        LEFT JOIN revenue ON paid.variation_id = revenue.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"✅ ARPPU 明细数据已插入到表 {table_name}")
    return table_name


# ============= 汇总并覆盖目标表 =============
def overwrite_arppu_table_with_summary(tag):
    """
    计算汇总 ARPPU 并覆盖写入目标表。
    """
    print(f"🚀 开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_arppu_{tag}"
    engine = get_db_connection()

    summary_query = f"""
    SELECT 
        variation_id,
        SUM(paying_users) AS paying_users,
        SUM(total_revenue) AS total_revenue,
        ROUND(SUM(total_revenue)/SUM(paying_users), 4) AS ARPPU,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation_id != 'null'
    GROUP BY variation_id;
    """
    summary_df = pd.read_sql(text(summary_query), engine)

    # **重新创建目标表（如果不存在）并清空数据**
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        paying_users INT,
        total_revenue DOUBLE,
        ARPPU DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        print(f"✅ 目标表 {table_name} 已重新清空。")

        # **将汇总结果逐行写入目标表**
        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation_id, paying_users, total_revenue, ARPPU, experiment_tag)
            VALUES ('{row['variation_id']}', {row['paying_users']}, {row['total_revenue']}, {row['ARPPU']}, '{row['experiment_tag']}');
            """
            conn.execute(text(insert_query))
    print(f"✅ 汇总数据已覆盖到表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_arppu_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    overwrite_arppu_table_with_summary(tag)
    print("🚀 主流程执行完毕。")


if __name__ == "__main__":
    main("backend")
