import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime

from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine

def insert_arpu_data(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_date = experiment_data['phase_start_time'].date()
    end_date = experiment_data['phase_end_time'].date()
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_date} 至 {end_date}")

    engine = get_db_connection()
    table_name = f"tbl_report_arpu_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        active_users INT,
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

        insert_query = f"""
        INSERT INTO {table_name} (event_date, variation_id, active_users, total_revenue, ARPU, experiment_tag)
        WITH 
        exp AS (
          SELECT user_id, variation_id, event_date
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        daily_active AS (
          SELECT e.event_date, e.variation_id, COUNT(DISTINCT pv.user_id) AS active_users
          FROM flow_event_info.tbl_app_event_page_view pv
          JOIN exp e ON pv.user_id = e.user_id AND pv.event_date = e.event_date
          GROUP BY e.event_date, e.variation_id
        ),
        sub AS (
          SELECT user_id, event_date, SUM(revenue) AS sub_revenue
          FROM flow_event_info.tbl_app_event_subscribe
          WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
          GROUP BY user_id, event_date
        ),
        ord AS (
          SELECT user_id, event_date, SUM(revenue) AS order_revenue
          FROM flow_event_info.tbl_app_event_currency_purchase
          WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
          GROUP BY user_id, event_date
        ),
        combined AS (
          SELECT COALESCE(s.user_id, o.user_id) AS user_id,
                 COALESCE(s.event_date, o.event_date) AS event_date,
                 COALESCE(s.sub_revenue, 0) AS sub_revenue,
                 COALESCE(o.order_revenue, 0) AS order_revenue,
                 COALESCE(s.sub_revenue, 0) + COALESCE(o.order_revenue, 0) AS total_revenue
          FROM sub s
          FULL OUTER JOIN ord o ON s.user_id = o.user_id AND s.event_date = o.event_date
        ),
        revenue_with_variation AS (
          SELECT e.event_date, e.variation_id, c.total_revenue
          FROM combined c
          JOIN exp e ON c.user_id = e.user_id AND c.event_date = e.event_date
        ),
        daily_revenue AS (
          SELECT event_date, variation_id, SUM(total_revenue) AS revenue
          FROM revenue_with_variation
          GROUP BY event_date, variation_id
        )
        SELECT 
          da.event_date,
          da.variation_id,
          da.active_users,
          COALESCE(dr.revenue, 0) AS total_revenue,
          ROUND(COALESCE(dr.revenue, 0)/NULLIF(da.active_users, 0), 4) AS ARPU,
          '{tag}' AS experiment_tag
        FROM daily_active da
        LEFT JOIN daily_revenue dr 
          ON da.event_date = dr.event_date AND da.variation_id = dr.variation_id
        WHERE da.event_date > '{start_date}' AND da.event_date < '{end_date}';
        """
        conn.execute(text(insert_query))
        print(f"✅ ARPU 明细数据已插入到表 {table_name}")
    return table_name

def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_arpu_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    print("🚀 主流程执行完毕。")

if __name__ == "__main__":
    main("trans_es")
