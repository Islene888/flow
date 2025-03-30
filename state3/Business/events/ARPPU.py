import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("\u2705 数据库连接已建立。")
    return engine


def insert_arppu_daily_data(tag):
    print(f"\U0001f680 开始获取每日 ARPPU 数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time'].date()
    end_time = experiment_data['phase_end_time'].date()

    # 删除第一天和最后一天
    if (end_time - start_time).days < 2:
        print("⚠️ 实验周期过短，无法剔除首尾两天。")
        return None

    start_time += timedelta(days=1)
    end_time -= timedelta(days=1)

    print(f"📝 实验名称：{experiment_name}，有效实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_arppu_daily_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date DATE,
        variation_id VARCHAR(255),
        total_subscribe_revenue DOUBLE,
        total_order_revenue DOUBLE,
        total_revenue DOUBLE,
        paying_users INT,
        active_users INT,
        arppu DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 表 {table_name} 已创建并清空数据。")

        current_date = start_time
        while current_date <= end_time:
            print(f"📅 处理日期：{current_date}")

            daily_insert_query = f"""
            INSERT INTO {table_name} (event_date, variation_id, total_subscribe_revenue, total_order_revenue, total_revenue, paying_users, active_users, arppu, experiment_tag)
            WITH 
            exp AS (
                SELECT DISTINCT user_id, variation_id
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
            ),
            active AS (
                SELECT e.variation_id, COUNT(DISTINCT c.user_id) AS active_users
                FROM (
                    SELECT COALESCE(s.user_id, o.user_id) AS user_id,
                           COALESCE(s.event_date, o.event_date) AS event_date
                    FROM flow_event_info.tbl_app_event_subscribe s
                    FULL OUTER JOIN flow_event_info.tbl_app_event_currency_purchase o
                    ON s.user_id = o.user_id AND s.event_date = o.event_date
                    WHERE COALESCE(s.event_date, o.event_date) = '{current_date}'
                ) c
                JOIN exp e ON c.user_id = e.user_id
                GROUP BY e.variation_id
            ),
            sub AS (
                SELECT user_id, event_date, SUM(revenue) AS sub_revenue
                FROM flow_event_info.tbl_app_event_subscribe
                WHERE event_date = '{current_date}'
                GROUP BY user_id, event_date
            ),
            ord AS (
                SELECT user_id, event_date, SUM(revenue) AS order_revenue
                FROM flow_event_info.tbl_app_event_currency_purchase
                WHERE event_date = '{current_date}'
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
            merged AS (
                SELECT e.variation_id, c.event_date, c.user_id,
                       SUM(c.sub_revenue) AS sub_revenue,
                       SUM(c.order_revenue) AS order_revenue,
                       SUM(c.total_revenue) AS total_revenue
                FROM combined c
                JOIN exp e ON c.user_id = e.user_id
                GROUP BY e.variation_id, c.event_date, c.user_id
            )
            SELECT 
                m.event_date,
                m.variation_id,
                SUM(m.sub_revenue) AS total_subscribe_revenue,
                SUM(m.order_revenue) AS total_order_revenue,
                SUM(m.total_revenue) AS total_revenue,
                COUNT(*) AS paying_users,
                a.active_users,
                ROUND(SUM(m.total_revenue) / NULLIF(COUNT(*), 0), 4) AS arppu,
                '{tag}' AS experiment_tag
            FROM merged m
            LEFT JOIN active a ON m.variation_id = a.variation_id
            GROUP BY m.variation_id, m.event_date, a.active_users
            ORDER BY m.event_date ASC, m.variation_id ASC;
            """

            try:
                conn.execute(text(daily_insert_query))
                print(f"✅ 日期 {current_date} 数据插入成功。")
            except Exception as e:
                print(f"❌ 日期 {current_date} 插入失败：{e}")

            current_date += timedelta(days=1)

    print(f"✅ 所有每日 ARPPU 数据插入完成，表：{table_name}")
    return table_name


# 在 ARPPU.py 文件里加上这个函数封装

def main(tag):
    insert_arppu_daily_data(tag)


# 只在单独运行 ARPPU.py 时才执行
if __name__ == "__main__":
    main("trans_es")
