import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=FutureWarning)


def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("数据库连接已建立。")
    return engine


def insert_subscribe_metrics_by_variation(tag):
    print(f"开始获取实验订阅数据（按 variation 汇总），标签: {tag}")
    from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time'].strftime("%Y-%m-%d")
    end_time = experiment_data['phase_end_time'].strftime("%Y-%m-%d")
    print(f"实验名称: {experiment_name}, 时间: {start_time} - {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_subscribe_metrics_{tag}"

    create_table_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        experiment_user_count INT,
        new_subscribe_users INT,
        new_subscribe_events INT,
        new_subscribe_rate DOUBLE,
        subscribe_arpu DOUBLE,
        renewal_rate DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        for stmt in create_table_query.strip().split(';'):
            if stmt.strip():
                conn.execute(text(stmt))

        insert_query = f"""
        INSERT INTO {table_name}
        WITH 
        exp AS (
          SELECT DISTINCT user_id, variation_id
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
        ),
        experiment_users AS (
          SELECT variation_id, COUNT(DISTINCT user_id) AS experiment_user_count
          FROM exp
          GROUP BY variation_id
        ),
        platform_subscribe AS (
          SELECT g.user_id AS user_id, DATE(g.sub_date) AS dt, g.sub_date, 
                 g.expiration_date, 'android' AS platform, 
                 g.notification_type, e.variation_id
          FROM flow_wide_info.tbl_wide_business_subscribe_google_detail g
          JOIN exp e ON g.user_id = e.user_id
          WHERE g.notification_type IN (2, 4)
            AND g.sub_date BETWEEN '{start_time}' AND '{end_time} 23:59:59'

          UNION ALL

          SELECT a.user_id AS user_id, DATE(a.sub_date) AS dt, a.sub_date,
                 a.expiration_date, 'ios' AS platform, 
                 a.notification_type, e.variation_id
          FROM flow_wide_info.tbl_wide_business_subscribe_apple_detail a
          JOIN exp e ON a.user_id = e.user_id
          WHERE a.notification_type IN ('SUBSCRIBED', 'DID_RENEW', 'DID_CHANGE_RENEWAL_PREF')
            AND a.sub_date BETWEEN '{start_time}' AND '{end_time} 23:59:59'
        ),
        revenue_table AS (
          SELECT user_id, dt, SUM(revenue) AS revenue
          FROM (
              SELECT user_id, event_date AS dt, revenue,
                     ROW_NUMBER() OVER (PARTITION BY user_id, event_date ORDER BY revenue DESC) AS rn
              FROM flow_event_info.tbl_app_event_subscribe
              WHERE event_date BETWEEN '{start_time}' AND '{end_time}'
          ) t
          WHERE rn = 1
          GROUP BY user_id, dt
        ),
        platform_with_revenue AS (
          SELECT ps.variation_id, ps.user_id, ps.notification_type, ps.expiration_date, COALESCE(r.revenue, 0) AS revenue
          FROM platform_subscribe ps
          LEFT JOIN revenue_table r ON ps.user_id = r.user_id AND ps.dt = r.dt
        ),
        new_subscribe_events AS (
          SELECT variation_id, COUNT(*) AS new_subscribe_events
          FROM platform_with_revenue
          WHERE notification_type IN (4, 'SUBSCRIBED')
          GROUP BY variation_id
        ),
        new_subscribe_users AS (
          SELECT variation_id, COUNT(DISTINCT user_id) AS new_subscribe_users
          FROM platform_with_revenue
          WHERE notification_type IN (4, 'SUBSCRIBED')
          GROUP BY variation_id
        ),
        active_users AS (
          SELECT variation_id, COUNT(DISTINCT user_id) AS active_user_count
          FROM exp
          GROUP BY variation_id
        ),
        subscribe_revenue AS (
          SELECT variation_id, SUM(revenue) AS total_subscribe_revenue
          FROM platform_with_revenue
          GROUP BY variation_id
        ),
        renewal AS (
          SELECT variation_id, COUNT(DISTINCT user_id) AS renewal_count
          FROM platform_with_revenue
          WHERE notification_type IN (2, 'DID_RENEW')
          GROUP BY variation_id
        ),
        due_subscriptions AS (
          SELECT variation_id, COUNT(DISTINCT user_id) AS due_count
          FROM platform_with_revenue
          WHERE expiration_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY variation_id
        )
        SELECT
          a.variation_id,
          a.experiment_user_count,
          COALESCE(nu.new_subscribe_users, 0) AS new_subscribe_users,
          COALESCE(ne.new_subscribe_events, 0) AS new_subscribe_events,
          ROUND(COALESCE(ne.new_subscribe_events, 0) / NULLIF(a.experiment_user_count, 0), 4) AS new_subscribe_rate,
          ROUND(COALESCE(sr.total_subscribe_revenue, 0) / NULLIF(au.active_user_count, 0), 4) AS subscribe_arpu,
          CASE WHEN COALESCE(d.due_count, 0) = 0 THEN 0 
               ELSE ROUND(COALESCE(r.renewal_count, 0) / d.due_count, 4) END AS renewal_rate,
          '{tag}' AS experiment_tag
        FROM experiment_users a
        LEFT JOIN new_subscribe_users nu ON a.variation_id = nu.variation_id
        LEFT JOIN new_subscribe_events ne ON a.variation_id = ne.variation_id
        LEFT JOIN subscribe_revenue sr ON a.variation_id = sr.variation_id
        LEFT JOIN active_users au ON a.variation_id = au.variation_id
        LEFT JOIN renewal r ON a.variation_id = r.variation_id
        LEFT JOIN due_subscriptions d ON a.variation_id = d.variation_id;
        """

        conn.execute(text(insert_query))
        print(f"✅ 订阅指标（按 variation 汇总）已写入表：{table_name}")


def main(tag):
    insert_subscribe_metrics_by_variation(tag)


if __name__ == "__main__":
    main("trans_pt")
