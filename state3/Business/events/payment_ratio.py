import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine

# ============= 插入每日付费率数据（实验周期内） =============
def insert_ltv_data(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_payment_ratio_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        event_date DATE,
        paying_users INT,
        revenue DOUBLE,
        LTV7 DOUBLE,
        LTV_experiment DOUBLE,
        total_users INT,
        purchase_rate DOUBLE,
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
        INSERT INTO {table_name} (variation_id, event_date, paying_users, revenue, LTV7, LTV_experiment, total_users, purchase_rate, experiment_tag)
        WITH 
        exp AS (
          SELECT 
            user_id, 
            variation_id,
            MIN(event_date) AS first_active_date
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY user_id, variation_id
        ),
        act_raw AS (
          SELECT DISTINCT
            e.user_id,
            e.variation_id,
            p.event_date
          FROM exp e
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.event_date BETWEEN '{start_time}' AND '{end_time}'
        ),
        purchase AS (
          SELECT
            e.user_id,
            e.variation_id,
            p.event_date,
            p.revenue,
            DATEDIFF(p.event_date, e.first_active_date) AS day_diff
          FROM exp e
          JOIN flow_event_info.tbl_app_event_all_purchase p
            ON e.user_id = p.user_id
           AND p.type IN ('subscription', 'currency')
           AND p.event_date BETWEEN e.first_active_date AND '{end_time}'
        ),
        filtered_days AS (
          SELECT DISTINCT event_date FROM purchase ORDER BY event_date
          LIMIT 100000 OFFSET 1
        ),
        final_purchase AS (
          SELECT DISTINCT user_id, variation_id, event_date, revenue, day_diff
          FROM purchase
          WHERE event_date IN (SELECT event_date FROM filtered_days)
        ),
        final_activity AS (
          SELECT DISTINCT user_id, variation_id, event_date
          FROM act_raw
          WHERE event_date IN (SELECT event_date FROM filtered_days)
        )
        SELECT 
          a.variation_id,
          a.event_date,
          COUNT(DISTINCT p.user_id) AS paying_users,
          SUM(IFNULL(p.revenue, 0)) AS revenue,
          ROUND(SUM(CASE WHEN p.day_diff <= 7 THEN p.revenue ELSE 0 END) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV7,
          ROUND(SUM(IFNULL(p.revenue, 0)) / NULLIF(COUNT(DISTINCT p.user_id), 0), 4) AS LTV_experiment,
          COUNT(DISTINCT a.user_id) AS total_users,
          ROUND(COUNT(DISTINCT p.user_id) / NULLIF(COUNT(DISTINCT a.user_id), 0), 4) AS purchase_rate,
          '{tag}' AS experiment_tag
        FROM final_activity a
        LEFT JOIN final_purchase p
          ON a.user_id = p.user_id AND a.variation_id = p.variation_id AND a.event_date = p.event_date
        GROUP BY a.variation_id, a.event_date;
        """
        conn.execute(text(insert_query))
        print(f"✅ 每日数据（含付费率、LTV7、实验期LTV）已插入到表 {table_name}")
    return table_name

# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_ltv_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    print("🚀 主流程执行完毕。")

if __name__ == "__main__":
    main("trans_es")
