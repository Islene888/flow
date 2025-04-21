import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime
import sys

from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine

def main(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time   = experiment_data['phase_end_time']

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str   = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_day = start_time.strftime("%Y-%m-%d")
    end_day   = end_time.strftime("%Y-%m-%d")

    print(f"📝 实验名称：{experiment_name}")
    print(f"⏰ 实验时间范围：{start_time_str} ~ {end_time_str}")

    engine = get_db_connection()
    table_name = f"tbl_report_follow_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        total_follow INT,
        unique_follow_users INT,
        follow_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    truncate_query = f"TRUNCATE TABLE {table_name};"

    # 判断是否过滤首尾日
    filter_days = (end_time - start_time).days > 2
    date_filter_clause = (
        f"WHERE raw.event_date > '{start_day}' AND raw.event_date < '{end_day}'"
        if filter_days else ""
    )
    if not filter_days:
        print("⚠️ 实验时间不足三天，未过滤首尾日。")

    insert_query = f"""
    INSERT INTO {table_name} (event_date, variation, total_follow, unique_follow_users, follow_ratio, experiment_name)
    SELECT 
        raw.event_date,
        raw.variation,
        raw.total_follow,
        raw.unique_follow_users,
        raw.follow_ratio,
        '{experiment_name}' AS experiment_name
    FROM (
        SELECT
            DATE(f.ingest_timestamp) AS event_date,
            a.variation_id AS variation,
            COUNT(DISTINCT f.event_id) AS total_follow,
            COUNT(DISTINCT f.user_id) AS unique_follow_users,
            CASE 
                WHEN COUNT(DISTINCT f.user_id) = 0 THEN 0 
                ELSE ROUND(COUNT(DISTINCT f.event_id) * 1.0 / COUNT(DISTINCT f.user_id), 4)
            END AS follow_ratio
        FROM flow_event_info.tbl_app_event_bot_follow f
        JOIN (
            SELECT user_id, variation_id
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = '{experiment_name}'
            GROUP BY user_id, variation_id
        ) a ON f.user_id = a.user_id
        WHERE f.ingest_timestamp BETWEEN '{start_time_str}' AND '{end_time_str}'
          AND f.user_id NOT LIKE 'test%'  -- 示例：排除测试账号
        GROUP BY DATE(f.ingest_timestamp), a.variation_id
    ) AS raw
    {date_filter_clause};
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 表 {table_name} 已创建并清空。")
        conn.execute(text(insert_query))
        print(f"✅ 已写入跟随行为统计数据至表 {table_name}。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    print("🚀 最终结果预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "trans_es"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
