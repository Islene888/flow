import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import sys

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

# ✅ 数据库连接
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

    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str   = end_time.strftime("%Y-%m-%d")

    engine = get_db_connection()
    table_name = f"tbl_report_chat_start_rate_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        clicked_bots INT,
        chat_bots INT,
        chat_start_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 准备就绪。")

        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):  # 排除首尾
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")

            # 先删除已有数据，避免重复插入
            delete_sql = f"DELETE FROM {table_name} WHERE event_date = '{current_date}';"
            conn.execute(text(delete_sql))

            insert_sql = f"""
            INSERT INTO {table_name} (
                event_date, variation, clicked_bots, chat_bots, chat_start_rate, experiment_name
            )
            WITH dedup_assignment AS (
                SELECT user_id, event_date, variation_id
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY user_id, event_date, experiment_id
                               ORDER BY variation_id
                           ) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                ) t
                WHERE rn = 1
            )
            SELECT
                '{current_date}' AS event_date,
                a.variation_id AS variation,
                COUNT(DISTINCT v.bot_id) AS clicked_bots,
                COUNT(DISTINCT cs.prompt_id) AS chat_bots,
                CASE 
                    WHEN COUNT(DISTINCT v.bot_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT cs.prompt_id) * 1.0 / COUNT(DISTINCT v.bot_id), 4)
                END AS chat_start_rate,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_bot_view v
            JOIN dedup_assignment a
                ON v.user_id = a.user_id AND v.event_date = a.event_date
            LEFT JOIN flow_event_info.tbl_app_event_chat_send cs
                ON v.user_id = cs.user_id AND v.bot_id = cs.prompt_id AND v.event_date = cs.event_date
            WHERE v.event_date = '{current_date}'
            GROUP BY a.variation_id;
            """

            print(f"👉 正在插入日期：{current_date}")
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{insert_sql}")

        print(f"✅ 所有开聊率数据已插入表 {table_name}。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    print("🚀 开聊率预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0416"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
