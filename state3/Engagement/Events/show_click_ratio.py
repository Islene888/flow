import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
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

    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str   = end_time.strftime("%Y-%m-%d")

    engine = get_db_connection()
    table_name = f"tbl_report_click_rate_{tag}"

    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        exposed_bots INT,
        clicked_bots INT,
        click_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        # 遍历每一天（不含首尾）
        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")

            insert_sql = f"""
            INSERT INTO {table_name} (event_date, variation, exposed_bots, clicked_bots, click_rate, experiment_name)
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
                COUNT(DISTINCT s.prompt_id) AS exposed_bots,
                COUNT(DISTINCT v.bot_id) AS clicked_bots,
                CASE
                    WHEN COUNT(DISTINCT s.prompt_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT v.bot_id) * 1.0 / COUNT(DISTINCT s.prompt_id), 4)
                END AS click_rate,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_show_prompt_card s
            JOIN dedup_assignment a
                ON s.user_id = a.user_id AND s.event_date = a.event_date
            LEFT JOIN flow_event_info.tbl_app_event_bot_view v
                ON s.user_id = v.user_id
                AND s.prompt_id = v.bot_id
                AND s.event_date = v.event_date
            WHERE s.event_date = '{current_date}'
            GROUP BY a.variation_id;
            """
            print(f"👉 正在插入日期：{current_date}")
            try:
                conn.execute(text(insert_sql))
            except Exception as e:
                print(f"❌ 插入 {current_date} 失败：{e}")
                print(f"🔍 SQL:\n{insert_sql}")

        print(f"✅ 所有分日点击率数据已插入表 {table_name}。")

    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    print("🚀 点击率结果预览：")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0416"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
