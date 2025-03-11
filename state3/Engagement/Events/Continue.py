import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine


# ============= 插入 Continue 事件数据 =============
def insert_continue_data(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")

    # 获取实验详情
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']

    print(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_continue_{tag}"

    # **创建目标表**
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_continue INT,
        unique_continue_users INT,
        continue_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建，并已清空历史数据。")

        # **按天循环**
        current_date = start_time
        while current_date <= end_time:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"📅 处理日期：{date_str}")

            # **每天分 10 批次插入**
            for batch_index in range(10):
                print(f"📌 执行日期 {date_str}，批次 {batch_index + 1}/10 插入...")

                batch_insert_query = f"""
                INSERT INTO {table_name} (variation, total_continue, unique_continue_users, continue_ratio, experiment_name)
                SELECT /*+ SET_VAR(query_timeout = 30000) */
                    a.variation_id AS variation,
                    COUNT(DISTINCT c.event_id) AS total_continue,
                    COUNT(DISTINCT c.user_id) AS unique_continue_users,
                    ROUND(COUNT(DISTINCT c.event_id) * 1.0 / COUNT(DISTINCT c.user_id), 4) AS continue_ratio,
                    '{experiment_name}' as experiment_name
                FROM flow_event_info.tbl_app_event_chat_send c
                JOIN flow_wide_info.tbl_wide_experiment_assignment_hi a
                    ON c.user_id = a.user_id
                WHERE a.experiment_id = '{experiment_name}'
                  AND c.ingest_timestamp >= '{date_str} 00:00:00'
                  AND c.ingest_timestamp < '{date_str} 23:59:59'
                  AND c.method = 'continue'
                  AND MOD(crc32(c.user_id), 10) = {batch_index}
                GROUP BY a.variation_id;
                """
                try:
                    conn.execute(text(batch_insert_query))
                    print(f"✅ 日期 {date_str}，批次 {batch_index + 1}/10 插入成功。")
                except Exception as e:
                    print(f"❌ 日期 {date_str}，批次 {batch_index + 1}/10 插入失败，错误：{e}")

            # **日期加 1 天**
            current_date += timedelta(days=1)

    print(f"✅ 所有数据插入完成，目标表：{table_name}")
    return table_name


# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行。")

    # 先插入数据
    table_name = insert_continue_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return

    print("✅ 主流程执行完毕。")


# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
