import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import logging

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL, pool_recycle=3600)
    logging.info("✅ 数据库连接已建立。")
    return engine


# ============= 按天 & 分片插入数据 =============
def insert_new_conversation_data(tag):
    logging.info(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']

    logging.info(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_new_conversation_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_new_conversation INT,
        unique_new_conversation_users INT,
        new_conversation_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        logging.info(f"✅ 目标表 {table_name} 已创建，并清空数据。")

        current_date = start_time
        while current_date <= end_time:
            date_str = current_date.strftime('%Y-%m-%d')
            logging.info(f"📅 处理日期：{date_str}")

            for batch_index in range(10):  # 10 分片
                batch_insert_query = text(f"""
                INSERT INTO {table_name} (variation, total_new_conversation, unique_new_conversation_users, new_conversation_ratio, experiment_name)
                SELECT 
                    a.variation_id AS variation,
                    COUNT(DISTINCT c.conversation_id) AS total_new_conversation,
                    COUNT(DISTINCT c.user_id) AS unique_new_conversation_users,
                    CASE 
                        WHEN COUNT(DISTINCT c.user_id) = 0 THEN 0 
                        ELSE ROUND(COUNT(DISTINCT c.conversation_id) / COUNT(DISTINCT c.user_id), 4) 
                    END AS new_conversation_ratio,
                    :experiment_name AS experiment_name
                FROM flow_event_info.tbl_app_event_chat_send c
                JOIN flow_wide_info.tbl_wide_experiment_assignment_hi a
                    ON c.user_id = a.user_id
                WHERE a.experiment_id = :experiment_name
                  AND c.ingest_timestamp >= :start_time
                  AND c.ingest_timestamp < :end_time
                  AND c.conversation_length = 1
                  AND MOD(crc32(c.user_id), 10) = :batch_index
                GROUP BY a.variation_id;
                """)

                conn.execute(batch_insert_query, {
                    "experiment_name": experiment_name,
                    "start_time": f"{date_str} 00:00:00",
                    "end_time": f"{date_str} 23:59:59",
                    "batch_index": batch_index
                })
                logging.info(f"✅ 日期 {date_str}，批次 {batch_index}/10 插入完成。")

            current_date += timedelta(days=1)

    logging.info(f"✅ 所有数据插入完成，目标表：{table_name}")
    return table_name


# ============= 计算汇总并覆盖原表 =============
def overwrite_new_conversation_table_with_summary(tag):
    logging.info(f"📊 开始生成汇总数据，并覆盖到原表，标签：{tag}")

    table_name = f"tbl_report_new_conversation_{tag}"
    engine = get_db_connection()

    summary_query = text(f"""
    SELECT 
        variation,
        SUM(total_new_conversation) AS total_new_conversation,
        SUM(unique_new_conversation_users) AS unique_new_conversation_users,
        CASE 
            WHEN SUM(unique_new_conversation_users) = 0 THEN 0
            ELSE ROUND(SUM(total_new_conversation) / SUM(unique_new_conversation_users), 4)
        END AS new_conversation_ratio,
        MAX(experiment_name) AS experiment_name
    FROM {table_name}
    GROUP BY variation;
    """)

    summary_df = pd.read_sql(summary_query, engine)
    summary_df.to_sql(table_name, engine, if_exists="replace", index=False)

    logging.info(f"✅ 汇总数据已覆盖表：{table_name}")

if __name__ == "__main__":
    main("backend")