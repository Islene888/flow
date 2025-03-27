import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import logging
from datetime import datetime

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 日志配置 =============
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("数据库连接已建立。")
    return engine

# ============= 插入 time_spent 数据 =============
def insert_time_spent_data(tag):
    logging.info(f"开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data["experiment_name"]
    start_time = experiment_data["phase_start_time"]
    end_time = experiment_data["phase_end_time"]
    logging.info(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    # 时间范围转字符串
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str = end_time.strftime("%Y-%m-%d")

    engine = get_db_connection()
    table_name = f"tbl_report_time_spent_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        avg_time_spent_minutes DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    # === 两级聚合 SQL ===
    # 1) session_agg: (user_id, event_date, total_minutes) 按天聚合
    # 2) experiment_var: 去重(用户 + variation_id)，防止同一user多次分配导致重复
    # 3) 最终关联后, group by event_date, variation_id => avg_time_spent_minutes
    insert_query = f"""
    INSERT INTO {table_name} (event_date, variation, avg_time_spent_minutes, experiment_name)
    WITH session_agg AS (
      SELECT
        s.user_id,
        s.event_date,
        SUM(TIMESTAMPDIFF(MINUTE, s.start_time, s.end_time)) AS total_minutes
      FROM flow_event_info.tbl_app_session_info s
      WHERE s.event_date BETWEEN '{start_day_str}' AND '{end_day_str}'
      GROUP BY s.user_id, s.event_date
    ),
    experiment_var AS (
      SELECT
        user_id,
        MIN(variation_id) AS variation_id
      FROM flow_wide_info.tbl_wide_experiment_assignment_hi
      WHERE experiment_id = '{experiment_name}'
      GROUP BY user_id
    )
    SELECT
      sa.event_date,
      ev.variation_id AS variation,
      ROUND(
        SUM(sa.total_minutes) / NULLIF(COUNT(DISTINCT sa.user_id), 0),
        2
      ) AS avg_time_spent_minutes,
      '{experiment_name}' AS experiment_name
    FROM session_agg sa
    JOIN experiment_var ev ON sa.user_id = ev.user_id
    GROUP BY sa.event_date, ev.variation_id
    ORDER BY sa.event_date, ev.variation_id;
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        conn.execute(text(insert_query))
        logging.info(f"time_spent 数据插入完成，表名：{table_name}")

    return table_name

# ============= 主流程 =============
def main(tag):
    logging.info("主流程开始执行。")

    table_name = insert_time_spent_data(tag)
    if table_name is None:
        logging.error("数据写入或建表失败！")
        return

    logging.info("主流程执行完毕。")

# ============= 示例调用 =============
if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "trans_es"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
