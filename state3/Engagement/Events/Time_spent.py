import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import logging

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

# ============= 生成明细数据 =============
def insert_page_view_time_spent_data(tag):
    logging.info(f"开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        logging.warning(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    logging.info(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_page_view_time_spent_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_time_spent_minutes DOUBLE,
        user_count INT,
        avg_daily_time_spent_minutes DOUBLE,
        avg_time_per_page_minutes DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))

        query = f"""
        INSERT INTO {table_name} (variation, total_time_spent_minutes, user_count, avg_daily_time_spent_minutes, avg_time_per_page_minutes, experiment_name)
        WITH ordered_page_view AS (
            SELECT
                user_id,
                event_id,
                ingest_timestamp,
                page_name,
                event_date,
                LAG(ingest_timestamp) OVER (PARTITION BY user_id ORDER BY ingest_timestamp) AS prev_timestamp
            FROM flow_event_info.tbl_app_event_page_view
            WHERE event_date BETWEEN :start_date AND :end_date
        ),
        sessionized AS (
            SELECT
                user_id,
                event_id,
                ingest_timestamp,
                page_name,
                event_date,
                CASE 
                    WHEN prev_timestamp IS NULL THEN 0
                    WHEN TIMESTAMPDIFF(SECOND, prev_timestamp, ingest_timestamp) > 1800 THEN 0
                    ELSE TIMESTAMPDIFF(SECOND, prev_timestamp, ingest_timestamp)
                END AS time_spent_seconds
            FROM ordered_page_view
        ),
        user_daily AS (
            SELECT
                user_id,
                event_date,
                SUM(time_spent_seconds) AS daily_time_spent_seconds,
                COUNT(*) AS page_views_count
            FROM sessionized
            GROUP BY user_id, event_date
        ),
        user_summary AS (
            SELECT
                user_id,
                SUM(daily_time_spent_seconds) AS total_time_spent_seconds,
                COUNT(DISTINCT event_date) AS active_days,
                SUM(page_views_count) AS total_page_views
            FROM user_daily
            GROUP BY user_id
        ),
        user_experiment AS (
            SELECT
                user_id,
                variation_id
            FROM flow_wide_info.tbl_wide_experiment_assignment_hi
            WHERE experiment_id = :experiment_name
        )
        SELECT
            ue.variation_id AS variation,
            ROUND(SUM(us.total_time_spent_seconds) / 60, 4) AS total_time_spent_minutes,
            COUNT(*) AS user_count,
            ROUND(AVG(us.total_time_spent_seconds * 1.0 / us.active_days) / 60, 4) AS avg_daily_time_spent_minutes,
            ROUND(AVG(us.total_time_spent_seconds * 1.0 / us.total_page_views) / 60, 4) AS avg_time_per_page_minutes,
            :experiment_name AS experiment_name
        FROM user_summary us
        JOIN user_experiment ue
            ON us.user_id = ue.user_id
        GROUP BY ue.variation_id;
        """
        conn.execute(text(query), {
            "start_date": start_time,
            "end_date": end_time,
            "experiment_name": experiment_name
        })
        logging.info(f"页面浏览时长统计完成，表名：{table_name}")

    return table_name

# ============= 汇总并覆盖表 =============
def overwrite_page_view_time_spent_summary(tag):
    logging.info(f"开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_page_view_time_spent_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_time_spent_minutes) AS total_time_spent_minutes,
        SUM(user_count) AS user_count,
        CASE 
            WHEN SUM(user_count) = 0 THEN 0
            ELSE ROUND(SUM(total_time_spent_minutes) / SUM(user_count), 4)
        END AS avg_time_spent_per_user,
        MAX(experiment_name) AS experiment_name
    FROM {table_name}
    WHERE variation IS NOT NULL
    GROUP BY variation;
    """

    engine = get_db_connection()
    summary_df = pd.read_sql(text(summary_query), engine)

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        # 使用 to_sql 批量插入汇总数据，减少逐行插入开销
        # 这里将 avg_time_spent_per_user 赋值给 avg_daily_time_spent_minutes，avg_time_per_page_minutes 设为 NULL
        summary_df["avg_daily_time_spent_minutes"] = summary_df["avg_time_spent_per_user"]
        summary_df["avg_time_per_page_minutes"] = None
        summary_df.to_sql(table_name, engine, if_exists="append", index=False)

    logging.info(f"汇总数据已覆盖表：{table_name}")

# ============= 主流程 =============
def main(tag):
    logging.info("主流程开始执行。")

    table_name = insert_page_view_time_spent_data(tag)
    if table_name is None:
        logging.error("数据写入或建表失败！")
        return

    overwrite_page_view_time_spent_summary(tag)
    logging.info("主流程执行完毕。")

# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
