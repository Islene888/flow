import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import logging
from datetime import datetime, timedelta

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

# ============= 分天分批插入 Bot View 数据 =============
def insert_bot_view_data(tag):
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
    table_name = f"tbl_report_bot_view_{tag}"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_bot_view INT,
        unique_bot_view_users INT,
        bot_view_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        logging.info(f"目标表 {table_name} 数据已清空。")

        # 确保 start_time 和 end_time 为 datetime 对象，否则需转换
        current_date = start_time.date() if isinstance(start_time, datetime) else start_time
        end_date = end_time.date() if isinstance(end_time, datetime) else end_time

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d") if isinstance(current_date, datetime) else str(current_date)
            logging.info(f"开始处理日期：{date_str}")
            for mod_value in range(100):
                logging.info(f"正在处理日期 {date_str} 批次 {mod_value+1}/100")
                batch_insert_query = f"""
                INSERT INTO {table_name} (variation, total_bot_view, unique_bot_view_users, bot_view_ratio, experiment_name)
                SELECT 
                    a.variation_id AS variation,
                    COUNT(DISTINCT b.event_id) AS total_bot_view,
                    COUNT(DISTINCT b.user_id) AS unique_bot_view_users,
                    CASE 
                        WHEN COUNT(DISTINCT b.user_id) = 0 THEN 0 
                        ELSE ROUND(COUNT(DISTINCT b.event_id)*1.0/COUNT(DISTINCT b.user_id),4)
                    END AS bot_view_ratio,
                    :experiment_name AS experiment_name
                FROM flow_event_info.tbl_app_event_bot_view b
                JOIN flow_wide_info.tbl_wide_experiment_assignment_hi a
                    ON b.user_id = a.user_id
                WHERE a.experiment_id = :experiment_name
                  AND b.event_date = :date_str
                  AND MOD(crc32(b.user_id), 100) = :mod_value
                GROUP BY a.variation_id;
                """
                try:
                    conn.execute(text(batch_insert_query), {
                        "experiment_name": experiment_name,
                        "date_str": date_str,
                        "mod_value": mod_value
                    })
                    logging.info(f"日期 {date_str} 批次 {mod_value+1}/100 插入成功。")
                except Exception as e:
                    logging.error(f"日期 {date_str} 批次 {mod_value+1}/100 插入失败，错误：{e}")
            current_date += timedelta(days=1)

    logging.info(f"所有批次数据插入完成，目标表：{table_name}")
    return table_name

# ============= 汇总并覆盖 Bot View 表 =============
def overwrite_bot_view_table_with_summary(tag):
    logging.info(f"开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_bot_view_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_bot_view) AS total_bot_view,
        SUM(unique_bot_view_users) AS unique_bot_view_users,
        CASE 
            WHEN SUM(unique_bot_view_users) = 0 THEN 0
            ELSE ROUND(SUM(total_bot_view) / SUM(unique_bot_view_users), 4)
        END AS bot_view_ratio,
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
        summary_df.to_sql(table_name, engine, if_exists="append", index=False)

    logging.info(f"汇总数据已覆盖表：{table_name}")

# ============= 主流程 =============
def main(tag):
    logging.info("主流程开始执行。")
    table_name = insert_bot_view_data(tag)
    if table_name is None:
        logging.error("数据写入或建表失败！")
        return
    overwrite_bot_view_table_with_summary(tag)
    logging.info("主流程执行完毕。")

# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
