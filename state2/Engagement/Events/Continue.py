import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("数据库连接已建立。")
    return engine


# ============= 动态建表并写入数据 =============

def insert_continue_data(tag):
    print(f"开始获取实验数据，标签：{tag}")
    # 获取实验的详细信息
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()

    # 动态生成目标表名称
    table_name = f"tbl_report_continue_{tag}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_continue INT,
        unique_continue_users INT,
        continue_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    # 清空目标表数据
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        print("开始设置查询超时，并创建目标表...")
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        print(f"目标表 {table_name} 创建成功或已存在。")
        conn.execute(text(truncate_query))
        print(f"目标表 {table_name} 数据已清空。")

        # 分10批插入
        for mod_value in range(100):
            print(f"开始批次插入，分片条件：MOD(c.user_id, 100) = {mod_value}")
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
              AND c.ingest_timestamp BETWEEN '{start_time}' AND '{end_time}'
              AND c.method = 'continue'
              AND MOD(crc32(c.user_id), 100) = {mod_value}
            GROUP BY a.variation_id;
            """
            conn.execute(text(batch_insert_query))
            print(f"批次插入完成，分片条件：MOD(c.user_id, 100) = {mod_value}")

    print(f"所有批次数据插入完成，目标表：{table_name}")
    return table_name


# ============= 主流程 =============
def main(tag):
    print("主流程开始执行。")
    # 先插入数据
    table_name = insert_continue_data(tag)
    if table_name is None:
        print("数据写入或建表失败！")
        return
    print("主流程执行完毕。")


# 示例调用
if __name__ == "__main__":
    # 请根据实际情况传入合适的 tag 值
    main("backend")
