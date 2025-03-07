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


# ============= 分片插入原始明细数据 =============
def insert_regen_data(tag):
    print(f"开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()

    table_name = f"tbl_report_regen_{tag}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_regen INT,
        unique_regen_users INT,
        regen_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"目标表 {table_name} 数据已清空。")

        for mod_value in range(100):
            batch_insert_query = f"""
            INSERT INTO {table_name} (variation, total_regen, unique_regen_users, regen_ratio, experiment_name)
            SELECT
                a.variation_id AS variation,
                COUNT(DISTINCT f.event_id) AS total_regen,
                COUNT(DISTINCT f.user_id) AS unique_regen_users,
                CASE 
                    WHEN COUNT(DISTINCT f.user_id) = 0 THEN 0 
                    ELSE ROUND(COUNT(DISTINCT f.event_id) * 1.0 / COUNT(DISTINCT f.user_id), 4)
                END AS regen_ratio,
                '{experiment_name}' as experiment_name
            FROM flow_event_info.tbl_app_event_chat_send f
            JOIN flow_wide_info.tbl_wide_experiment_assignment_hi a
                ON f.user_id = a.user_id
            WHERE a.experiment_id = '{experiment_name}'
              AND f.ingest_timestamp BETWEEN '{start_time}' AND '{end_time}'
              AND f.method = 'regenerate'
              AND MOD(crc32(f.user_id), 100) = {mod_value}
            GROUP BY a.variation_id;
            """
            conn.execute(text(batch_insert_query))
            print(f"批次插入完成，分片条件：MOD(f.user_id, 10) = {mod_value}")

    print(f"所有批次数据插入完成，目标表：{table_name}")
    return table_name


# ============= 计算汇总并覆盖原表 =============
def overwrite_regen_table_with_summary(tag):
    print(f"开始生成汇总数据，并覆盖到原表，标签：{tag}")

    table_name = f"tbl_report_regen_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_regen) AS total_regen,
        SUM(unique_regen_users) AS unique_regen_users,
        ROUND(SUM(total_regen) / SUM(unique_regen_users), 4) AS regen_ratio,
        MAX(experiment_name) AS experiment_name
    FROM {table_name}
    WHERE variation != 'null'
    GROUP BY variation;
    """

    engine = get_db_connection()
    summary_df = pd.read_sql(summary_query, engine)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_regen INT,
        unique_regen_users INT,
        regen_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation, total_regen, unique_regen_users, regen_ratio, experiment_name)
            VALUES ('{row['variation']}', {row['total_regen']}, {row['unique_regen_users']}, {row['regen_ratio']}, '{row['experiment_name']}');
            """
            conn.execute(text(insert_query))

    print(f"汇总数据已覆盖表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("主流程开始执行。")

    # Step 1: 插入原始分片数据
    table_name = insert_regen_data(tag)
    if table_name is None:
        print("数据写入或建表失败！")
        return

    # Step 2: 汇总并覆盖原表
    overwrite_regen_table_with_summary(tag)

    print("主流程执行完毕。")


# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
