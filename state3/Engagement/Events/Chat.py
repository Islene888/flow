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


# ============= 分批插入 Chat 事件数据 =============
def insert_chat_data(tag):
    print(f"🚀 开始获取 Chat 事件数据，标签：{tag}")

    # 获取实验信息
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_chat_{tag}"

    # **创建目标表**
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_chat INT,
        total_chat_users INT,
        chat_per_user DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建，并已清空历史数据。")

        # **按天循环**
        # **按天循环**
        current_date = start_time  # 直接使用 datetime 对象
        end_date = end_time  # 直接使用 datetime 对象

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")  # 直接调用 strftime() 转换成字符串

            print(f"📅 处理日期：{date_str}")

            # **分 10 批次插入**
            for batch_index in range(10):
                print(f"📌 执行日期 {date_str}，批次 {batch_index + 1}/10 插入...")

                batch_insert_query = f"""
                INSERT INTO {table_name} (variation, total_chat, total_chat_users, chat_per_user, experiment_tag)
                SELECT /*+ SET_VAR(query_timeout = 30000, query_mem_limit = 2147483648) */
                    a.variation_id AS variation,
                    COUNT(c.event_id) AS total_chat,
                    COUNT(DISTINCT c.user_id) AS total_chat_users,
                    ROUND(COUNT(c.event_id) * 1.0 / COUNT(DISTINCT c.user_id), 4) AS chat_per_user,
                    '{tag}' AS experiment_tag
                FROM flow_wide_info.tbl_wide_chat_llm_info_hi c
                JOIN flow_wide_info.tbl_wide_experiment_assignment_hi a
                    ON c.user_id = a.user_id
                WHERE a.experiment_id = '{experiment_name}'
                  AND a.event_date = '{date_str}'  -- 只使用 experiment_assignment 表的 event_date
                  AND MOD(crc32(c.user_id), 10) = {batch_index} -- 分 10 批次
                GROUP BY a.variation_id;
                """
                try:
                    conn.execute(text(batch_insert_query))
                    print(f"✅ 日期 {date_str}，批次 {batch_index + 1}/10 插入成功。")
                except Exception as e:
                    print(f"❌ 日期 {date_str}，批次 {batch_index + 1}/10 插入失败，错误：{e}")

            # **日期加 1 天**
            current_date += timedelta(days=1)

    print(f"✅ Chat 事件数据插入完成，目标表：{table_name}")
    return table_name


# ============= 计算汇总并覆盖原表 =============
def overwrite_chat_table_with_summary(tag):
    print(f"📊 开始生成 Chat 事件汇总数据，并覆盖到原表，标签：{tag}")

    table_name = f"tbl_report_chat_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_chat) AS total_chat,
        SUM(total_chat_users) AS total_chat_users,
        ROUND(SUM(total_chat) / SUM(total_chat_users), 4) AS chat_per_user,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation != 'null'
    GROUP BY variation;
    """

    engine = get_db_connection()
    summary_df = pd.read_sql(summary_query, engine)

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation, total_chat, total_chat_users, chat_per_user, experiment_tag)
            VALUES ('{row['variation']}', {row['total_chat']}, {row['total_chat_users']}, {row['chat_per_user']}, '{row['experiment_tag']}');
            """
            conn.execute(text(insert_query))

    print(f"✅ Chat 事件汇总数据已覆盖表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行 - Chat 事件")

    table_name = insert_chat_data(tag)
    if table_name is None:
        print("⚠️ Chat 事件数据写入或建表失败！")
        return

    overwrite_chat_table_with_summary(tag)

    print("✅ 主流程执行完毕 - Chat 事件")


# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
