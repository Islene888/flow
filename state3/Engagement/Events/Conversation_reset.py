import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta
import sys

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine

def main(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")

    # 获取实验信息
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time   = experiment_data['phase_end_time']

    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_time_str   = end_time.strftime("%Y-%m-%d %H:%M:%S")

    # 用于外层过滤的首日和末日
    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str   = end_time.strftime("%Y-%m-%d")

    print(f"📝 实验名称：{experiment_name}")
    print(f"⏰ 计算时间范围：{start_time_str} ~ {end_time_str}")
    print(f"   首日：{start_day_str}，末日：{end_day_str}")

    engine = get_db_connection()
    # 修改目标表名，将 new_conversation 改为 conversation_reset
    table_name = f"tbl_report_conversation_reset_{tag}"

    # 建表（如表存在则覆盖），字段名称做相应修改
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date VARCHAR(255),
        variation VARCHAR(255),
        total_conversation_reset INT,
        unique_conversation_reset_users INT,
        conversation_reset_ratio DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    # 执行建表操作
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

    # 将开始和结束日期转换为 datetime 对象，并计算中间日期（不包含首日和末日）
    start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
    delta_days = (end_date - start_date).days

    # 遍历首日之后到末日前的每一天，分批插入数据
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            batch_insert_query = f"""
            INSERT INTO {table_name} (event_date, variation, total_conversation_reset, unique_conversation_reset_users, conversation_reset_ratio, experiment_name)
            SELECT
                a.event_date,
                b.variation_id AS variation,
                COUNT(DISTINCT a.event_id) AS total_conversation_reset,
                COUNT(DISTINCT a.user_id) AS unique_conversation_reset_users,
                CASE
                    WHEN COUNT(DISTINCT a.user_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT a.event_id) * 1.0 / COUNT(DISTINCT a.user_id), 4)
                END AS conversation_reset_ratio,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_conversation_reset a
            JOIN flow_wide_info.tbl_wide_experiment_assignment_hi b
                ON a.user_id = b.user_id
            WHERE b.experiment_id = '{experiment_name}'
              AND a.ingest_timestamp BETWEEN '{start_time_str}' AND '{end_time_str}'
              AND a.event_date = '{current_date}'
            GROUP BY a.event_date, b.variation_id
            ORDER BY a.event_date, b.variation_id;
            """
            print(f"👉 正在插入日期：{current_date}")
            conn.execute(text(batch_insert_query))
        print(f"✅ 所有批次数据已插入到表 {table_name} 中。")

    # 查询结果
    result_df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY event_date, variation;", engine)
    print("🚀 最终表数据:")
    print(result_df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "trans_es"
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
