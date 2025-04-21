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
    print(f"🚀 开始获取实验 edit 数据（按天按组），标签：{tag}")

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']

    start_day_str = start_time.strftime("%Y-%m-%d")
    end_day_str = end_time.strftime("%Y-%m-%d")

    print(f"📝 实验名称：{experiment_name}")
    print(f"⏰ 实验时间范围：{start_day_str} ~ {end_day_str}")

    engine = get_db_connection()
    table_name = f"tbl_report_edit_daily_{tag}"

    # 创建表（包含中文注释）
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"""
    CREATE TABLE {table_name} (
        event_date DATE COMMENT '日期',
        variation VARCHAR(255) COMMENT '实验分组',
        total_edit INT COMMENT '编辑事件数',
        unique_edit_users INT COMMENT '活跃编辑用户数',
        edit_ratio DOUBLE COMMENT '人均编辑次数',
        experiment_name VARCHAR(255) COMMENT '实验名称'
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(drop_table_query))
        conn.execute(text(create_table_query))
        print(f"✅ 表 {table_name} 已创建。")

        # 遍历每天插入数据（排除首日和末日）
        start_date = datetime.strptime(start_day_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_day_str, "%Y-%m-%d")
        delta_days = (end_date - start_date).days

        for d in range(1, delta_days):
            current_date = (start_date + timedelta(days=d)).strftime("%Y-%m-%d")
            insert_query = f"""
            INSERT INTO {table_name} (event_date, variation, total_edit, unique_edit_users, edit_ratio, experiment_name)
            SELECT
                '{current_date}' AS event_date,
                b.variation_id AS variation,
                COUNT(DISTINCT a.event_id) AS total_edit,
                COUNT(DISTINCT a.user_id) AS unique_edit_users,
                CASE
                    WHEN COUNT(DISTINCT a.user_id) = 0 THEN 0
                    ELSE ROUND(COUNT(DISTINCT a.event_id) * 1.0 / COUNT(DISTINCT a.user_id), 4)
                END AS edit_ratio,
                '{experiment_name}' AS experiment_name
            FROM flow_event_info.tbl_app_event_chat_send a
            JOIN flow_wide_info.tbl_wide_experiment_assignment_hi b
              ON a.user_id = b.user_id
              and a.event_date = b.event_date
            WHERE b.experiment_id = '{experiment_name}'
              AND a.event_date = '{current_date}'
              AND a.Method = 'edit'
            GROUP BY b.variation_id;
            """
            print(f"👉 正在处理日期：{current_date}")
            conn.execute(text(insert_query))

        print(f"✅ 所有每日 edit 数据已插入表 {table_name}。")

    # 加载结果并排序展示
    final_query = f"""
    SELECT 
        event_date AS `日期`,
        variation AS `实验分组`,
        total_edit AS `编辑事件数`,
        unique_edit_users AS `活跃编辑用户数`,
        edit_ratio AS `人均编辑次数`,
        experiment_name AS `实验名称`
    FROM {table_name}
    ORDER BY event_date, variation;
    """

    result_df = pd.read_sql(final_query, engine)

    print("🚀 最终每日 Edit 数据（按天按组返回）：")
    grouped = result_df.groupby(['日期', '实验分组'])

    for (event_date, variation), group in grouped:
        row = group.iloc[0]
        print(f"📅 日期: {event_date} ｜ 分组: {variation}")
        print(f"   ✏️ 编辑事件数: {row['编辑事件数']} ｜ 活跃编辑用户数: {row['活跃编辑用户数']} ｜ 人均编辑次数: {row['人均编辑次数']}")
        print("-" * 50)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        tag = sys.argv[1]
    else:
        tag = "chat_0416"  # 未来可以从外部传入或读取配置
        print(f"⚠️ 未指定实验标签，默认使用：{tag}")
    main(tag)
