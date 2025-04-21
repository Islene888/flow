import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


# ============= 数据库连接 =============
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("数据库连接已建立。")
    return engine


# ============= 插入充值指标明细数据 =============
def insert_recharge_data(tag):
    print(f"开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    # 获取实验参数
    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_recharge_{tag}"

    # 创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_active_users INT,
        total_recharge_revenue DOUBLE,
        recharge_ARPU DOUBLE,
        recharge_conversion_rate DOUBLE,
        recharge_frequency DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"目标表 {table_name} 数据已清空。")

        insert_query = f"""
             INSERT INTO {table_name}
(variation, total_active_users, total_recharge_revenue, recharge_ARPU, recharge_conversion_rate, recharge_frequency, experiment_tag)
        WITH 
        exp AS (
          SELECT DISTINCT
            user_id, 
            variation_id
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
        ),
        active_users AS (
          SELECT 
            variation_id, 
            COUNT(DISTINCT user_id) AS total_active_users
          FROM exp
          GROUP BY variation_id
        ),
        recharge_stats AS (
          SELECT 
            e.variation_id,
            COUNT(*) AS total_recharge_orders,
            COUNT(DISTINCT p.user_id) AS recharge_user_count,
            SUM(p.revenue) AS total_recharge_revenue,
            COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT p.user_id), 0) AS recharge_frequency
          FROM flow_event_info.tbl_app_event_currency_purchase p
          JOIN exp e 
            ON p.user_id = e.user_id
          WHERE p.event_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY e.variation_id
        )
        SELECT 
          a.variation_id,
          a.total_active_users,
          r.total_recharge_revenue,
          ROUND(r.total_recharge_revenue / a.total_active_users, 4) AS recharge_ARPU,
          ROUND(r.recharge_user_count * 1.0 / a.total_active_users, 4) AS recharge_conversion_rate,
          ROUND(r.recharge_frequency, 4) AS recharge_frequency,
          '{tag}' AS experiment_tag
        FROM active_users a
        LEFT JOIN recharge_stats r 
          ON a.variation_id = r.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"充值指标数据已插入到表 {table_name}")
    return table_name


# ============= 汇总并覆盖目标表 =============
def overwrite_recharge_table_with_summary(tag):
    print(f"开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_recharge_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_active_users) AS total_active_users,
        SUM(total_recharge_revenue) AS total_recharge_revenue,
        ROUND(SUM(total_recharge_revenue) / SUM(total_active_users), 4) AS recharge_ARPU,
        SUM(recharge_conversion_rate) AS recharge_conversion_rate,
        SUM(recharge_frequency) AS recharge_frequency,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation != 'null'
    GROUP BY variation;
    """
    engine = get_db_connection()
    summary_df = pd.read_sql(summary_query, engine)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_active_users INT,
        total_recharge_revenue DOUBLE,
        recharge_ARPU DOUBLE,
        recharge_conversion_rate DOUBLE,
        recharge_frequency DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation, total_active_users, total_recharge_revenue, recharge_ARPU, recharge_conversion_rate, recharge_frequency, experiment_tag)
            VALUES ('{row['variation']}', {row['total_active_users']}, {row['total_recharge_revenue']}, {row['recharge_ARPU']}, {row['recharge_conversion_rate']}, {row['recharge_frequency']}, '{row['experiment_tag']}');
            """
            conn.execute(text(insert_query))
    print(f"汇总数据已覆盖表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("主流程开始执行。")
    table_name = insert_recharge_data(tag)
    if table_name is None:
        print("数据写入或建表失败！")
        return
    overwrite_recharge_table_with_summary(tag)
    print("主流程执行完毕。")


if __name__ == "__main__":
    main("chat_0416")