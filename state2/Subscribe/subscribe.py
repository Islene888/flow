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


# ============= 插入订阅指标明细数据 =============
def insert_subscribe_data(tag):
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
    table_name = f"tbl_report_subscribe_{tag}"

    # 创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_active_users INT,
        new_subscribe_users INT,
        new_subscribe_rate DOUBLE,
        total_subscribe_revenue DOUBLE,
        subscribe_ARPU DOUBLE,
        renewal_users INT,
        due_users INT,
        renewal_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"目标表 {table_name} 数据已清空。")

        # 插入订阅指标数据
        insert_query = f"""
        INSERT INTO {table_name} (variation, total_active_users, new_subscribe_users, new_subscribe_rate, total_subscribe_revenue, subscribe_ARPU, renewal_users, due_users, renewal_rate, experiment_name)
        WITH 
        exp AS (
          SELECT 
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
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY variation_id
        ),
        new_subscribe AS (
          SELECT 
            e.variation_id,
            COUNT(DISTINCT s.user_id) AS new_subscribe_users
          FROM flow_event_info.tbl_app_event_subscribe s
          JOIN exp e ON s.user_id = e.user_id
          WHERE s.new_subscription = TRUE
            AND s.sub_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY e.variation_id
        ),
        subscribe_revenue AS (
          SELECT 
            e.variation_id,
            SUM(s.revenue) AS total_subscribe_revenue
          FROM flow_event_info.tbl_app_event_subscribe s
          JOIN exp e ON s.user_id = e.user_id
          WHERE s.sub_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY e.variation_id
        ),
        renewal AS (
          SELECT 
            e.variation_id,
            COUNT(DISTINCT s.user_id) AS renewal_users
          FROM flow_event_info.tbl_app_event_subscribe s
          JOIN exp e ON s.user_id = e.user_id
          WHERE s.new_subscription = FALSE
            AND s.sub_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY e.variation_id
        ),
        due_subscriptions AS (
          SELECT 
            e.variation_id,
            COUNT(DISTINCT s.user_id) AS due_users
          FROM flow_event_info.tbl_app_event_subscribe s
          JOIN exp e ON s.user_id = e.user_id
          WHERE s.expiration_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY e.variation_id
        )
        SELECT
          /*+ SET_VAR(query_timeout = 30000) */ 
          a.variation_id,
          a.total_active_users,
          COALESCE(n.new_subscribe_users, 0) AS new_subscribe_users,
          ROUND(COALESCE(n.new_subscribe_users, 0) / a.total_active_users, 4) AS new_subscribe_rate,
          COALESCE(sr.total_subscribe_revenue, 0) AS total_subscribe_revenue,
          ROUND(COALESCE(sr.total_subscribe_revenue, 0) / a.total_active_users, 4) AS subscribe_ARPU,
          COALESCE(r.renewal_users, 0) AS renewal_users,
          COALESCE(d.due_users, 0) AS due_users,
          CASE WHEN COALESCE(d.due_users, 0) = 0 THEN 0 
               ELSE ROUND(COALESCE(r.renewal_users, 0) / d.due_users, 4)
          END AS renewal_rate,
          '{experiment_name}' AS experiment_name
        FROM active_users a
        LEFT JOIN new_subscribe n ON a.variation_id = n.variation_id
        LEFT JOIN subscribe_revenue sr ON a.variation_id = sr.variation_id
        LEFT JOIN renewal r ON a.variation_id = r.variation_id
        LEFT JOIN due_subscriptions d ON a.variation_id = d.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"订阅指标数据插入完成，目标表：{table_name}")
    return table_name


# ============= 汇总并覆盖目标表 =============
def overwrite_subscribe_table_with_summary(tag):
    print(f"开始生成订阅指标汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_subscribe_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_active_users) AS total_active_users,
        SUM(new_subscribe_users) AS new_subscribe_users,
        ROUND(SUM(new_subscribe_users) / SUM(total_active_users), 4) AS new_subscribe_rate,
        SUM(total_subscribe_revenue) AS total_subscribe_revenue,
        ROUND(SUM(total_subscribe_revenue) / SUM(total_active_users), 4) AS subscribe_ARPU,
        SUM(renewal_users) AS renewal_users,
        SUM(due_users) AS due_users,
        ROUND(SUM(renewal_users) / SUM(due_users), 4) AS renewal_rate,
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
        total_active_users INT,
        new_subscribe_users INT,
        new_subscribe_rate DOUBLE,
        total_subscribe_revenue DOUBLE,
        subscribe_ARPU DOUBLE,
        renewal_users INT,
        due_users INT,
        renewal_rate DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation, total_active_users, new_subscribe_users, new_subscribe_rate, total_subscribe_revenue, subscribe_ARPU, renewal_users, due_users, renewal_rate, experiment_name)
            VALUES ('{row['variation']}', {row['total_active_users']}, {row['new_subscribe_users']}, {row['new_subscribe_rate']},
                    {row['total_subscribe_revenue']}, {row['subscribe_ARPU']}, {row['renewal_users']}, {row['due_users']}, {row['renewal_rate']},
                    '{row['experiment_name']}');
            """
            conn.execute(text(insert_query))
    print(f"订阅指标汇总数据已覆盖表：{table_name}")


# ============= 主流程 =============
def main(tag):
    print("主流程开始执行。")
    table_name = insert_subscribe_data(tag)
    if table_name is None:
        print("数据写入或建表失败！")
        return
    overwrite_subscribe_table_with_summary(tag)
    print("主流程执行完毕。")


if __name__ == "__main__":
    main("backend")
