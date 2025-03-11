import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

# ============= 数据库连接 =============
def get_db_connection():
    """
    建立并返回数据库连接引擎。
    """
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine

# ============= 插入 LTV 明细数据 =============
def insert_ltv_data(tag):
    """
    插入 LTV 明细数据：
      - 获取实验信息，统计实验期间每个 variation 的首购用户，
        计算 7 日内收入（revenue_7d）和实验期内收入（revenue_exp），
        计算 LTV7 与 LTV_experiment 指标；
      - 在写入前创建目标表（如果不存在）并清空数据。
    """
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    # 获取实验参数
    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_ltv_{tag}"

    # 创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        paying_users INT,
        total_revenue_7d DOUBLE,
        LTV7 DOUBLE,
        total_revenue_exp DOUBLE,
        LTV_experiment DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 已创建并清空数据。")

        # 插入 LTV 明细数据，利用 WITH 子查询计算指标
        insert_query = f"""
        INSERT INTO {table_name} (variation_id, paying_users, total_revenue_7d, LTV7, total_revenue_exp, LTV_experiment, experiment_tag)
        WITH 
        exp AS (
          SELECT 
            user_id, 
            variation_id
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
        ),
        first_pay AS (
          SELECT 
            p.user_id,
            e.variation_id,
            MIN(p.event_date) AS first_pay_date
          FROM flow_event_info.tbl_app_event_all_purchase p
          JOIN exp e ON p.user_id = e.user_id
          WHERE p.type IN ('subscription', 'currency')
          GROUP BY p.user_id, e.variation_id
        ),
        ltv7 AS (
          SELECT 
            fp.user_id,
            fp.variation_id,
            SUM(p.revenue) AS revenue_7d
          FROM first_pay fp
          JOIN flow_event_info.tbl_app_event_all_purchase p ON p.user_id = fp.user_id
          WHERE p.type IN ('subscription', 'currency')
            AND p.event_date BETWEEN fp.first_pay_date AND DATE_ADD(fp.first_pay_date, INTERVAL 7 DAY)
          GROUP BY fp.user_id, fp.variation_id
        ),
        ltv_exp AS (
          SELECT 
            fp.user_id,
            fp.variation_id,
            SUM(p.revenue) AS revenue_exp
          FROM first_pay fp
          JOIN flow_event_info.tbl_app_event_all_purchase p ON p.user_id = fp.user_id
          WHERE p.type IN ('subscription', 'currency')
            AND p.event_date BETWEEN fp.first_pay_date AND '{end_time}'
          GROUP BY fp.user_id, fp.variation_id
        )
        SELECT 
          fp.variation_id,
          COUNT(*) AS paying_users,
          SUM(l7.revenue_7d) AS total_revenue_7d,
          ROUND(SUM(l7.revenue_7d) / COUNT(*), 4) AS LTV7,
          SUM(le.revenue_exp) AS total_revenue_exp,
          ROUND(SUM(le.revenue_exp) / COUNT(*), 4) AS LTV_experiment,
          '{tag}' AS experiment_tag
        FROM first_pay fp
        LEFT JOIN ltv7 l7 ON fp.user_id = l7.user_id AND fp.variation_id = l7.variation_id
        LEFT JOIN ltv_exp le ON fp.user_id = le.user_id AND fp.variation_id = le.variation_id
        GROUP BY fp.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"✅ LTV 明细数据已插入到表 {table_name}")
    return table_name

# ============= 汇总并覆盖目标表 =============
def overwrite_ltv_table_with_summary(tag):
    """
    将插入的 LTV 明细数据进行汇总（按 variation_id 聚合），
    重新计算 LTV 指标，然后覆盖写入目标表，同时添加 experiment_tag 字段。
    """
    print(f"🚀 开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_ltv_{tag}"
    engine = get_db_connection()

    summary_query = f"""
    SELECT 
        variation_id,
        SUM(paying_users) AS paying_users,
        SUM(total_revenue_7d) AS total_revenue_7d,
        SUM(total_revenue_exp) AS total_revenue_exp,
        ROUND(SUM(total_revenue_7d) / SUM(paying_users), 4) AS LTV7,
        ROUND(SUM(total_revenue_exp) / SUM(paying_users), 4) AS LTV_experiment,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation_id != 'null'
    GROUP BY variation_id;
    """
    summary_df = pd.read_sql(text(summary_query), engine)

    # 重新创建目标表（如果不存在）并清空数据
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(255),
        paying_users INT,
        total_revenue_7d DOUBLE,
        LTV7 DOUBLE,
        total_revenue_exp DOUBLE,
        LTV_experiment DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """
    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        print(f"✅ 目标表 {table_name} 已重新清空。")

        # 将汇总结果逐行写入目标表
        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation_id, paying_users, total_revenue_7d, LTV7, total_revenue_exp, LTV_experiment, experiment_tag)
            VALUES ('{row['variation_id']}', {row['paying_users']}, {row['total_revenue_7d']}, {row['LTV7']}, {row['total_revenue_exp']}, {row['LTV_experiment']}, '{row['experiment_tag']}');
            """
            conn.execute(text(insert_query))
    print(f"✅ 汇总数据已覆盖到表：{table_name}")

# ============= 主流程 =============
def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_ltv_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    overwrite_ltv_table_with_summary(tag)
    print("🚀 主流程执行完毕。")

if __name__ == "__main__":
    main("backend")
