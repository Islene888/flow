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

# ============= 插入广告指标明细数据 =============
def insert_ad_data(tag):
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
    table_name = f"tbl_report_ad_{tag}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation VARCHAR(255),
        total_active_users INT,
        total_ad_revenue DOUBLE,
        ad_arpu DOUBLE,
        ad_exposure_users INT,
        ad_exposure_rate DOUBLE,
        ad_exposure_count INT,
        eCPM DOUBLE,
        ad_exposure_per_user DOUBLE,
        experiment_name VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"目标表 {table_name} 数据已清空。")

        # 这里直接一次性插入，不分批（如果数据量大，也可采用分片方式）
        insert_query = f"""
        INSERT INTO {table_name} (variation, total_active_users, total_ad_revenue, ad_arpu, ad_exposure_users, ad_exposure_rate, ad_exposure_count, eCPM, ad_exposure_per_user, experiment_name)
        WITH 
        exp AS (
          SELECT 
            user_id, 
            variation_id
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
        ),
        total AS (
          SELECT 
            variation_id,
            COUNT(DISTINCT user_id) AS total_active_users
          FROM flow_wide_info.tbl_wide_experiment_assignment_hi
          WHERE experiment_id = '{experiment_name}'
            AND event_date BETWEEN '{start_time}' AND '{end_time}'
          GROUP BY variation_id
        ),
        ad_revenue AS (
          SELECT 
            e.variation_id,
            SUM(p.ad_revenue) AS total_ad_revenue
          FROM flow_event_info.tbl_app_event_ads_end p
          JOIN exp e 
            ON p.user_id = e.user_id
          GROUP BY e.variation_id
        ),
        ad_exposure AS (
          SELECT 
            e.variation_id, 
            COUNT(*) AS ad_exposure_count,
            COUNT(DISTINCT p.user_id) AS ad_exposure_users
          FROM flow_event_info.tbl_app_event_ads_impression p
          JOIN exp e 
            ON p.user_id = e.user_id
          GROUP BY e.variation_id
        )
        SELECT
          /*+ SET_VAR(query_timeout = 30000) */ 
          t.variation_id,
          t.total_active_users,
          ar.total_ad_revenue,
          ROUND(ar.total_ad_revenue / t.total_active_users, 4) AS ad_arpu,
          ae.ad_exposure_users,
          ROUND(ae.ad_exposure_users / t.total_active_users, 4) AS ad_exposure_rate,
          ae.ad_exposure_count,
          ROUND((ar.total_ad_revenue / ae.ad_exposure_count) * 1000, 4) AS eCPM,
          ROUND(ae.ad_exposure_count / ae.ad_exposure_users, 4) AS ad_exposure_per_user,
          '{experiment_name}' AS experiment_name
        FROM total t
        LEFT JOIN ad_revenue ar ON t.variation_id = ar.variation_id
        LEFT JOIN ad_exposure ae ON t.variation_id = ae.variation_id;
        """
        conn.execute(text(insert_query))
        print(f"广告指标数据插入完成，目标表：{table_name}")
    return table_name

# ============= 汇总并覆盖表 =============
def overwrite_ad_table_with_summary(tag):
    print(f"开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_ad_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_active_users) AS total_active_users,
        SUM(total_ad_revenue) AS total_ad_revenue,
        ROUND(SUM(total_ad_revenue) / SUM(total_active_users), 4) AS ad_arpu,
        SUM(ad_exposure_users) AS ad_exposure_users,
        ROUND(SUM(ad_exposure_users) / SUM(total_active_users), 4) AS ad_exposure_rate,
        SUM(ad_exposure_count) AS ad_exposure_count,
        ROUND((SUM(total_ad_revenue) / SUM(ad_exposure_count)) * 1000, 4) AS eCPM,
        ROUND(SUM(ad_exposure_count) / SUM(ad_exposure_users), 4) AS ad_exposure_per_user,
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
        total_ad_revenue DOUBLE,
        ad_arpu DOUBLE,
        ad_exposure_users INT,
        ad_exposure_rate DOUBLE,
        ad_exposure_count INT,
        eCPM DOUBLE,
        ad_exposure_per_user DOUBLE,
        experiment_name VARCHAR(255)
    );
    """

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

        for _, row in summary_df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (variation, total_active_users, total_ad_revenue, ad_arpu, ad_exposure_users, ad_exposure_rate, ad_exposure_count, eCPM, ad_exposure_per_user, experiment_name)
            VALUES ('{row['variation']}', {row['total_active_users']}, {row['total_ad_revenue']}, {row['ad_arpu']},
                    {row['ad_exposure_users']}, {row['ad_exposure_rate']}, {row['ad_exposure_count']}, {row['eCPM']}, {row['ad_exposure_per_user']},
                    '{row['experiment_name']}');
            """
            conn.execute(text(insert_query))
    print(f"汇总数据已覆盖表：{table_name}")

# ============= 主流程 =============
def main(tag):
    print("主流程开始执行。")
    table_name = insert_ad_data(tag)
    if table_name is None:
        print("数据写入或建表失败！")
        return
    overwrite_ad_table_with_summary(tag)
    print("主流程执行完毕。")

# ============= 示例调用 =============
if __name__ == "__main__":
    main("backend")
