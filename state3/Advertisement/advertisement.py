import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime, timedelta

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)


def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("\u2705 数据库连接已建立。")
    return engine


def insert_ad_data(tag):
    print(f"🚀 开始获取实验数据，标签：{tag}")
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"⚠️ 没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time'].date()
    end_time = experiment_data['phase_end_time'].date()
    print(f"📝 实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

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
        experiment_tag VARCHAR(255)
    );
    """
    truncate_query = f"TRUNCATE TABLE {table_name};"

    with engine.connect() as conn:
        conn.execute(text("SET query_timeout = 30000;"))
        conn.execute(text(create_table_query))
        conn.execute(text(truncate_query))
        print(f"✅ 目标表 {table_name} 数据已清空。")

        current_date = start_time
        while current_date <= end_time:
            print(f"🗕️ 处理日期：{current_date}")

            for batch_index in range(10):
                print(f"📌 执行第 {batch_index + 1}/10 批次 SQL 插入...")

                batch_insert_query = f"""
                SET query_mem_limit=2147483648;

                INSERT INTO {table_name} (
                    variation, total_active_users, total_ad_revenue, ad_arpu,
                    ad_exposure_users, ad_exposure_rate, ad_exposure_count, eCPM,
                    ad_exposure_per_user, experiment_tag
                )
                WITH 
                exp AS (
                  SELECT DISTINCT user_id, variation_id
                  FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                  WHERE experiment_id = '{experiment_name}'
                    AND event_date = '{current_date}'
                    AND MOD(crc32(user_id), 10) = {batch_index}
                ),
                total AS (
                  SELECT variation_id, COUNT(DISTINCT user_id) AS total_active_users
                  FROM exp
                  GROUP BY variation_id
                ),
                ad_revenue AS (
                  SELECT e.variation_id, SUM(p.ad_revenue) AS total_ad_revenue
                  FROM flow_event_info.tbl_app_event_ads_impression p
                  JOIN exp e ON p.user_id = e.user_id
                  WHERE p.event_date = '{current_date}'
                  GROUP BY e.variation_id
                ),
                ad_exposure AS (
                  SELECT e.variation_id,
                         COUNT(*) AS ad_exposure_count,
                         COUNT(DISTINCT p.user_id) AS ad_exposure_users
                  FROM flow_event_info.tbl_app_event_ads_impression p
                  JOIN exp e ON p.user_id = e.user_id
                  WHERE p.event_date = '{current_date}'
                  GROUP BY e.variation_id
                )
                SELECT 
                  t.variation_id,
                  t.total_active_users,
                  ar.total_ad_revenue,
                  ROUND(ar.total_ad_revenue / t.total_active_users, 4) AS ad_arpu,
                  ae.ad_exposure_users,
                  ROUND(ae.ad_exposure_users / t.total_active_users, 4) AS ad_exposure_rate,
                  ae.ad_exposure_count,
                  ROUND((ar.total_ad_revenue / NULLIF(ae.ad_exposure_count, 0)) * 1000, 4) AS eCPM,
                  ROUND(ae.ad_exposure_count / NULLIF(ae.ad_exposure_users, 0), 4) AS ad_exposure_per_user,
                  '{tag}' AS experiment_tag
                FROM total t
                LEFT JOIN ad_revenue ar ON t.variation_id = ar.variation_id
                LEFT JOIN ad_exposure ae ON t.variation_id = ae.variation_id;
                """

                try:
                    conn.execute(text(batch_insert_query))
                    print(f"✅ 日期 {current_date} - 批次 {batch_index + 1}/10 插入成功。")
                except Exception as e:
                    print(f"❌ 日期 {current_date} - 批次 {batch_index + 1}/10 插入失败，错误：{e}")

            current_date += timedelta(days=1)

    print(f"✅ 所有数据插入完成，目标表：{table_name}")
    return table_name


def overwrite_ad_table_with_summary(tag):
    print(f"📊 开始生成汇总数据，并覆盖到原表，标签：{tag}")
    table_name = f"tbl_report_ad_{tag}"

    summary_query = f"""
    SELECT 
        variation,
        SUM(total_active_users) AS total_active_users,
        SUM(total_ad_revenue) AS total_ad_revenue,
        ROUND(SUM(total_ad_revenue) / NULLIF(SUM(total_active_users), 0), 4) AS ad_arpu,
        SUM(ad_exposure_users) AS ad_exposure_users,
        ROUND(SUM(ad_exposure_users) / NULLIF(SUM(total_active_users), 0), 4) AS ad_exposure_rate,
        SUM(ad_exposure_count) AS ad_exposure_count,
        ROUND(SUM(total_ad_revenue) / NULLIF(SUM(ad_exposure_count), 0) * 1000, 4) AS eCPM,
        ROUND(SUM(ad_exposure_count) / NULLIF(SUM(ad_exposure_users), 0), 4) AS ad_exposure_per_user,
        MAX(experiment_tag) AS experiment_tag
    FROM {table_name}
    WHERE variation IS NOT NULL AND variation != 'null'
    GROUP BY variation;
    """

    engine = get_db_connection()
    summary_df = pd.read_sql(summary_query, engine)

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        summary_df.to_sql(table_name, conn, if_exists="append", index=False)

    print(f"✅ 汇总数据已覆盖表：{table_name}")


def main(tag):
    print("🚀 主流程开始执行。")
    table_name = insert_ad_data(tag)
    if table_name is None:
        print("⚠️ 数据写入或建表失败！")
        return
    overwrite_ad_table_with_summary(tag)
    print("✅ 主流程执行完毕。")


if __name__ == "__main__":
    main("trans_es")