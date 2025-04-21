import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)


def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("✅ 数据库连接已建立。")
    return engine


def insert_ad_metrics_by_variation(tag):
    print(f"开始获取广告指标（按 variation 汇总），标签: {tag}")
    from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"❌ 没有找到符合标签 '{tag}' 的实验数据！")
        return

    experiment_name = experiment_data["experiment_name"]
    start_time = experiment_data["phase_start_time"].strftime("%Y-%m-%d")
    end_time = experiment_data["phase_end_time"].strftime("%Y-%m-%d")
    print(f"实验名称: {experiment_name}, 时间: {start_time} ~ {end_time}")

    engine = get_db_connection()
    table_name = f"tbl_report_ad_{tag}"

    create_table_query = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE IF NOT EXISTS {table_name} (
        variation_id VARCHAR(64),
        total_active_users INT,
        total_ad_revenue DOUBLE,
        ad_arpu DOUBLE,
        ad_exposure_users INT,
        ad_exposure_rate DOUBLE,
        ad_exposure_count INT,
        ecpm DOUBLE,
        ad_exposure_per_user DOUBLE,
        experiment_tag VARCHAR(255)
    );
    """

    insert_query = f"""
    INSERT INTO {table_name}
    WITH 
    exp AS (
      SELECT DISTINCT user_id, variation_id
      FROM flow_wide_info.tbl_wide_experiment_assignment_hi
      WHERE experiment_id = '{experiment_name}'
        AND event_date BETWEEN '{start_time}' AND '{end_time}'
    ),
    experiment_users AS (
      SELECT variation_id, COUNT(DISTINCT user_id) AS total_active_users
      FROM exp
      GROUP BY variation_id
    ),
    ad_revenue AS (
      SELECT e.variation_id, SUM(p.ad_revenue) AS total_ad_revenue
      FROM flow_event_info.tbl_app_event_ads_end p
      JOIN exp e ON p.user_id = e.user_id
      GROUP BY e.variation_id
    ),
    ad_exposure AS (
      SELECT e.variation_id,
             COUNT(*) AS ad_exposure_count,
             COUNT(DISTINCT p.user_id) AS ad_exposure_users
      FROM flow_event_info.tbl_app_event_ads_impression p
      JOIN exp e ON p.user_id = e.user_id
      GROUP BY e.variation_id
    )
    SELECT
      a.variation_id,
      a.total_active_users,
      COALESCE(r.total_ad_revenue, 0) AS total_ad_revenue,
      ROUND(COALESCE(r.total_ad_revenue, 0) / NULLIF(a.total_active_users, 0), 4) AS ad_arpu,
      COALESCE(e.ad_exposure_users, 0) AS ad_exposure_users,
      ROUND(COALESCE(e.ad_exposure_users, 0) / NULLIF(a.total_active_users, 0), 4) AS ad_exposure_rate,
      COALESCE(e.ad_exposure_count, 0) AS ad_exposure_count,
      CASE 
        WHEN COALESCE(e.ad_exposure_count, 0) = 0 THEN 0
        ELSE ROUND((r.total_ad_revenue / e.ad_exposure_count) * 1000, 4)
      END AS ecpm,
      CASE 
        WHEN COALESCE(e.ad_exposure_users, 0) = 0 THEN 0
        ELSE ROUND(e.ad_exposure_count / e.ad_exposure_users, 4)
      END AS ad_exposure_per_user,
      '{tag}' AS experiment_tag
    FROM experiment_users a
    LEFT JOIN ad_revenue r ON a.variation_id = r.variation_id
    LEFT JOIN ad_exposure e ON a.variation_id = e.variation_id;
    """

    try:
        with engine.connect() as conn:
            for stmt in create_table_query.strip().split(";"):
                if stmt.strip():
                    conn.execute(text(stmt))
            conn.execute(text(insert_query))
        print(f"✅ 广告指标（按 variation 汇总）已写入表：{table_name}")
    except Exception as e:
        print(f"❌ 执行失败：{e}")


def main():
    tag = "trans_ru"  # 修改为你实际的 tag
    insert_ad_metrics_by_variation(tag)


if __name__ == "__main__":
    main()
