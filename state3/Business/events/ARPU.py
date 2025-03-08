import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=FutureWarning)

def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    print("数据库连接已建立。")
    return engine

def get_arpu_report(tag):
    # 获取实验详细信息
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    query = f"""
    WITH 
    exp AS (
      SELECT 
        user_id, 
        variation_id
      FROM flow_wide_info.tbl_wide_experiment_assignment_hi AS a
      WHERE experiment_id = '{experiment_name}'
        AND event_date BETWEEN '{start_time}' AND '{end_time}'
    ),
    total AS (
      SELECT 
        variation_id,
        COUNT(DISTINCT user_id) AS total_active_users
      FROM flow_wide_info.tbl_wide_experiment_assignment_hi AS a
      WHERE experiment_id = '{experiment_name}'
        AND event_date BETWEEN '{start_time}' AND '{end_time}'
      GROUP BY variation_id
    ),
    revenue AS (
      SELECT 
        e.variation_id,
        SUM(p.revenue) AS total_revenue
      FROM flow_event_info.tbl_app_event_all_purchase p
      JOIN exp e 
        ON p.user_id = e.user_id
      WHERE p.type IN ('subscription', 'currency')
      GROUP BY e.variation_id
    )
    SELECT
      /*+ SET_VAR (query_timeout = 30000) */ 
      t.variation_id,
      t.total_active_users,
      r.total_revenue,
      ROUND(r.total_revenue / t.total_active_users, 4) AS ARPU
    FROM total t
    LEFT JOIN revenue r ON t.variation_id = r.variation_id;
    """
    engine = get_db_connection()
    df = pd.read_sql(query, engine)
    return df

def main(tag):
    df = get_arpu_report(tag)
    if df is not None:
        print("ARPU 报告：")
        print(df)

if __name__ == "__main__":
    main("backend")
