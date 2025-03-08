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

def get_ltv_report(tag):
    # 获取实验信息
    experiment_data = get_experiment_details_by_tag(tag)
    if not experiment_data:
        print(f"没有找到符合标签 '{tag}' 的实验数据！")
        return None

    experiment_name = experiment_data['experiment_name']
    start_time = experiment_data['phase_start_time']
    end_time = experiment_data['phase_end_time']
    print(f"实验名称：{experiment_name}，实验时间：{start_time} 至 {end_time}")

    # 构造 SQL 查询：统计 LTV7（首次付费后7天内收入平均值）和 LTV 实验周期（首次付费后至实验结束的收入平均值）
    query = f"""
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
      JOIN exp e 
        ON p.user_id = e.user_id
      WHERE p.type IN ('subscription', 'currency')
      GROUP BY p.user_id, e.variation_id
    ),
    ltv7 AS (
      SELECT 
        fp.user_id,
        fp.variation_id,
        SUM(p.revenue) AS revenue_7d
      FROM first_pay fp
      JOIN flow_event_info.tbl_app_event_all_purchase p 
        ON p.user_id = fp.user_id
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
      JOIN flow_event_info.tbl_app_event_all_purchase p 
        ON p.user_id = fp.user_id
      WHERE p.type IN ('subscription', 'currency')
        AND p.event_date BETWEEN fp.first_pay_date AND '{end_time}'
      GROUP BY fp.user_id, fp.variation_id
    )
    SELECT
      /*+ SET_VAR (query_timeout = 30000) */ 
      fp.variation_id,
      COUNT(*) AS paying_users,
      SUM(l7.revenue_7d) AS total_revenue_7d,
      ROUND(SUM(l7.revenue_7d) / COUNT(*), 4) AS LTV7,
      SUM(le.revenue_exp) AS total_revenue_exp,
      ROUND(SUM(le.revenue_exp) / COUNT(*), 4) AS LTV_experiment
    FROM first_pay fp
    LEFT JOIN ltv7 l7 
      ON fp.user_id = l7.user_id AND fp.variation_id = l7.variation_id
    LEFT JOIN ltv_exp le 
      ON fp.user_id = le.user_id AND fp.variation_id = le.variation_id
    GROUP BY fp.variation_id;
    """
    engine = get_db_connection()
    df = pd.read_sql(text(query), engine)
    return df

def main(tag):
    df = get_ltv_report(tag)
    if df is not None:
        print("LTV 报告：")
        print(df)

if __name__ == "__main__":
    main("backend")
