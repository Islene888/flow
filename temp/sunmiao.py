from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import warnings
import urllib.parse
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)



def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_wide_info?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    return engine


def get_experiment_users_by_date(event_date: str):
    engine = get_db_connection()
    query = f"""
        SELECT DISTINCT user_id, variation_id, timestamp_assigned,
        '{event_date}' as event_date
        FROM flow_wide_info.tbl_wide_experiment_assignment_hi hi
        WHERE hi.experiment_id = 'chat-skip-translation-th-new'
          AND hi.event_date = '{event_date}'
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql(text(query), connection)
            return df
    except SQLAlchemyError as e:
        print(f"❌ {event_date} 查询失败：", e)
        return pd.DataFrame()


def get_users_over_date_range(start_date: str, end_date: str):
    all_data = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"📅 查询日期：{date_str}")
        df = get_experiment_users_by_date(date_str)
        if not df.empty:
            all_data.append(df)
        current_date += timedelta(days=1)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    start = "2025-04-11"
    today = datetime.today().strftime("%Y-%m-%d")
    result_df = get_users_over_date_range(start, today)

    print(f"\n✅ 共找到 {len(result_df)} 条记录，{result_df['user_id'].nunique()} 个去重 user_id")
    print(result_df.head())

    output_path = "chat_skip_translation_users.csv"
    result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 结果已保存到 {output_path}")
