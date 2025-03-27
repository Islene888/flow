import pymysql
from datetime import datetime, timedelta

# 数据库配置
host = '3.135.224.186'
port = 9030
user = 'flowgptdwj'
password = 'GgJ34Q1aGTO7'
database = 'flow_event_info'


# 生成2025年2月所有日期列表
def generate_date_range():
    start_date = datetime(2024, 7, 20)
    end_date = datetime(2024, 12, 1)  # 结束日期为3月1日（不包含）
    date_list = []

    current_date = start_date
    while current_date < end_date:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return date_list


# SQL模板
sql_template = """
SET SESSION query_timeout = 30000;
insert into flow_event_info.tbl_app_session_info
with order_time_lag as (
    select
        event_date,
        event_name,
        event_id,
        event_timestamp,
        user_id,
        lag(event_timestamp, 1) over(partition by user_id order by event_timestamp) as prev_event_ts
    from flowgpt.tbl_event_app
    where event_date = '{target_date}'
      and event_name not in ('_app_end','_app_exception','_app_start','_app_update','_clickstream_error','_first_open','_os_update','_profile_set','_screen_view','_session_start','_user_engagement')
      and user_id is not null and user_id != ''
),
session_group as (
    select
        event_date,
        event_name,
        event_id,
        event_timestamp,
        user_id,
        prev_event_ts,
        sum(case when prev_event_ts is not null and (event_timestamp - prev_event_ts) >= 1000*60*20 then 1 else 0 end)
            over(partition by user_id order by event_timestamp) as group_num
    from order_time_lag
),
session_info as (
    select
        event_date,
        user_id,
        group_num,
        min(event_timestamp) as start_timestamp,
        max(event_timestamp) as end_timestamp
    from session_group
    where event_name != 'app_background'
      and event_name != 'app_active'
    group by event_date, user_id, group_num
)
select
    concat(si.user_id, '_', si.start_timestamp, '_', si.end_timestamp) as id,
    si.user_id,
    si.group_num as session_sequence,
    from_unixtime(si.start_timestamp/1000) as start_time,
    ot1.event_name as start_event,
    from_unixtime(si.end_timestamp/1000) as end_time,
    ot2.event_name as end_event,
    si.end_timestamp - si.start_timestamp as duration,
    si.event_date
from session_info si
left join order_time_lag ot1 on si.user_id = ot1.user_id
                           and si.start_timestamp = ot1.event_timestamp
left join order_time_lag ot2 on si.user_id = ot2.user_id
                           and si.end_timestamp = ot2.event_timestamp
"""


def main():
    # 生成日期列表
    dates = generate_date_range()

    try:
        # 建立数据库连接
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with conn.cursor() as cursor:
            # 设置 session 超时时间
            cursor.execute("SET SESSION query_timeout = 30000;")
            print("[Info] 已设置 SESSION query_timeout = 30000")

            for date_str in dates:
                try:
                    # 替换SQL中的日期
                    formatted_sql = sql_template.format(target_date=date_str)

                    # 执行SQL
                    cursor.execute(formatted_sql)
                    conn.commit()  # 提交事务
                    print(f"[Success] {date_str} 数据插入完成")

                except Exception as e:
                    conn.rollback()  # 回滚事务
                    print(f"[Error] {date_str} 处理失败: {str(e)}")

    except pymysql.Error as e:
        print(f"数据库连接失败: {str(e)}")
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()


if __name__ == "__main__":
    main()