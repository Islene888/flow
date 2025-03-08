import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

def insert_experiment_data_to_wide_table(tag):
    try:
        # 获取实验的详细信息
        experiment_data = get_experiment_details_by_tag(tag)
        if not experiment_data:
            print(f"没有找到符合标签 '{tag}' 的实验数据！")
            return

        experiment_name = experiment_data['experiment_name']
        start_time = experiment_data['phase_start_time']
        end_time = experiment_data['phase_end_time']
        variations = experiment_data['number_of_variations']
        control_group_key = experiment_data['control_group_key']

        # 时间数据提取
        formatted_start_time = start_time.strftime('%Y-%m-%d')
        formatted_end_time = end_time.strftime('%Y-%m-%d')

        # 对密码进行 URL 编码
        password = urllib.parse.quote_plus("flowgpt@2024.com")

        # 构造数据库连接 URL
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"

        # 创建数据库连接
        engine = create_engine(DATABASE_URL)

        # 使用 f-string 动态构建表名
        table_name1 = f"tbl_wide_user_retention_{tag}"  # 生成表名
        table_name2 = f"tbl_report_user_retention_{tag}"  # 生成表名

        create_table_query1 = f"""
        CREATE TABLE IF NOT EXISTS {table_name1} (
            dt DATE,
            variation VARCHAR(255),
            new_users INT,
            d1 INT,
            d3 INT,
            d7 INT,
            d15 INT,
            total_assigned INT
        );
        """

        create_table_query2 = f"""
        CREATE TABLE IF NOT EXISTS {table_name2} (
            dt DATE,
            day INT,
            variation VARCHAR(255),
            users INT,
            retained INT,
            retention_rate DOUBLE,
            ci_lower DOUBLE,
            ci_upper DOUBLE,
            control_rate DOUBLE,
            exp_rate DOUBLE,
            uplift DOUBLE,
            uplift_ci_lower DOUBLE,
            uplift_ci_upper DOUBLE,
            z_score DOUBLE,
            p_value DOUBLE,
            retention_rate_baseline DOUBLE
        );
        """
        # 执行查询并创建表1
        try:
            with engine.connect() as conn:
                conn.execute(text(create_table_query1))
            print(f"✅ 宽表 {table_name1} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据库表格创建失败: {e}")

        # 执行查询并创建表2
        try:
            with engine.connect() as conn:
                conn.execute(text(create_table_query2))
            print(f"✅ 宽表 {table_name2} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据库表格创建失败: {e}")

        # 构建插入查询，通过 LEFT JOIN 子查询 ta 获取每个日期、variation 的 total_assigned
        insert_query = f"""            
            INSERT OVERWRITE {table_name1} (dt, variation, new_users, d1, d3, d7, d15, total_assigned)
SELECT
    /*+ SET_VAR (query_timeout = 30000) */ 
    u.first_visit_date AS dt, 
    e.variation, 
    COUNT(DISTINCT u.user_id) AS new_users,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 1 THEN a.user_id END) AS d1,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 3 THEN a.user_id END) AS d3,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 7 THEN a.user_id END) AS d7,
    COUNT(DISTINCT CASE WHEN DATEDIFF(a.active_date, u.first_visit_date) = 15 THEN a.user_id END) AS d15,
    MAX(COALESCE(ta.total_assigned, 0)) AS total_assigned
FROM (
    -- 严格新用户定义：筛选指定日期区间内首次访问的用户
    SELECT 
        user_id,
        DATE(first_visit_date) AS first_visit_date
    FROM flow_wide_info.tbl_wide_user_first_visit_app_info
    WHERE first_visit_date BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
) u
LEFT JOIN (
    -- 活跃用户：使用 tbl_wide_active_user_app_info 表，keep_alive_flag = 1 的数据
    SELECT
        d.user_id,
        d.active_date
    FROM flow_wide_info.tbl_wide_active_user_app_info d
    WHERE
        d.active_date BETWEEN '{start_time}' AND '{end_time}'
        AND d.keep_alive_flag = 1
        AND d.user_id IS NOT NULL
        AND d.user_id != ''
    GROUP BY d.active_date, d.user_id
) a ON u.user_id = a.user_id
LEFT JOIN (
    -- 实验分组信息：获取指定实验的分组信息
    SELECT
        user_id,
        CAST(variation_id AS CHAR) AS variation
    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
    WHERE
        experiment_id = '{experiment_name}'
        AND timestamp_assigned BETWEEN '{start_time}' AND '{end_time}'
) e ON u.user_id = e.user_id
LEFT JOIN (
    -- 统计每天、每个 variation 被分配的用户数量
    SELECT 
        DATE(timestamp_assigned) AS assign_date,
        CAST(variation_id AS CHAR) AS variation,
        COUNT(DISTINCT user_id) AS total_assigned
    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
    WHERE experiment_id = '{experiment_name}'
    GROUP BY DATE(timestamp_assigned), CAST(variation_id AS CHAR)
) ta ON ta.assign_date = u.first_visit_date AND ta.variation = e.variation
-- 排除未分组用户
WHERE e.variation IS NOT NULL
GROUP BY u.first_visit_date, e.variation
ORDER BY u.first_visit_date, e.variation;

        """

        # 执行查询并插入数据
        try:
            with engine.connect() as conn:
                conn.execute(text(insert_query))  # 直接执行一次插入
            print(f"✅ 宽表数据已成功写入 {table_name1} 中！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据插入失败: {e}")

    except Exception as e:
        print(f"🚨 执行失败: {e}")
