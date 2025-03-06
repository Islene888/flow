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

        # 时间数据提取（适配DATE类型）
        formatted_start_time = start_time.strftime('%Y-%m-%d')  # 确保纯日期格式
        formatted_end_time = end_time.strftime('%Y-%m-%d')

        # 对密码进行 URL 编码
        password = urllib.parse.quote_plus("flowgpt@2024.com")

        # 构造数据库连接 URL
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"

        # 创建数据库连接
        engine = create_engine(DATABASE_URL)

        # 使用 f-string 动态构建表名
        table_name1 = f"tbl_wide_user_retention_{tag}"
        table_name2 = f"tbl_report_user_retention_{tag}"

        # 修复点1：完整的Doris表定义
        create_table_query1 = f"""
        CREATE TABLE IF NOT EXISTS {table_name1} (
            dt DATE,
            variation VARCHAR(255),
            users INT,
            d1 INT,
            d2 INT,
            d3 INT,
            d4 INT,
            d5 INT,
            d6 INT,
            d7 INT,
            d8 INT,
            d9 INT,
            d10 INT,
            d11 INT,
            d12 INT,
            d13 INT,
            d14 INT,
            d15 INT,
            d16 INT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, variation)
        DISTRIBUTED BY HASH(dt) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
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
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, day, variation)
        DISTRIBUTED BY HASH(dt) BUCKETS 10
        PROPERTIES (
            "replication_num" = "3"
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

        # 修复点2：改用INSERT OVERWRITE并拆分SET语句
        insert_query = f"""
            INSERT OVERWRITE {table_name1}  -- Doris支持的覆盖写入语法
            SELECT 
                u.first_visit_date AS dt, 
                e.variation, 
                COUNT(DISTINCT u.user_id) AS users,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 1 DAY) THEN a.user_id END) AS d1,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 2 DAY) THEN a.user_id END) AS d2,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 3 DAY) THEN a.user_id END) AS d3,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 4 DAY) THEN a.user_id END) AS d4,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 5 DAY) THEN a.user_id END) AS d5,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 6 DAY) THEN a.user_id END) AS d6,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 7 DAY) THEN a.user_id END) AS d7,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 8 DAY) THEN a.user_id END) AS d8,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 9 DAY) THEN a.user_id END) AS d9,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 10 DAY) THEN a.user_id END) AS d10,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 11 DAY) THEN a.user_id END) AS d11,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 12 DAY) THEN a.user_id END) AS d12,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 13 DAY) THEN a.user_id END) AS d13,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 14 DAY) THEN a.user_id END) AS d14,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 15 DAY) THEN a.user_id END) AS d15,
                COUNT(DISTINCT CASE WHEN a.active_date >= DATE_ADD(u.first_visit_date, INTERVAL 16 DAY) THEN a.user_id END) AS d16
            FROM (
                -- 严格新用户定义
                SELECT 
                    user_id,
                    DATE(first_visit_date) AS first_visit_date
                FROM
                    flow_wide_info.tbl_wide_user_first_visit_app_info
                WHERE
                    first_visit_date BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
            ) u
            LEFT JOIN (
                -- 用户活跃行为（满足会话时长条件）
                SELECT
                    e.user_id,
                    DATE(FROM_UNIXTIME(e.ingest_timestamp / 1000, '%%Y-%%m-%%d')) AS active_date
                FROM
                    flowgpt.tbl_event_app e
                INNER JOIN flowgpt.tbl_parameter_app p 
                    ON p.event_id = e.event_id
                    AND p.event_name = '_app_start'
                    AND p.event_param_key = '_session_duration'
                WHERE
                    e.event_name = '_app_start'
                    AND e.user_id IS NOT NULL
                    AND e.user_id != ''
                    AND p.event_param_value >= 10000
                    AND DATE(FROM_UNIXTIME(e.ingest_timestamp / 1000)) BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
            ) a ON u.user_id = a.user_id
            LEFT JOIN (
                -- 动态获取实验分组
                SELECT
                    user_id,
                    CAST(variation_id AS CHAR) AS variation
                FROM
                    flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE
                    experiment_id = '{experiment_name}'
                    AND timestamp_assigned BETWEEN '{start_time}' AND '{end_time}'
            ) e ON u.user_id = e.user_id
            -- 排除未分组用户
            WHERE e.variation IS NOT NULL
            GROUP BY
                u.first_visit_date, 
                e.variation
            ORDER BY 
                u.first_visit_date,
                e.variation;
        """

        # 执行插入查询（分步执行SET和INSERT）
        try:
            with engine.connect() as conn:
                # 修复点3：拆分SET语句和主查询
                conn.execute(text("SET query_timeout = 30000;"))  # 单独执行SET
                conn.execute(text(insert_query))  # 执行主插入
            print(f"✅ 宽表数据已成功写入 {table_name1} 中！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据插入失败: {e}")

    except Exception as e:
        print(f"🚨 执行失败: {e}")