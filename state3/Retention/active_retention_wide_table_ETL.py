import urllib.parse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

def insert_experiment_data_to_wide_active_table(tag):
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

        # 时间数据格式化
        formatted_start_time = start_time.strftime('%Y-%m-%d')
        formatted_end_time = end_time.strftime('%Y-%m-%d')

        # 对密码进行 URL 编码
        password = urllib.parse.quote_plus("flowgpt@2024.com")

        # 构造数据库连接 URL
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"

        # 创建数据库连接
        engine = create_engine(DATABASE_URL)

        # 动态构建表名（原表，用于分批数据插入及后续聚合覆盖）
        table_name = f"tbl_wide_user_retention_active_{tag}"  # 宽表表名
        report_table_name = f"tbl_report_user_retention_active_{tag}"  # 报告表表名

        # 创建宽表和报告表（如果不存在）
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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

        create_report_table_query = f"""
        CREATE TABLE IF NOT EXISTS {report_table_name} (
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
        # 创建宽表
        try:
            with engine.connect() as conn:
                conn.execute(text(create_table_query))
            print(f"✅ 宽表 {table_name} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 宽表数据库表格创建失败: {e}")

        # 创建报告表
        try:
            with engine.connect() as conn:
                conn.execute(text(create_report_table_query))
            print(f"✅ 报告表 {report_table_name} 已成功创建！")
        except SQLAlchemyError as e:
            print(f"🚨 报告表数据库表格创建失败: {e}")

        # 清空宽表中原有数据（分批数据）
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 表 {table_name} 已成功清空原有数据！")
        except SQLAlchemyError as e:
            print(f"🚨 清空数据失败: {e}")

        # 使用 CRC32 函数对 user_id 转数字，利用 MOD 方法分批执行插入
        batch_count = 20  # 可根据数据量调整分批数
        for i in range(batch_count):
            insert_query = f"""            
                INSERT INTO {table_name} (dt, variation, new_users, d1, d3, d7, d15, total_assigned)
                SELECT
                    base.active_date AS dt,
                    e.variation,
                    COUNT(DISTINCT base.user_id) AS new_users,
                    COUNT(DISTINCT d1.user_id) AS d1,
                    COUNT(DISTINCT d3.user_id) AS d3,
                    COUNT(DISTINCT d7.user_id) AS d7,
                    COUNT(DISTINCT d15.user_id) AS d15,
                    MAX(COALESCE(ta.total_assigned, 0)) AS total_assigned
                FROM (
                    SELECT user_id, active_date
                    FROM flow_wide_info.tbl_wide_active_user_app_info
                    WHERE active_date BETWEEN '{start_time}' AND '{end_time}'
                      AND keep_alive_flag = 1
                      AND user_id IS NOT NULL AND user_id != ''
                      AND MOD(CRC32(user_id), {batch_count}) = {i}
                    GROUP BY user_id, active_date
                ) base
                LEFT JOIN (
                    -- 保留每个 user_id 的唯一实验分配记录（最早时间）
                    SELECT user_id, variation
                    FROM (
                        SELECT 
                            user_id, 
                            CAST(variation_id AS CHAR) AS variation,
                            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                        FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                        WHERE experiment_id = '{experiment_name}'
                          AND timestamp_assigned BETWEEN '{start_time}' AND '{end_time}'
                    ) t WHERE rn = 1
                ) e ON base.user_id = e.user_id
                LEFT JOIN (
                    -- d1 留存行为
                    SELECT user_id, active_date
                    FROM flow_wide_info.tbl_wide_active_user_app_info
                    WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 1 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
                      AND keep_alive_flag = 1
                    GROUP BY user_id, active_date
                ) d1 ON base.user_id = d1.user_id AND DATEDIFF(d1.active_date, base.active_date) = 1
                LEFT JOIN (
                    SELECT user_id, active_date
                    FROM flow_wide_info.tbl_wide_active_user_app_info
                    WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 3 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
                      AND keep_alive_flag = 1
                    GROUP BY user_id, active_date
                ) d3 ON base.user_id = d3.user_id AND DATEDIFF(d3.active_date, base.active_date) = 3
                LEFT JOIN (
                    SELECT user_id, active_date
                    FROM flow_wide_info.tbl_wide_active_user_app_info
                    WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 7 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
                      AND keep_alive_flag = 1
                    GROUP BY user_id, active_date
                ) d7 ON base.user_id = d7.user_id AND DATEDIFF(d7.active_date, base.active_date) = 7
                LEFT JOIN (
                    SELECT user_id, active_date
                    FROM flow_wide_info.tbl_wide_active_user_app_info
                    WHERE active_date BETWEEN DATE_ADD('{start_time}', INTERVAL 15 DAY) AND DATE_ADD('{end_time}', INTERVAL 15 DAY)
                      AND keep_alive_flag = 1
                    GROUP BY user_id, active_date
                ) d15 ON base.user_id = d15.user_id AND DATEDIFF(d15.active_date, base.active_date) = 15
                LEFT JOIN (
                    SELECT 
                        DATE(timestamp_assigned) AS assign_date,
                        CAST(variation_id AS CHAR) AS variation,
                        COUNT(DISTINCT user_id) AS total_assigned
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                    GROUP BY DATE(timestamp_assigned), CAST(variation_id AS CHAR)
                ) ta ON ta.assign_date = base.active_date AND ta.variation = e.variation
                WHERE e.variation IS NOT NULL
                GROUP BY base.active_date, e.variation
                ORDER BY base.active_date, e.variation;
            """

            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_query))
                print(f"✅ 分批 {i+1}/{batch_count} 数据已成功写入 {table_name} 中！")
            except SQLAlchemyError as e:
                print(f"🚨 分批 {i+1}/{batch_count} 数据插入失败: {e}")

        # 所有批次数据插入完毕后，进行数据聚合
        merge_query = f"""
        SELECT
            dt,
            variation,
            SUM(new_users) AS new_users,
            SUM(d1) AS d1,
            SUM(d3) AS d3,
            SUM(d7) AS d7,
            SUM(d15) AS d15,
            MAX(total_assigned) AS total_assigned
        FROM {table_name}
        GROUP BY dt, variation;
        """
        aggregated_data = []
        try:
            with engine.connect() as conn:
                result = conn.execute(text(merge_query))
                # 使用 .mappings() 获取字典格式结果（需 SQLAlchemy 1.4+）
                aggregated_data = result.mappings().all()
            print("✅ 数据聚合成功！")
        except SQLAlchemyError as e:
            print(f"🚨 数据聚合失败: {e}")

        # 清空原表中的分批数据（覆盖）
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 表 {table_name} 已成功清空，准备写入聚合后的数据！")
        except SQLAlchemyError as e:
            print(f"🚨 清空数据失败: {e}")

        # 将聚合后的数据重新插入原表中
        for row in aggregated_data:
            insert_row_query = f"""
            INSERT INTO {table_name} (dt, variation, new_users, d1, d3, d7, d15, total_assigned)
            VALUES (:dt, :variation, :new_users, :d1, :d3, :d7, :d15, :total_assigned);
            """
            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_row_query), {
                        'dt': row['dt'],
                        'variation': row['variation'],
                        'new_users': row['new_users'],
                        'd1': row['d1'],
                        'd3': row['d3'],
                        'd7': row['d7'],
                        'd15': row['d15'],
                        'total_assigned': row['total_assigned']
                    })
                print(f"✅ 聚合数据插入 {row['dt']} - {row['variation']} 成功！")
            except SQLAlchemyError as e:
                print(f"🚨 聚合数据插入失败: {e}")

    except Exception as e:
        print(f"🚨 执行失败: {e}")

# 如果需要运行，可调用函数，例如：
if __name__ == "__main__":
    tag = "trans_pt"  # 根据实际标签修改
    insert_experiment_data_to_wide_active_table(tag)