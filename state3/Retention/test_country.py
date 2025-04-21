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

        # 时间格式化
        formatted_start_time = start_time.strftime('%Y-%m-%d')
        formatted_end_time = end_time.strftime('%Y-%m-%d')

        # 数据库连接
        password = urllib.parse.quote_plus("flowgpt@2024.com")
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)

        table_name = f"tbl_wide_user_retention_active_{tag}"

        # 创建宽表（增加 country 字段）
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dt DATE,
            variation VARCHAR(255),
            country VARCHAR(64),
            new_users INT,
            d1 INT,
            d3 INT,
            d7 INT,
            d15 INT,
            total_assigned INT
        );
        """

        # 创建表
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
        print(f"✅ 宽表 {table_name} 已成功创建！")

        # 清空历史数据
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        print(f"✅ 表 {table_name} 已成功清空原有数据！")

        # 分批插入
        batch_count = 100
        for i in range(batch_count):
            insert_query = f"""
            INSERT INTO {table_name} (dt, variation, country, new_users, d1, d3, d7, d15, total_assigned)
            SELECT
            /*+ SET_VAR(query_timeout = 60000) */
                base.active_date AS dt,
                e.variation,
                e.country,
                COUNT(DISTINCT base.user_id) AS new_users,
                COUNT(DISTINCT CASE WHEN d1.user_id IS NOT NULL THEN base.user_id END) AS d1,
                COUNT(DISTINCT CASE WHEN d3.user_id IS NOT NULL THEN base.user_id END) AS d3,
                COUNT(DISTINCT CASE WHEN d7.user_id IS NOT NULL THEN base.user_id END) AS d7,
                COUNT(DISTINCT CASE WHEN d15.user_id IS NOT NULL THEN base.user_id END) AS d15,
                MAX(COALESCE(ta.total_assigned, 0)) AS total_assigned
            FROM (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
                  AND keep_alive_flag = 1
                  AND user_id IS NOT NULL AND user_id != ''
                  AND MOD(CRC32(user_id), {batch_count}) = {i}
                GROUP BY user_id, active_date
            ) base
            LEFT JOIN (
                SELECT t.user_id, t.variation, geo.country
                FROM (
                    SELECT user_id, CAST(variation_id AS CHAR) AS variation,
                           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned ASC) AS rn
                    FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                    WHERE experiment_id = '{experiment_name}'
                      AND timestamp_assigned BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'
                ) t
                LEFT JOIN (
                    SELECT user_id,
                           MAX(get_json_string(geo, '$.country')) AS country
                    FROM flowgpt.tbl_event_app
                    WHERE user_id IS NOT NULL AND user_id != ''
                    GROUP BY user_id
                ) geo ON t.user_id = geo.user_id
                WHERE rn = 1
            ) e ON base.user_id = e.user_id
            LEFT JOIN (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date BETWEEN DATE_ADD('{formatted_start_time}', INTERVAL 1 DAY)
                                        AND DATE_ADD('{formatted_end_time}', INTERVAL 15 DAY)
                  AND keep_alive_flag = 1
                GROUP BY user_id, active_date
            ) d1 ON base.user_id = d1.user_id AND DATEDIFF(d1.active_date, base.active_date) = 1
            LEFT JOIN (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date BETWEEN DATE_ADD('{formatted_start_time}', INTERVAL 3 DAY)
                                        AND DATE_ADD('{formatted_end_time}', INTERVAL 15 DAY)
                  AND keep_alive_flag = 1
                GROUP BY user_id, active_date
            ) d3 ON base.user_id = d3.user_id AND DATEDIFF(d3.active_date, base.active_date) = 3
            LEFT JOIN (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date BETWEEN DATE_ADD('{formatted_start_time}', INTERVAL 7 DAY)
                                        AND DATE_ADD('{formatted_end_time}', INTERVAL 15 DAY)
                  AND keep_alive_flag = 1
                GROUP BY user_id, active_date
            ) d7 ON base.user_id = d7.user_id AND DATEDIFF(d7.active_date, base.active_date) = 7
            LEFT JOIN (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date BETWEEN DATE_ADD('{formatted_start_time}', INTERVAL 15 DAY)
                                        AND DATE_ADD('{formatted_end_time}', INTERVAL 15 DAY)
                  AND keep_alive_flag = 1
                GROUP BY user_id, active_date
            ) d15 ON base.user_id = d15.user_id AND DATEDIFF(d15.active_date, base.active_date) = 15
            LEFT JOIN (
                SELECT DATE(timestamp_assigned) AS assign_date,
                       CAST(variation_id AS CHAR) AS variation,
                       COUNT(DISTINCT user_id) AS total_assigned
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_name}'
                GROUP BY DATE(timestamp_assigned), CAST(variation_id AS CHAR)
            ) ta ON ta.assign_date = base.active_date AND ta.variation = e.variation
            WHERE e.variation IS NOT NULL AND e.country IS NOT NULL
            GROUP BY base.active_date, e.variation, e.country
            ORDER BY base.active_date, e.variation, e.country;
            """

            try:
                with engine.connect() as conn:
                    conn.execute(text(insert_query))
                print(f"✅ 分批 {i + 1}/{batch_count} 数据已成功写入 {table_name}！")
            except SQLAlchemyError as e:
                print(f"🚨 分批 {i + 1}/{batch_count} 数据插入失败: {e}")

        # 聚合汇总
        merge_query = f"""
        SELECT
            dt,
            variation,
            country,
            SUM(new_users) AS new_users,
            SUM(d1) AS d1,
            SUM(d3) AS d3,
            SUM(d7) AS d7,
            SUM(d15) AS d15,
            MAX(total_assigned) AS total_assigned
        FROM {table_name}
        GROUP BY dt, variation, country;
        """

        aggregated_data = []
        try:
            with engine.connect() as conn:
                result = conn.execute(text(merge_query))
                aggregated_data = result.mappings().all()
            print("✅ 数据聚合成功！")
        except SQLAlchemyError as e:
            print(f"🚨 数据聚合失败: {e}")

        # 清空原始分批数据，插入聚合数据
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            print(f"✅ 表 {table_name} 已成功清空，准备写入聚合后的数据！")
        except SQLAlchemyError as e:
            print(f"🚨 清空数据失败: {e}")

        insert_row_query = f"""
        INSERT INTO {table_name} (dt, variation, country, new_users, d1, d3, d7, d15, total_assigned)
        VALUES (:dt, :variation, :country, :new_users, :d1, :d3, :d7, :d15, :total_assigned);
        """

        try:
            with engine.begin() as conn:
                conn.execute(text(insert_row_query), aggregated_data)
            print(f"✅ 聚合数据成功插入 {table_name}！")
        except SQLAlchemyError as e:
            print(f"🚨 聚合数据插入失败: {e}")

    except Exception as e:
        print(f"🚨 执行失败: {e}")


if __name__ == "__main__":
    tag = "trans_pt"  # 根据实际标签修改
    insert_experiment_data_to_wide_active_table(tag)