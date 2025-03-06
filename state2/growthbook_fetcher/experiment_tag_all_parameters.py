from sqlalchemy import create_engine, text
import urllib.parse

def get_experiment_details_by_tag(tag):
    try:
        # 连接数据库
        password = urllib.parse.quote_plus("flowgpt@2024.com")
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)

        # 创建 SQL 查询语句并使用 text() 包裹，获取额外字段
        query = text("""
            SELECT experiment_name, phase_start_time, phase_end_time, 
                   number_of_variations, control_group_key
            FROM tbl_experiment_data 
            WHERE tags = :tag
        """)

        # 执行查询，传递参数
        with engine.connect() as connection:
            result = connection.execute(query, {'tag': tag})

            # 获取查询结果（一个实验）
            experiment = result.mappings().fetchone()  # 使用 fetchone() 获取一行数据

            if experiment:
                # 将查询结果封装为字典并返回
                experiment_data = {
                    "experiment_name": experiment['experiment_name'],
                    "phase_start_time": experiment['phase_start_time'],
                    "phase_end_time": experiment['phase_end_time'],
                    "number_of_variations": experiment['number_of_variations'],
                    "control_group_key": experiment['control_group_key']
                }
                return experiment_data  # 返回实验的详细数据字典
            else:
                print(f"没有找到符合标签 '{tag}' 的实验。")
                return None

    except Exception as e:
        print(f"查询失败: {e}")
        return None

    finally:
        engine.dispose()
