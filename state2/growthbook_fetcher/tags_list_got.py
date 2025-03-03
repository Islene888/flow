import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

def get_all_tags_from_db():
    """
    从数仓中获取所有实验数据中的 tags，并返回唯一的标签列表
    """
    try:
        # 数据库用户名、密码和主机
        db_user = "bigdata"
        db_password = "flowgpt@2024.com"
        db_host = "18.188.196.105"
        db_port = "9030"
        db_name = "flow_ab_test"

        # 使用 urllib 对密码进行编码
        encoded_password = urllib.parse.quote_plus(db_password)

        # 正确格式化数据库 URL
        db_url = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"

        # 创建数据库引擎
        engine = create_engine(db_url)

        # 查询数据
        query = "SELECT tags,experiment_name FROM tbl_experiment_data"
        with engine.connect() as connection:
            result = connection.execute(text(query))  # 执行查询

            # 将查询结果转换为 DataFrame，方便操作
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        unique_tuples = set()
        for index, row in df.iterrows():
            tag = row['tags'].strip()
            experiment = row['experiment_name']
            unique_tuples.add((tag, experiment))

        # 从唯一的 (tag, experiment) 元组中提取 tag，再去重
        unique_tags = set()
        for tag, experiment in unique_tuples:
            unique_tags.add(tag)
        return list(unique_tags)

    except Exception as e:
        print(f"Error retrieving tags from database: {e}")
        return []
    finally:
        engine.dispose()
