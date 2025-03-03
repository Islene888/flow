import requests
import warnings
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
from sqlalchemy.exc import SQLAlchemyError

# 忽略 urllib3 的 SSL 警告
warnings.filterwarnings("ignore", category=UserWarning, module='urllib3')

# 配置 GrowthBook API
GROWTHBOOK_API_URL = "https://api.growthbook.io/api/v1/experiments"
GROWTHBOOK_API_KEY = "secret_user_co34d1yJbEzlafF7ZAtrLYsd38u9oe6FLVYAWVEUhFY"

# 设置 Authorization 头部进行 Bearer 身份验证
headers = {
    "Authorization": f"Bearer {GROWTHBOOK_API_KEY}",
}

# 发送请求并处理实验数据，支持分页获取所有实验
def fetch_and_save_experiments():
    limit = 100  # 设置每次请求的实验数量
    offset = 0  # 从第一个实验开始
    all_experiments = []

    while True:
        params = {
            'limit': limit,
            'offset': offset,
        }

        response = requests.get(GROWTHBOOK_API_URL, headers=headers, params=params)

        if response.status_code == 200:
            experiments = response.json().get("experiments", [])
            all_experiments.extend(experiments)

            # 如果返回的数据少于请求的 limit，说明已经获取完所有数据
            if len(experiments) < limit:
                break
            else:
                offset += limit  # 增加 offset，获取下一页的数据
        else:
            print("请求失败:", response.status_code, response.text)
            break

    # 按 date_created 排序并获取最新3次的实验
    sorted_experiments = sorted(all_experiments, key=lambda x: datetime.strptime(x.get('dateCreated'), '%Y-%m-%dT%H:%M:%S.%fZ'), reverse=True)

    # 只取最近3个实验
    latest_three_experiments = sorted_experiments[:3]

    # 创建实验数据的列表
    experiment_data = []
    for experiment in latest_three_experiments:
        experiment_id = experiment.get('id')
        experiment_name = experiment.get('name')
        project = experiment.get('project')
        hypothesis = experiment.get('hypothesis')
        description = experiment.get('description')
        tags = ", ".join(experiment.get('tags', []))
        owner = experiment.get('owner')
        date_created = experiment.get('dateCreated')
        date_updated = experiment.get('dateUpdated')
        archived = experiment.get('archived')
        status = experiment.get('status')
        auto_refresh = experiment.get('autoRefresh')
        hash_attribute = experiment.get('hashAttribute')
        hash_version = experiment.get('hashVersion')

        variations = experiment.get('variation', [])
        num_variations = len(variations)

        # 获取对照组的key值
        control_group_key = variations[0].get('key') if variations else None

        # 获取阶段信息
        phases = experiment.get('phases', [])
        phases_info = "; ".join(
            [f"{phase.get('name')} ({phase.get('dateStarted')} - {phase.get('dateEnded')})" for phase in phases])

        result_summary = experiment.get('resultSummary', {})
        result_status = result_summary.get('status')
        winner = result_summary.get('winner')

        # 提取实验相关信息
        experiment_info = {
            'experiment_id': experiment_id,
            'experiment_name': experiment_name,
            'project': project,
            'hypothesis': hypothesis,
            'description': description,
            'tags': tags,
            'owner': owner,
            'date_created': date_created,
            'date_updated': date_updated,
            'archived': archived,
            'status': status,
            'auto_refresh': auto_refresh,
            'hash_attribute': hash_attribute,
            'hash_version': hash_version,
            'variations': num_variations,
            'control_group_key': control_group_key,  # 返回的是 key
            'phases_info': phases_info,
            'result_status': result_status,
            'winner': winner
        }
        experiment_data.append(experiment_info)

    # 转换为 DataFrame
    experiment_df = pd.DataFrame(experiment_data)

    # ✅ 6. 连接到数据库并插入数据
    try:
        # ✅ 1. 连接数据库
        password = urllib.parse.quote_plus("flowgpt@2024.com")
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@18.188.196.105:9030/flow_test"
        engine = create_engine(DATABASE_URL)

        # ✅ 2. 创建表（如果表不存在）
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS tbl_experiment_data (
                experiment_id VARCHAR(255) NOT NULL,
                experiment_name VARCHAR(255) NOT NULL,
                project VARCHAR(255),
                hypothesis TEXT,
                description TEXT,
                tags VARCHAR(255),
                owner VARCHAR(255),
                date_created DATETIME NOT NULL,
                date_updated DATETIME NOT NULL,
                archived BOOLEAN NOT NULL,
                status VARCHAR(50) NOT NULL,
                auto_refresh BOOLEAN NOT NULL,
                hash_attribute VARCHAR(50),
                hash_version INT NOT NULL,
                variation INT NOT NULL,
                control_group_key VARCHAR(50) NOT NULL,
                phases_info TEXT,
                result_status VARCHAR(50),
                winner VARCHAR(255)
            ) ENGINE=OLAP;
        """

        # 使用 connection 来执行创建表语句
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))  # 使用 text() 将 SQL 包裹起来
        print("✅ 表格创建成功！")

        # ✅ 3. 将 DataFrame 插入到 tbl_experiment_data 表
        experiment_df.to_sql('tbl_experiment_data', con=engine, if_exists='append', index=False, method='multi')
        print("✅ 实验数据已成功保存到数据库！")
    except SQLAlchemyError as e:
        print(f"Error inserting data: {e}")
    finally:
        engine.dispose()


# 调用函数
fetch_and_save_experiments()
