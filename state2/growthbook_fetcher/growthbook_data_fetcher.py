import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import urllib.parse

def fetch_and_save_experiment_data():
    # 设置 GrowthBook API URL 和 Bearer Token
    GROWTHBOOK_API_URL = "https://api.growthbook.io/api/v1/experiments"
    GROWTHBOOK_API_KEY = "secret_user_co34d1yJbEzlafF7ZAtrLYsd38u9oe6FLVYAWVEUhFY"

    # 设置 Authorization 头部进行 Bearer 身份验证
    headers = {
        "Authorization": f"Bearer {GROWTHBOOK_API_KEY}",
    }

    # 设置参数，返回多个实验
    params = {
        'limit': 100,  # 设置返回100个实验
        'offset': 0,
    }

    # 发送请求
    response = requests.get(GROWTHBOOK_API_URL, headers=headers, params=params)

    # 如果响应成功（状态码 200），解析并打印响应的 JSON 数据
    if response.status_code == 200:
        print("响应成功，返回的 JSON 数据如下：")
        try:
            response_json = response.json()  # 解析 JSON
            experiments = response_json.get('experiments', [])

            # 创建一个空列表用于保存每个实验的信息
            experiments_data = []

            if experiments:
                # 按 tag 分组实验
                tag_dict = {}
                for experiment in experiments:
                    tags = experiment.get('tags', [])
                    for tag in tags:
                        if tag not in tag_dict:
                            tag_dict[tag] = []
                        tag_dict[tag].append(experiment)

                # 获取每个 tag 对应的最后一个实验（根据最后阶段的开始时间排序）
                for tag, experiments_with_tag in tag_dict.items():
                    # 按照最后阶段的开始时间排序实验，确保取到最近开始的实验
                    experiments_with_tag.sort(key=lambda x: get_last_phase_start_time(x), reverse=True)
                    last_experiment = experiments_with_tag[0]  # 获取排序后的最新实验
                    experiment_name = last_experiment.get('name')  # 获取实验名称
                    tags = last_experiment.get('tags', [])  # 获取实验标签
                    tags_str = ', '.join(tags)  # 将 tags 列表转换为字符串
                    variations = last_experiment.get('variations', [])  # 获取变体信息
                    num_variations = len(variations)  # 变体个数
                    control_group_key = variations[0].get('key') if variations else None  # 获取对照组（key）

                    # 获取最后一个阶段的时间
                    phases = last_experiment.get('phases', [])
                    if phases:
                        last_phase = phases[-1]  # 获取最后一个阶段
                        start_time = datetime.strptime(last_phase.get('dateStarted'), '%Y-%m-%dT%H:%M:%S.%fZ')  # 阶段开始时间
                        end_time_str = last_phase.get('dateEnded')

                        if end_time_str:
                            # 如果结束时间存在，则使用结束时间
                            end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                        else:
                            # 如果没有结束时间，则使用当前时间
                            end_time = datetime.now()

                        # 计算实验持续时间（天数）
                        duration = (end_time - start_time).days

                        # 如果实验持续时间大于 2 个月（约 60 天），则跳过该实验
                        if duration > 60:
                            print(f"实验 {experiment_name} 持续时间超过 2 个月，跳过该实验。")
                            continue

                        # 将数据添加到列表中
                        experiments_data.append({
                            "experiment_name": experiment_name,
                            "tags": tags_str,  # 使用转换后的 tags 字符串
                            "phase_start_time": start_time,
                            "phase_end_time": end_time,
                            "number_of_variations": num_variations,
                            "control_group_key": control_group_key
                        })
                    else:
                        print(f"No phases available for experiment: {experiment_name}")

                # 将实验数据封装到 DataFrame 中
                experiment_df = pd.DataFrame(experiments_data)

                # 连接到数据库并插入数据
                try:
                    # 对密码进行 URL 编码
                    db_password = "flowgpt@2024.com"
                    encoded_password = urllib.parse.quote_plus(db_password)

                    # 构造数据库连接 URL
                    DATABASE_URL = f"mysql+pymysql://bigdata:{encoded_password}@18.188.196.105:9030/flow_test"

                    # 创建数据库连接
                    engine = create_engine(DATABASE_URL)

                    # 创建表（如果表不存在）
                    create_table_sql = """
                        CREATE TABLE IF NOT EXISTS tbl_experiment_data (
                            experiment_name VARCHAR(255) NOT NULL,
                            tags VARCHAR(255),
                            phase_start_time DATETIME NOT NULL,
                            phase_end_time DATETIME NOT NULL,
                            number_of_variations INT NOT NULL,
                            control_group_key VARCHAR(50) NOT NULL
                        ) ENGINE=OLAP;
                    """

                    # 使用 connection 来执行创建表语句
                    with engine.connect() as connection:
                        connection.execute(text(create_table_sql))  # 使用 text() 将 SQL 包裹起来
                    print("✅ 表格创建成功！")

                    # 将 DataFrame 插入到 tbl_experiment_data 表
                    experiment_df.to_sql('tbl_experiment_data', con=engine, if_exists='append', index=False, method='multi')
                    print("✅ 实验数据已成功保存到数据库！")
                except SQLAlchemyError as e:
                    print(f"Error inserting data: {e}")
                finally:
                    engine.dispose()

            else:
                print("No experiments found in the response.")
        except ValueError as e:
            print("响应内容不是有效的 JSON 格式:", e)
    else:
        print(f"请求失败，状态码: {response.status_code}, 错误信息: {response.text}")


def get_last_phase_start_time(experiment):
    """
    获取实验最后一个阶段的开始时间，如果没有开始时间，则返回当前时间。
    """
    phases = experiment.get('phases', [])
    if phases:
        last_phase = phases[-1]  # 获取最后一个阶段
        start_time_str = last_phase.get('dateStarted')
        if start_time_str:
            # 如果开始时间存在，则返回开始时间
            return datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    # 如果没有开始时间，返回当前时间
    return datetime.now()
