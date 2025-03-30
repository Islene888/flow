import warnings
from urllib3.exceptions import NotOpenSSLWarning
warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)
from state2.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
from state2.retention import retention_report_table_ETL
from state2.retention.retention_wide_table_ETL import insert_experiment_data_to_wide_table
from state2.growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data
from state2.growthbook_fetcher.experiment_all_tags import get_all_tags_from_db

# 1. 获取并保存 GrowthBook 实验数据
fetch_and_save_experiment_data()

# 2. 获取所有标签
tags = get_all_tags_from_db()
#
# # 3. 遍历所有标签并执行后续操作
# for tag in tags:
#     print(f"Processing tag: {tag}")
#
#     # 根据标签获取实验详细信息
#     get_experiment_details_by_tag(tag)
#
#     # 将实验数据插入宽表
#     insert_experiment_data_to_wide_table(tag)
#
#     # 生成并保存留存报告
#     retention_report_table_ETL.main(tag)
