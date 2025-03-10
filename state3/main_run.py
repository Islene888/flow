import warnings
from urllib3.exceptions import NotOpenSSLWarning

from state2.Engagement.Events import Follow
from state3.Retention import retention_report_table_ETL
from state3.Retention.retention_wide_table_ETL import insert_experiment_data_to_wide_table
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# 1.获取并保存 GrowthBook 实验数据
# fetch_and_save_experiment_data()

tag = 'backend'  # 定义实验标签
# tag = 'web'  # 定义实验标签

# test.main(tag)
#根据标签获取实验详细信息
get_experiment_details_by_tag(tag)

# 将实验数据插入宽表
insert_experiment_data_to_wide_table(tag)

# 生成并保存留存报告
retention_report_table_ETL.main(tag)

# Engagement_report_table_ETL.main(tag)











