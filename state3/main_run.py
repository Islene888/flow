import warnings
from symbol import subscript

from urllib3.exceptions import NotOpenSSLWarning

from state3.Advertisement import advertisement
from state3.Business import Main_business
from state3.Engagement import Main_Engagement
from state3.Recharge import recharge
from state3.Retention import retention_report_table_ETL, Main_Retention
from state3.Retention.retention_wide_table_ETL import insert_experiment_data_to_wide_table
from state3.Subscribe import subscribe
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
from state3.growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# 1.获取并保存 GrowthBook 实验数据
fetch_and_save_experiment_data()
#
# tag = 'chat_0310'  # 定义实验标签
# # tag = 'web'  # 定义实验标签
# # 留存计算
# Main_Retention.main(tag)
#
# # 充值计算
# recharge.main(tag)
#
# # 广告计算
# advertisement.main(tag)
#
# # 订阅计算
# subscribe.main(tag)
#
# # 商业化相关的5个指标计算
# Main_business.main(tag)
#
# # Engagement 相关的9个指标计算
# Main_Engagement.main(tag)












