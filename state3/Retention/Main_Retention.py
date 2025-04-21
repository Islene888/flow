import time  # 如果后续不使用，可以移除

from state3.Retention import retention_report_table_ETL, active_retention_wide_table_ETL, \
    retention_report_table_active_ETL, First_Retention_overall, Active_Retention_overall, test_country
from state3.Retention.active_retention_wide_table_ETL import insert_experiment_data_to_wide_active_table
from state3.Retention.retention_wide_table_ETL import insert_experiment_data_to_wide_table
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag


def run_experiment_data_etl(tag):
    # 根据标签获取实验详细信息，并存储返回结果（如果需要）
    experiment_data = get_experiment_details_by_tag(tag)
    if experiment_data:
        print("成功获取实验详细信息。")
    else:
        print("未获取到实验详细信息。")

    # 将实验数据插入宽表
    insert_experiment_data_to_wide_table(tag)
    retention_report_table_ETL.main(tag)

    insert_experiment_data_to_wide_active_table(tag)
    retention_report_table_active_ETL.main(tag)

    Active_Retention_overall.main(tag)
    First_Retention_overall.main(tag)

    # test_country.main(tag)
def main(tag):
    run_experiment_data_etl(tag)


if __name__ == "__main__":
    tag = "trans_pt"
    main(tag)
