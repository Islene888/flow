阶段二：


控制台返回结果：
响应成功，返回的 JSON 数据如下：
实验 app_rs_special_bot_exp 的开始时间不足2天，跳过该实验。
实验 web-v2 持续时间超过 3 个月，跳过该实验。
实验 gpt-3.5-turbo-llm-router-exp-phase-2 持续时间超过 3 个月，跳过该实验。
实验 app_rs_special_bot_exp 的开始时间不足2天，跳过该实验。
实验 gbdemo-checkout-layout 持续时间超过 3 个月，跳过该实验。
✅ 实验数据表格experiment_data 创建成功！
✅ 实验数据已成功保存到experiment_data中！
Processing tag: tag_test
✅ 宽表 tbl_wide_user_retention_tag_test 已成功创建！
✅ 宽表 tbl_report_user_retention_tag_test 已成功创建！
✅ 宽表数据已成功写入 tbl_wide_user_retention_tag_test 中！
✅ report表 tbl_report_user_retention_tag_test 已成功创建！
✅ 数据从表 'tbl_wide_user_retention_tag_test' 成功提取！
✅ report表数据已成功写入 tbl_report_user_retention_tag_test 中！
Processing tag: mobie
✅ 宽表 tbl_wide_user_retention_mobie 已成功创建！
✅ 宽表 tbl_report_user_retention_mobie 已成功创建！
✅ 宽表数据已成功写入 tbl_wide_user_retention_mobie 中！
✅ report表 tbl_report_user_retention_mobie 已成功创建！
✅ 数据从表 'tbl_wide_user_retention_mobie' 成功提取！
✅ report表数据已成功写入 tbl_report_user_retention_mobie 中！


该项目搭建了一套AB测试全套数据管道，主要包含3部分：
1.从growthbook调取实验参数到数仓的完整ETL流程
2.数仓宽表ETL
3.report表ETL（贝叶斯算法、uplift算法）

* 全部脚本的实验参数已经抽象化，可以适用于任何实验。
* 添加job function 定时功能，可以实现实时计算展示效果。
* 根据tag（growthbook上的配置参数，可手动选择，对应不同团队：产品、前后端、推荐）调取实验参数，
* 经过定时的脚本运算后，可以在Metabase(任何BI平台），自动更新dashboard数据结果。


