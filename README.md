# A/B 测试全流程数据管道

本项目基于 GrowthBook 提供的实验配置，构建了一套自动化的 A/B 测试数据管道，主要包括：

## 1. 实验数据采集与入仓 (ETL)
- 通过 GrowthBook API 拉取实验元数据、配置参数及相关指标。
- 将实验数据写入数据仓库的 `experiment_data` 表，便于后续聚合和分析。

## 2. 宽表 ETL
- 使用 PySpark/Hive 等大数据处理工具，将原始数据整合到宽表（如 `tbl_wide_user_retention_xxx`），实现多维度指标聚合。
- 针对不同业务组（通过 `tag` 区分）自动生成相应的宽表，以便灵活、可扩展地进行后续分析。

## 3. 报告表 ETL
- 基于宽表数据，执行贝叶斯算法、Uplift 算法等统计分析，输出实验效果评估指标。
- 将结果写入 `tbl_report_user_retention_xxx`，与任何 BI 工具（如 Metabase）对接，实现仪表盘自动更新。

## 目录结构
```
state3/
├── Advertisement/
├── Business/
├── Engagement/
├── growthbook_fetcher/
├── Recharge/
├── Retention/
├── Subscribe/
├── main_all.py
├── main_run.py
└── README.md
```
- **growthbook_fetcher/**：从 GrowthBook 获取实验数据的脚本。  
- **Business/Retention/Subscribe/...**：按业务场景或团队区分的处理脚本。  
- **main_all.py**：可一键执行完整 ETL 流程（数据采集、宽表构建、报告生成）。  
- **main_run.py**：示例脚本，可指定特定 `tag` 进行测试或局部执行。


### 4. 查看结果
- `experiment_data`：存储从 GrowthBook 获取的实验数据。
- `tbl_wide_user_retention_xxx`：宽表，聚合了核心业务指标。
- `tbl_report_user_retention_xxx`：报告表，包含贝叶斯、Uplift 分析结果，可在 Metabase 等 BI 工具中自动更新。

## 业务价值与技术要点

### 业务价值
- 支持多团队（产品、前后端、推荐等）同时开展 A/B 测试，降低实验冲突。
- 通过定时调度自动化运行脚本，实现快速迭代与实时洞察实验效果。

### 技术要点
- 通过贝叶斯与 Uplift 等算法评估实验效果，避免单纯依赖传统的统计检验方法。
- 基于标签化 (tag) 管理模式，可灵活扩展至更多业务模块或实验场景。

##  (FAQ)

### 1. 如何添加新的实验？
在 GrowthBook 新增实验并配置相应的 `tag`，脚本会在下次运行时自动检索并处理该实验。

### 2. 如何调度任务？
可通过 Airflow、Crontab 或其他调度系统定时运行 `main_all.py` 或 `main_run.py`，实现自动化管道。

### 3. 如何可视化分析？
将数仓中的报告表接入 Metabase（或其他 BI 工具），即可实时查看实验指标和可视化报表。


