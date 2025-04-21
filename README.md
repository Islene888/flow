# Full-Stack A/B Testing Data Pipeline

This project builds an enterprise-grade A/B testing data platform based on experiment configurations provided by GrowthBook. It covers core processes such as experimental data collection, user-level wide table construction, statistical modeling, and visual analysis—enabling end-to-end automation from data tracking to business insights.

## 1. Experiment Data Collection & Ingestion (ETL)
- Integrates with the GrowthBook API to automatically retrieve experiment metadata, traffic-splitting parameters, and configured metrics.
- Stores experiment configurations and user assignments into the `experiment_data` table in the data warehouse, enabling downstream metric aggregation and attribution.
- Decouples experiment management logic from assignment mechanics, ensuring auditability, traceability, and support for concurrent experiments.

## 2. User-Level Wide Table Construction
- Leverages big data frameworks such as PySpark and Hive to aggregate key user behaviors during the experiment period (e.g., login, activity, subscriptions, conversions) into unified wide tables (e.g., `tbl_wide_user_retention_xxx`).
- Automatically generates tag-specific wide tables for different business modules, supporting version control and parallel development across teams.
- All wide tables follow standardized schema and field naming conventions, ensuring consistency and reusability in modeling and analytics workflows.

## 3. Experiment Modeling & Report Generation
- Based on the wide tables, applies Bayesian inference, uplift modeling, and confidence interval estimation to produce robust experiment evaluation results.
- Automatically outputs core metric performance and statistical significance for each variation into standardized reporting tables such as `tbl_report_user_retention_xxx`.
- Report schemas are aligned with BI tools (e.g., Metabase) to enable seamless integration with visual dashboards, supporting real-time monitoring and business storytelling.

## Directory Structure
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
- **growthbook_fetcher/**: Scripts to retrieve experiment metadata from GrowthBook.  
- **Business/Retention/Subscribe/...**: ETL logic organized by business modules or teams.  
- **main_all.py**: Entry script to run the complete ETL pipeline (data fetch → wide table → report) for all configured `tag`s.  
- **main_run.py**: Example runner to test or execute a single tag pipeline.

## 4. Output Tables
- `experiment_data`: Stores basic experiment configuration and user assignment info pulled from GrowthBook.
- `tbl_wide_user_retention_xxx`: Unified wide tables that aggregate core metrics such as assignment, retention, and monetization.
- `tbl_report_user_retention_xxx`: Report tables with metrics such as confidence intervals, Bayesian win rates, uplift scores—ready for consumption by BI tools like Metabase.

## Business Value & Technical Highlights

### Business Value
- Compared to GrowthBook’s native in-platform reporting, this platform integrates directly with enterprise data warehouses and behavior systems, supporting flexible metric definitions and high-quality data standardization.
- Enables end-to-end automation and reduces dependence on manual analysis, allowing fast iteration on high-frequency experiments.
- Supports unified experimentation across multiple business lines via shared platform and data structure—reducing duplication and improving horizontal comparability.
- Produces reusable and traceable experiment data assets, enabling scenario-specific needs such as auditing, retrospective analysis, team collaboration, and executive reporting.

### Technical Highlights
- Applies advanced statistical methodologies including Bayesian inference and uplift modeling, offering greater stability in evaluation and suitability for small-sample or noisy metric scenarios.
- Modular ETL architecture with tag-based extensibility to support diverse business cases and experiment types.
- Unifies multi-source heterogeneous data (behavior, assignment, revenue) into coherent pipelines for full-funnel insight.
- Standardized schema design across wide tables and report outputs ensures high-quality data reuse and seamless integration with downstream analytics, modeling, or visualization systems.

## FAQ

### 1. How do I add a new experiment?
Simply configure a new experiment in GrowthBook and assign a tag. The pipeline will automatically detect and process it during the next scheduled run.

### 2. How are tasks scheduled?
You can schedule `main_all.py` or `main_run.py` via Airflow, Crontab, or any orchestration system to automate the data pipeline.

### 3. How can I visualize the results?
Connect the report tables to Metabase (or other BI tools) to access real-time dashboards and experiment visualizations.




# A/B 测试全流程数据管道

本项目基于 GrowthBook 提供的实验配置，构建了一套企业级的 A/B 测试数据开发平台，覆盖实验数据采集、用户行为宽表构建、统计建模与可视化分析等核心流程，实现从数据埋点到业务洞察的全链路自动化处理。

## 1. 实验数据采集与入仓 (ETL)
- 接入 GrowthBook API，自动化采集实验元数据、分流参数、指标配置等核心信息，构建标准化实验配置表。
- 将实验配置与分组结果写入数据仓库中的 `experiment_data` 表，为后续数据集成与指标归因打通关键链路。
- 实验管理逻辑与分组机制解耦，具备可审计性、可追溯性，支持多实验并行执行。

## 2. 用户维度宽表构建
- 基于 PySpark/Hive 等大数据处理框架，聚合用户在实验期内的关键行为数据（如登录、活跃、订阅、转化等），统一汇总至宽表（如 `tbl_wide_user_retention_xxx`）。
- 系统根据业务模块自动识别并生成对应 tag 的宽表，支持版本管理与多业务模块并行开发。
- 所有宽表采用统一 schema 与字段规范，确保各模块建模与分析逻辑的一致性与可复用性。

## 3. 实验结果建模与报告生成
- 在宽表基础上，融合贝叶斯推断、Uplift 模型、置信区间估计等算法，生成更稳健的实验评估结果。
- 自动输出各 variation 下的关键指标表现及统计显著性结论，结果落地于 `tbl_report_user_retention_xxx` 等标准报告表。
- 报告表结构对齐 BI 工具（如 Metabase），支持一键接入可视化看板，实现实时监控与业务解释闭环。

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
- **Business/Retention/Subscribe/...**：按业务场景或团队分组的处理脚本。
- **main_all.py**：可一键执行完整 ETL 流程（数据采集、宽表构建、报告生成），遍历所有 tag 执行实验数据。
- **main_run.py**：示例脚本，可指定特定 `tag` 进行测试或局部执行。

## 4. 查看结果
- `experiment_data`：存储从 GrowthBook 获取的实验基础配置与分组数据。
- `tbl_wide_user_retention_xxx`：用户行为宽表，整合了用户分组、活跃留存、付费订阅等核心业务指标。
- `tbl_report_user_retention_xxx`：实验效果报告表，包含置信区间、贝叶斯胜率、Uplift 值等多种实验结果分析指标，可在 Metabase 等 BI 工具中实时展示。

## 业务价值与技术要点

### 业务价值
- 相比 GrowthBook 平台内置的简易指标分析，该平台可对接企业级数据仓库与用户行为系统，实现灵活口径定义与高质量数据沉淀。
- 全链路自动化部署，提升实验分析效率，降低依赖数据分析师人工干预，支持高频实验的快速迭代。
- 多业务线共用统一平台与数据结构，减少重复建设，提升实验数据的对齐度与横向可比性。
- 实验数据具备可复用性与可追踪性，支持数据回溯、决策复盘、团队协同、指标审计等真实业务场景。

### 技术要点
- 引入贝叶斯推断、Uplift 模型等前沿统计方法，提升实验评估稳定性与实用性，适配中小样本与多变指标场景。
- 模块化 ETL 流程设计，支持通过 tag 拓展至更多业务场景与实验类型，具备强扩展性。
- 多源异构数据对齐与统一建模能力，打通用户行为、实验分组与转化收入之间的全链路数据。
- 宽表与报告表标准规范定义，保证数据资产高质量复用，便于与建模、分析、可视化系统集成。

## FAQ

### 1. 如何添加新的实验？
在 GrowthBook 新增实验并配置相应的 tag，脚本会在下次运行时自动检索并处理该实验。

### 2. 如何调度任务？
可通过 Airflow、Crontab 或其他调度系统定时运行 `main_all.py` 或 `main_run.py`，实现自动化数据管道。

### 3. 如何可视化分析？
将数仓中的报告表接入 Metabase （或其他 BI 工具），即可实时查看实验指标和可视化报表。

