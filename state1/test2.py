import urllib.parse
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib
from sqlalchemy.exc import SQLAlchemyError
import re
from datetime import datetime
import logging

# ✅ 初始化日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ✅ 解决 Matplotlib 中文乱码
matplotlib.rcParams['font.sans-serif'] = ['Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False

# ✅ 安全密码处理
password = urllib.parse.quote_plus("flowgpt@2024.com")
DATABASE_URL = f"mysql+pymysql://bigdata:{password}@18.188.196.105:9030/flow_test"
engine = create_engine(DATABASE_URL)


def validate_experiment_id(experiment_id):
    """验证实验ID格式（白名单验证）"""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', experiment_id):
        raise ValueError(f"Invalid experiment_id format: {experiment_id}")


def parse_iso_datetime(dt_str):
    """灵活解析ISO日期时间格式"""
    try:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')


def get_experiment_params():
    """获取实验参数（增强版）"""
    query = """
    SELECT experiment_id, phases_info
    FROM tbl_experiment_data
    WHERE experiment_id IS NOT NULL
    """
    try:
        experiment_data = pd.read_sql(query, engine)
        if experiment_data.empty:
            raise ValueError("No experiments found in tbl_experiment_data")

        experiment_params = []
        for _, row in experiment_data.iterrows():
            experiment_id = str(row['experiment_id'])
            validate_experiment_id(experiment_id)  # 安全验证

            phases_info = row['phases_info']
            logger.info(f"Processing experiment: {experiment_id}")

            # ✅ 改进的日期解析逻辑
            date_pattern = r"\((\d{4}-\d{2}-\d{2}T[\d:.]+Z?) - (\d{4}-\d{2}-\d{2}T[\d:.]+Z?)\)"
            matches = re.findall(date_pattern, phases_info)

            if not matches:
                logger.warning(f"Skipping invalid phases_info format: {experiment_id}")
                continue

            try:
                start_date = parse_iso_datetime(matches[0][0])
                end_date = parse_iso_datetime(matches[0][1])
            except Exception as e:
                logger.error(f"Date parsing failed for {experiment_id}: {str(e)}")
                continue

            experiment_days = (end_date - start_date).days + 1
            if experiment_days <= 0:
                logger.error(f"Invalid date range for {experiment_id}")
                continue

            experiment_params.append({
                "experiment_id": experiment_id,
                "start_date": start_date,
                "end_date": end_date,
                "experiment_days": experiment_days
            })

        return experiment_params

    except Exception as e:
        logger.error(f"Error fetching experiment params: {str(e)}")
        raise


def create_retention_table(experiment_id, experiment_days):
    """创建留存率结果表（带事务处理）"""
    table_name = f"tbl_new_retention_results_{experiment_id}"

    columns = [
        "dt DATE", "day INT", "variation VARCHAR(255)",
        "users INT", "retained INT", "retention_rate DOUBLE",
        "ci_lower DOUBLE", "ci_upper DOUBLE", "control_rate DOUBLE",
        "exp_rate DOUBLE", "uplift DOUBLE", "uplift_ci_lower DOUBLE",
        "uplift_ci_upper DOUBLE", "z_score DOUBLE", "p_value DOUBLE",
        "retention_rate_baseline DOUBLE"
    ]

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    try:
        with engine.begin() as conn:  # 使用事务
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            conn.execute(text(create_sql))
            logger.info(f"Table {table_name} created/verified")
    except SQLAlchemyError as e:
        logger.error(f"Table creation failed: {str(e)}")
        raise


def calculate_retention_stats(df, experiment_days):
    """计算留存统计指标（增强空值处理）"""
    results = []
    days_columns = [f"d{i}" for i in range(1, experiment_days + 1)]

    for _, row in df.iterrows():
        if row["users"] <= 0:
            continue

        base_data = {
            "dt": row["dt"],
            "variation": str(row["variation"]),
            "users": row["users"]
        }

        for idx, day_col in enumerate(days_columns, 1):
            retained = row.get(day_col, 0)
            if pd.isna(retained):
                retained = 0

            retention_rate = retained / row["users"] if row["users"] > 0 else 0.0
            se = np.sqrt((retention_rate * (1 - retention_rate)) / row["users"]) if row["users"] > 0 else 0.0

            results.append({
                **base_data,
                "day": idx,
                "retained": retained,
                "retention_rate": round(retention_rate, 4),
                "ci_lower": round(max(retention_rate - 1.96 * se, 0.0), 4),
                "ci_upper": round(min(retention_rate + 1.96 * se, 1.0), 4)
            })

    return pd.DataFrame(results)


def calculate_uplift(control_df, experiment_df):
    """计算增长率指标（带鲁棒性检查）"""
    comparison_results = []

    if control_df.empty:
        logger.warning("No control group data available")
        return pd.DataFrame()

    for (dt, day), c_group in control_df.groupby(["dt", "day"]):
        if c_group.empty:
            continue

        control_rate = c_group["retention_rate"].values[0]
        control_users = c_group["users"].values[0]
        if control_users == 0:
            continue

        for variation, exp_group in experiment_df.groupby("variation"):
            exp_data = exp_group[(exp_group["dt"] == dt) & (exp_group["day"] == day)]
            if exp_data.empty:
                continue

            exp_row = exp_data.iloc[0]
            exp_rate = exp_row["retention_rate"]
            exp_users = exp_row["users"]

            # ✅ 安全计算逻辑
            uplift = np.nan
            uplift_ci_lower = np.nan
            uplift_ci_upper = np.nan
            z_score = np.nan
            p_value = np.nan

            if control_rate > 0 and exp_users > 0:
                try:
                    # 计算Uplift
                    uplift = (exp_rate - control_rate) / control_rate

                    # 计算标准误
                    control_se = np.sqrt((control_rate * (1 - control_rate)) / control_users)
                    exp_se = np.sqrt((exp_rate * (1 - exp_rate)) / exp_users)
                    uplift_se = np.sqrt((control_se / control_rate) ** 2 + (exp_se / exp_rate) ** 2)

                    # 置信区间
                    uplift_ci_lower = uplift - 1.96 * uplift_se
                    uplift_ci_upper = uplift + 1.96 * uplift_se

                    # 假设检验
                    z_score = (exp_rate - control_rate) / np.sqrt(control_se ** 2 + exp_se ** 2)
                    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
                except Exception as e:
                    logger.debug(f"Error in stats calculation: {str(e)}")

            comparison_results.append({
                "dt": dt,
                "day": day,
                "variation": variation,
                "control_rate": round(control_rate, 4),
                "exp_rate": round(exp_rate, 4),
                "uplift": round(uplift, 4) if not np.isnan(uplift) else None,
                "uplift_ci_lower": round(uplift_ci_lower, 4) if not np.isnan(uplift_ci_lower) else None,
                "uplift_ci_upper": round(uplift_ci_upper, 4) if not np.isnan(uplift_ci_upper) else None,
                "z_score": round(z_score, 4) if not np.isnan(z_score) else None,
                "p_value": round(p_value, 4) if not np.isnan(p_value) else None
            })

    return pd.DataFrame(comparison_results)


def main():
    try:
        experiment_params = get_experiment_params()
        if not experiment_params:
            logger.error("No valid experiments to process")
            return

        for params in experiment_params:
            experiment_id = params["experiment_id"]
            start_date = params["start_date"].strftime('%Y-%m-%d')
            end_date = params["end_date"].strftime('%Y-%m-%d')
            experiment_days = params["experiment_days"]

            # ✅ 创建目标表
            create_retention_table(experiment_id, experiment_days)

            # ✅ 获取原始数据
            logger.info(f"Fetching data for {experiment_id}")
            query = f"""
            SELECT * FROM tbl_user_engagement_filtered_{experiment_id}
            ORDER BY dt ASC, CAST(variation AS UNSIGNED) ASC;
            """
            try:
                df = pd.read_sql(query, engine)
                if df.empty:
                    logger.warning(f"No data found for {experiment_id}")
                    continue
            except Exception as e:
                logger.error(f"Query failed: {str(e)}")
                continue

            # ✅ 计算基础指标
            result_df = calculate_retention_stats(df, experiment_days)
            if result_df.empty:
                logger.warning(f"No Retention data for {experiment_id}")
                continue

            # ✅ 分割对照组/实验组
            control_df = result_df[result_df["variation"] == "0"]
            experiment_df = result_df[result_df["variation"] != "0"]

            # ✅ 计算增量指标
            comparison_df = calculate_uplift(control_df, experiment_df)

            # ✅ 合并最终结果
            final_df = pd.merge(
                result_df,
                comparison_df,
                on=["dt", "day", "variation"],
                how="left"
            )

            # ✅ 添加基准留存率
            baseline_df = control_df[["dt", "day", "retention_rate"]].rename(
                columns={"retention_rate": "retention_rate_baseline"}
            )
            final_df = pd.merge(
                final_df,
                baseline_df,
                on=["dt", "day"],
                how="left"
            )

            # ✅ 数据清洗
            final_df = final_df.where(pd.notnull(final_df), None)
            final_df["day"] = final_df["day"].astype(int)

            # ✅ 保存结果
            table_name = f"tbl_new_retention_results_{experiment_id}"
            try:
                final_df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                    method='multi'
                )
                logger.info(f"Data saved to {table_name}")
            except Exception as e:
                logger.error(f"Database save failed: {str(e)}")

            # ✅ 生成可视化
            plt.figure(figsize=(12, 6))
            sns.lineplot(
                data=final_df[final_df["variation"] != "0"],
                x="day",
                y="retention_rate",
                hue="variation",
                style="variation",
                markers=True,
                dashes=False
            )
            plt.axhline(
                y=control_df["retention_rate"].mean(),
                color='red',
                linestyle='--',
                label='Control Baseline'
            )
            plt.title(f"Retention Analysis - Experiment {experiment_id}")
            plt.xlabel("Days Since First Visit")
            plt.ylabel("Retention Rate")
            plt.legend()
            plt.savefig(f"retention_plot_{experiment_id}.png")
            plt.close()

    except Exception as e:
        logger.error(f"Critical error in main process: {str(e)}")
        raise


if __name__ == "__main__":
    main()