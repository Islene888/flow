import os
import urllib.parse
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from scipy.stats import ttest_ind, zscore
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import zipfile
import time
import openpyxl

from tag_comparie.process import generate_markdown, zip_output, remove_outliers_by_zscore, prepare_data, \
    stratified_sample_by_metric

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

TAGS = ["Sora"]
OUTPUT_DIR = "../task_ai_bot/output"
BATCH_SIZE = 1000
MAX_WORKERS = 6
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    db_url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_rds_prod?charset=utf8mb4"
    return create_engine(db_url)

engine = get_engine()

def read_sql(query, label):
    print(f"🧮 正在查询 {label} 数据...")
    start = time.time()
    df = pd.read_sql(query, con=engine)
    print(f"✅ {label} 获取成功，共 {len(df)} 行，用时 {round(time.time() - start, 2)} 秒\n")
    return df

def get_prompt_ids_by_tag(tag_name):
    path = os.path.join(OUTPUT_DIR, f"{tag_name}_prompt_ids.csv")
    if os.path.exists(path):
        print(f"📦 从缓存加载 Prompt ID：{path}")
        return pd.read_csv(path)
    sql = f'''
        SELECT DISTINCT p.`"id"` AS prompt_id, p.`"title"` AS name
        FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
        JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
        JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
        WHERE pt.`"name"` = '{tag_name}'
    '''
    df = read_sql(sql, f"{tag_name} Prompt IDs")
    df.to_csv(path, index=False)
    return df

def get_prompt_ids_excluding_tag(tag_name):
    sql = f'''
        SELECT DISTINCT p.`"id"` AS prompt_id, p.`"title"` AS name
        FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
        JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
        JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
        WHERE pt.`"name"` != '{tag_name}'
    '''
    return read_sql(sql, f"非{tag_name} Prompt IDs")

def run_clustering(df, tag_name):
    cluster_path = os.path.join(OUTPUT_DIR, f"{tag_name}_clustered.csv")
    if os.path.exists(cluster_path):
        print(f"📦 读取缓存聚类结果：{cluster_path}")
        return pd.read_csv(cluster_path)

    features = ["click_rate", "chat_rate", "chat_depth", "conv_per_user"]
    df_filtered = remove_outliers_by_zscore(df.copy(), features)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_filtered[features])
    best_k, best_score = 2, -1
    for k in range(2, 7):
        model = KMeans(n_clusters=k, random_state=42).fit(X_scaled)
        score = silhouette_score(X_scaled, model.labels_)
        if score > best_score:
            best_k, best_score, best_model = k, score, model
    df_filtered["cluster"] = best_model.predict(X_scaled)
    pca = PCA(n_components=2)
    df_filtered[["pca1", "pca2"]] = pca.fit_transform(X_scaled)
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df_filtered, x="pca1", y="pca2", hue="cluster", palette="Set2", s=60)
    plt.title(f"{tag_name} 聚类结果")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f"{tag_name}_聚类_PCA.png"))
    top_prompts = df_filtered.sort_values("cluster")
    top_n = top_prompts.groupby("cluster").head(5)
    top_n.to_csv(os.path.join(OUTPUT_DIR, f"top_prompts_by_cluster_{tag_name}.csv"), index=False)
    df_filtered.to_csv(cluster_path, index=False)
    return df_filtered

def analyze_tag(tag_name):
    start = time.time()

    # 标签组
    df_tag = get_prompt_ids_by_tag(tag_name)
    df_tag = prepare_data(df_tag, tag_name=tag_name, group_name="tag")

    df_all_other = get_prompt_ids_excluding_tag(tag_name)
    df_preview_other = prepare_data(df_all_other.sample(n=8000, random_state=42), tag_name=tag_name, group_name="preview_other")

    df_sampled_other = stratified_sample_by_metric(df_preview_other, df_tag, metric='total_clicks')
    df_other = df_sampled_other

    # 合并打标
    df_tag["group"] = tag_name
    df_other["group"] = f"非{tag_name}"
    df_all = pd.concat([df_tag, df_other])
    df_all.to_csv(os.path.join(OUTPUT_DIR, f"{tag_name}_combined_labeled.csv"), index=False)

    # 指标分析
    metrics = ["click_rate", "chat_rate", "chat_depth", "conv_per_user"]
    summary = []
    for m in metrics:
        mean_1 = df_tag[m].mean()
        mean_2 = df_other[m].mean()
        diff = mean_1 - mean_2
        stat, pval = ttest_ind(df_tag[m], df_other[m], equal_var=False)
        summary.append([m, round(mean_1, 4), round(mean_2, 4), round(diff, 4), round(pval, 4), "✅" if pval < 0.05 else "❌"])

        plt.figure(figsize=(8, 4))
        sns.kdeplot(df_tag[m], label=tag_name, fill=True)
        sns.kdeplot(df_other[m], label=f"非{tag_name}", fill=True)
        plt.title(f"{m} 分布对比 - {tag_name}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"{tag_name}_{m}_分布对比.png"))

        plt.figure(figsize=(6, 4))
        sns.boxplot(data=df_all, x="group", y=m)
        plt.title(f"{m} 箱线图对比 - {tag_name}")
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, f"{tag_name}_{m}_箱线图对比.png"))

    df_summary = pd.DataFrame(summary, columns=["指标", f"{tag_name}均值", f"非{tag_name}均值", "差值", "p值", "显著"])
    df_summary.to_csv(os.path.join(OUTPUT_DIR, f"{tag_name}_指标对比.csv"), index=False)

    run_clustering(df_tag, tag_name)
    generate_markdown(tag_name, df_summary, metrics)
    zip_output(tag_name)
    print(f"⏱️ 总耗时：{round(time.time() - start, 2)} 秒")

if __name__ == '__main__':
    for tag in TAGS:
        analyze_tag(tag)
    print("\n✅ 所有标签分析 + Markdown 报告 + ZIP 打包完成！")