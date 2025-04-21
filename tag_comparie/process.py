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

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

TAGS = ["Sora"]
OUTPUT_DIR = "output"
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
    sql = f'''
        SELECT DISTINCT p.`"id"` AS prompt_id, p.`"title"` AS name
        FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
        JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
        JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
        WHERE pt.`"name"` = '{tag_name}'
    '''
    return read_sql(sql, f"{tag_name} Prompt IDs")

def get_prompt_ids_excluding_tag(tag_name):
    sql = f'''
        SELECT DISTINCT p.`"id"` AS prompt_id, p.`"title"` AS name
        FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
        JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
        JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
        WHERE pt.`"name"` != '{tag_name}'
    '''
    return read_sql(sql, f"非{tag_name} Prompt IDs")

def fetch_behavior(prompt_ids):
    ids_str = ",".join([f"'{x}'" for x in prompt_ids])
    def _sql(table, id_col, fields, event_col):
        return f'''
        SELECT 
        /*+ SET_VAR(query_timeout = 30000) */ 
            {id_col} AS prompt_id,
               {fields}
        FROM {table}
        WHERE {event_col} > '2025-01-01'
          AND {id_col} IN ({ids_str})
        GROUP BY {id_col}
        '''
    return (
        read_sql(_sql("flow_event_info.tbl_app_event_bot_view", "bot_id",
                      "COUNT(*) AS total_clicks, COUNT(DISTINCT user_id) AS total_click_users", "event_date"), "点击行为"),
        read_sql(_sql("flow_event_info.tbl_app_event_show_prompt_card", "prompt_id",
                      "COUNT(*) AS total_shows", "event_date"), "展示行为"),
        read_sql(_sql("flow_event_info.tbl_app_event_chat_send", "prompt_id",
                      "COUNT(*) AS total_chat_count, COUNT(DISTINCT user_id) AS total_chat_users, COUNT(DISTINCT conversation_id) AS total_convs", "event_date"), "聊天行为"),
        read_sql(_sql("flow_event_info.tbl_app_event_bot_follow", "bot_id",
                      "COUNT(DISTINCT event_id) AS total_follows", "event_date"), "关注行为")
    )

def fetch_behavior_in_parallel(prompt_ids, batch_size=BATCH_SIZE, max_workers=MAX_WORKERS):
    def fetch_batch(batch):
        try:
            return fetch_behavior(batch)
        except Exception as e:
            print(f"❌ 批次拉取失败，跳过：{e}")
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    all_click, all_show, all_chat, all_follow = [], [], [], []
    print(f"🚀 并发拉取 {len(prompt_ids)} 个 prompt_id，batch_size={batch_size}, workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_batch, prompt_ids[i:i+batch_size]) for i in range(0, len(prompt_ids), batch_size)]
        for i, f in enumerate(as_completed(futures), 1):
            df_click, df_show, df_chat, df_follow = f.result()
            all_click.append(df_click)
            all_show.append(df_show)
            all_chat.append(df_chat)
            all_follow.append(df_follow)
            print(f"✅ 完成第 {i}/{len(futures)} 批")
    return (
        pd.concat(all_click, ignore_index=True),
        pd.concat(all_show, ignore_index=True),
        pd.concat(all_chat, ignore_index=True),
        pd.concat(all_follow, ignore_index=True)
    )

def prepare_data(df_prompt, tag_name=None, group_name="tag"):
    cache_path = os.path.join(OUTPUT_DIR, tag_name or "cache", f"{group_name}_behavior.csv")
    if os.path.exists(cache_path):
        print(f"📦 读取缓存行为数据：{cache_path}")
        return pd.read_csv(cache_path)

    prompt_ids = df_prompt["prompt_id"].tolist()
    df_click, df_show, df_chat, df_follow = fetch_behavior_in_parallel(prompt_ids)
    df = df_prompt.merge(df_click, on="prompt_id", how="left") \
                  .merge(df_show, on="prompt_id", how="left") \
                  .merge(df_chat, on="prompt_id", how="left") \
                  .merge(df_follow, on="prompt_id", how="left")
    df = df.fillna(0)
    df["click_rate"] = df["total_clicks"] / df["total_shows"].replace(0, 1)
    df["chat_rate"] = df["total_chat_users"] / df["total_click_users"].replace(0, 1)
    df["chat_depth"] = df["total_chat_count"] / df["total_chat_users"].replace(0, 1)
    df["conv_per_user"] = df["total_convs"] / df["total_chat_users"].replace(0, 1)

    if tag_name:
        os.makedirs(os.path.join(OUTPUT_DIR, tag_name), exist_ok=True)
        df.to_csv(cache_path, index=False)
        print(f"✅ 缓存已保存：{cache_path}")
    return df


def remove_outliers_by_zscore(df, features, threshold=3):
    z_scores = np.abs(zscore(df[features]))
    return df[(z_scores < threshold).all(axis=1)]

def stratified_sample_by_metric(df, target_df, metric='total_clicks', bins=5, sample_size=None):
    target_hist = pd.qcut(target_df[metric], q=bins, duplicates='drop')
    df["bin"] = pd.qcut(df[metric], q=bins, duplicates='drop')
    sampled = []
    for b in target_hist.unique():
        bin_df = df[df["bin"] == b]
        n = len(target_df[target_hist == b]) if sample_size is None else sample_size // bins
        sampled.append(bin_df.sample(n=min(n, len(bin_df)), random_state=42))
    return pd.concat(sampled).drop(columns=["bin"])

def run_clustering(df, tag_name):
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
    return df_filtered

def generate_markdown(tag_name, df_summary, metrics):
    md_path = os.path.join(OUTPUT_DIR, f"{tag_name}_report.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Prompt 标签分析报告 - {tag_name}\n")
        f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("## 📊 指标对比分析\n\n")
        f.write("| 指标 | 标签均值 | 大盘均值 | 差值 | p值 | 显著性 |\n")
        f.write("|------|-----------|------------|--------|------|--------|\n")
        for row in df_summary.itertuples():
            f.write(f"| {row.指标} | {row[1]} | {row[2]} | {row.差值} | {row.p值} | {row.显著} |\n")
        f.write("\n## 📈 分布图 + 箱线图\n")
        for m in metrics:
            f.write(f"\n### {m}\n")
            f.write(f"![{m}分布](./{tag_name}_{m}_分布对比.png)\n")
            f.write(f"![{m}箱线图](./{tag_name}_{m}_箱线图对比.png)\n")
        f.write("\n## 🧠 聚类结果 PCA 可视化\n")
        f.write(f"![聚类图](./{tag_name}_聚类_PCA.png)\n")
        f.write("\n## 🔍 每个聚类代表性 Prompt 示例\n")
        cluster_path = os.path.join(OUTPUT_DIR, f"top_prompts_by_cluster_{tag_name}.csv")
        if os.path.exists(cluster_path):
            df_clusters = pd.read_csv(cluster_path)
            for cluster_id, group_df in df_clusters.groupby("cluster"):
                top_titles = group_df.head(3)["name"].tolist()
                f.write(f"\n### Cluster {cluster_id}\n")
                for title in top_titles:
                    f.write(f"- {title}\n")
        else:
            f.write(f"聚类示例未找到。请确认文件：{cluster_path}\n")

def zip_output(tag_name):
    zip_path = os.path.join(OUTPUT_DIR, f"{tag_name}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in os.listdir(OUTPUT_DIR):
            if file.startswith(tag_name):
                zipf.write(os.path.join(OUTPUT_DIR, file), arcname=file)
    print(f"🗜️ 结果已打包为：{zip_path}")

def analyze_tag(tag_name):
    start = time.time()

    # 标签组
    df_tag = get_prompt_ids_by_tag(tag_name)
    df_tag = prepare_data(df_tag, tag_name=tag_name, group_name="tag")

    df_all_other = get_prompt_ids_excluding_tag(tag_name)
    df_preview_other = prepare_data(df_all_other.sample(n=8000, random_state=42), tag_name=tag_name,
                                    group_name="preview_other")

    df_sampled_other = stratified_sample_by_metric(df_preview_other, df_tag, metric='total_clicks')
    df_other = df_sampled_other

    # 合并打标
    df_tag["group"] = tag_name
    df_other["group"] = f"非{tag_name}"
    df_all = pd.concat([df_tag, df_other])

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
    df_summary.to_excel(os.path.join(OUTPUT_DIR, f"{tag_name}_指标对比.csv"), index=False)

    run_clustering(df_tag, tag_name)
    generate_markdown(tag_name, df_summary, metrics)
    zip_output(tag_name)
    print(f"⏱️ 总耗时：{round(time.time() - start, 2)} 秒")

if __name__ == '__main__':
    for tag in TAGS:
        analyze_tag(tag)
    print("\n✅ 所有标签分析 + Markdown 报告 + ZIP 打包完成！")
