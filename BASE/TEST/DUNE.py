import requests
import time
import json
from dune_client.client import DuneClient
import pandas as pd
import os

API_KEY = "ND70rL9gwfRedkvc9acftqVI6MIqp73p"
DUNE_URL = "https://api.dune.com/api/v1"

# 初始化 Dune 客户端
dune = DuneClient(API_KEY)

# 查询 ID
QUERY_ID = 5315070

# 获取最新查询结果
try:
    query_result = dune.get_latest_result(QUERY_ID)
    if not query_result.result.rows:
        print("查询结果为空")
    else:
        # 将结果转换为 DataFrame
        df_new = pd.DataFrame(query_result.result.rows)

        # 文件路径
        output_path = f"dune_query_{QUERY_ID}.csv"

        # 如果文件存在，读取旧数据并合并去重
        if os.path.exists(output_path):
            df_old = pd.read_csv(output_path)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)

            # 你可以指定去重依据的列，例如 tx_hash + evt_index 确保唯一
            if 'tx_hash' in df_combined.columns and 'evt_index' in df_combined.columns:
                df_combined = df_combined.drop_duplicates(subset=['tx_hash', 'evt_index'])
            else:
                df_combined = df_combined.drop_duplicates()

            df_combined.to_csv(output_path, index=False)
            print(f"✅ 成功更新并去重保存到 {output_path}")
        else:
            # 文件不存在，直接保存
            df_new.to_csv(output_path, index=False)
            print(f"✅ 成功首次保存到 {output_path}")

except Exception as e:
    print(f"❌ 获取或保存结果时出错: {e}")
