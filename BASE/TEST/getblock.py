import json
import httpx
from BASE.config import *

# ====== 配置 RPC 地址与目标区块号 ======
RPC_URL = BASE_RPC_URL  # 换成你的 RPC 地址
BLOCK_NUMBER = START_BLOCK  # 修改为你想查的区块号

# ====== 构造请求 payload ======
payload = {
    "jsonrpc": "2.0",
    "method": "eth_getBlockByNumber",
    "params": [hex(BLOCK_NUMBER), True],  # True 表示包含交易
    "id": 1
}

# ====== 发起 POST 请求 ======
try:
    response = httpx.post(RPC_URL, json=payload, timeout=15.0)
    response.raise_for_status()
    data = response.json()
    print(f"✅ 成功获取区块 {BLOCK_NUMBER} 原始数据")

    # ====== 保存为 JSON 文件 ======
    with open("test.json", "w") as f:
        json.dump(data, f, indent=2)
    print("✅ 已保存为 test.json")

except httpx.HTTPError as e:
    print(f"❌ HTTP 请求错误: {e}")
except Exception as e:
    print(f"❌ 其他错误: {e}")
