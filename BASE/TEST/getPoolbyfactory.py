import requests
import json
from web3 import Web3
from web3._utils.events import get_event_data
from eth_utils import keccak

# ===== 1. 输入参数 =====
API_KEY = "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
factory_address = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
RPC_URL = "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"

# ===== 2. 获取工厂 ABI.csv =====
url = f"https://api.etherscan.io/v2/api?chainid=8453&module=contract&action=getabi&address={factory_address}&apikey={API_KEY}"
response = requests.get(url)
result = response.json()

if result["status"] != "1":
    print("❌ 获取 ABI.csv 失败:", result["result"])
    exit()

abi = json.loads(result["result"])

# ===== 3. 提取 PoolCreated 事件 ABI.csv 并构造事件签名 =====
event_abi = None
for item in abi:
    if item.get("type") == "event" and item.get("name") == "PoolCreated":
        event_abi = item
        break

if not event_abi:
    print("❌ 未找到 PoolCreated 事件")
    exit()

# 构造事件签名字符串：例如 "PoolCreated(address,address,uint24,int24,address)"
types = [input["type"] for input in event_abi["inputs"]]
event_sig = f'PoolCreated({",".join(types)})'
event_topic = "0x" + keccak(text=event_sig).hex()
print("event_topic:",event_topic)

# ===== 4. 监听事件日志 =====
w3 = Web3(Web3.HTTPProvider(RPC_URL))
latest = w3.eth.block_number
logs = w3.eth.get_logs({
    "address": factory_address,
    "fromBlock": latest - 5000,  # 建议别从 0 开始
    "toBlock": latest,
    "topics": [event_topic]
})

if not logs:
    print("❌ 未找到任何 PoolCreated 日志")
    exit()

# ===== 5. 解码第一个日志 =====
log = logs[0]
decoded = get_event_data(w3.codec, event_abi, log)

# ===== 6. 获取池子地址 =====
pool_address = decoded["args"]["pool"]
print("✅ 获取成功，一个池子地址为：", pool_address)
