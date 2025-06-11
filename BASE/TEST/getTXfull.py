import json
import requests
from web3 import Web3
from web3._utils.events import get_event_data

API_KEY = "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
CHAIN_ID = 8453

# 多个 Swap 类型的 topic0 哈希（统一为小写、无 0x 前缀）
SWAP_SIG_HASHES = {
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",
    "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b",
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
}


# ====== 1. 设置 Base RPC 与交易哈希 ======
RPC_URL = "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"
w3 = Web3(Web3.HTTPProvider(RPC_URL))
tx_hash = "0xe270542a513ec784f77bb7565e1a65ef03bb2cd78cfdc504204880c0c4703177"

# ====== 2. 获取交易收据 ======
try:
    receipt = w3.eth.get_transaction_receipt(tx_hash)
except Exception as e:
    print(f"❌ 获取交易收据失败: {e}")
    exit(1)

logs = receipt.get("logs", [])
if not logs:
    print("❌ 交易中无日志")
    exit(0)

# ====== 3. 获取合约 ABI.csv ======
def fetch_abi(address):
    url = f"https://api.etherscan.io/v2/api?chainid={CHAIN_ID}&module=contract&action=getabi&address={address}&apikey={API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    if data.get("status") != "1":
        raise Exception(f"❌ ABI 获取失败: {data.get('result')}")
    return json.loads(data["result"])


# ====== 4. 解码 log（仅在 topic0 命中后调用） ======
def decode_swap_log(log):
    try:
        abi = fetch_abi(log["address"])
    except Exception as e:
        print(f"❌ 获取 ABI 失败: {e}")
        return None

    topic0 = Web3.to_hex(log["topics"][0]).lower()[2:]
    event_abi = None
    for item in abi:
        if item.get("type") == "event":
            sig = Web3.keccak(text=f"{item['name']}({','.join(i['type'] for i in item['inputs'])})").hex()
            if sig == topic0:
                print(item)
                event_abi = item
                break
    if not event_abi:
        print(f"❌ 未在 ABI 中找到 Swap 事件: {log['address']}")
        return None

    try:
        decoded = get_event_data(w3.codec, event_abi, log)
        return {
            "event": decoded["event"],
            "address": log["address"],
            "args": dict(decoded["args"]),
        }
    except Exception as e:
        print(f"❌ 解码失败: {e}")
        return None

# ====== 5. 遍历所有 log，仅解码 topic0 命中的 swap log ======
print(f"🔍 正在扫描交易 {tx_hash} 中的 Swap 日志...")
swap_logs = []
for idx, log in enumerate(logs):
    topic0 = Web3.to_hex(log["topics"][0]).lower()
    if  topic0 not in SWAP_SIG_HASHES:
        continue
    print(f"\n➡️ Log #{idx} 命中 Swap 事件 (合约: {log['address']})")
    result = decode_swap_log(log)
    if result:
        #print(f"✅ 解码成功 - 事件: {result['event']}")
        #print(json.dumps(result["args"], indent=2))
        swap_logs.append(result)

if not swap_logs:
    print("\n❌ 没有匹配的 Swap 日志")
