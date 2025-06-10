import json
import requests
from web3 import Web3
from web3._utils.events import get_event_data

API_KEY = "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
CHAIN_ID = 8453

# ✅ Step 1: 输入 log 内容
log = {
    "address": "0xF4DFb8647C3Ef75c5A71b7B0ee9240BdccCe8697",
    "topics": [
        "19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83",
        "00000000000000000000000017cf46ed08lccef3c8abca349633f4546cea1916",
        "00000000000000000000000017cf46ed08lccef3c8abca349633f4546cea1916"
    ],
    "data": (
        "fffffffffffffffffffffffffffffffffffffffffffffffffedb2bfb400000000000000000000000000000000000000000000000000000000000000014cd"
        "ee6c1e79db4fc8000000000000000000000000000000000000000000000000000000000000000444033090cba476e9114ffaa3200000000000000000000000"
        "00000000000000000230fecbd9c8f79306a500000000000000000000000000000000000000000000000000000000000000149f300000000000000000000000"
        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002cfe53ea2bfd00"
    ),
    "blockNumber": 28672346,
    "transactionIndex": 15,
    "logIndex": 50
}

# ✅ Step 2: 获取合约 ABI
def fetch_abi(address):
    url = f"https://api.etherscan.io/v2/api?chainid={CHAIN_ID}&module=contract&action=getabi&address={address}&apikey={API_KEY}"
    resp = requests.get(url)
    data = resp.json()
    if data.get("status") != "1":
        raise Exception(f"❌ ABI 获取失败: {data.get('result')}")
    return json.loads(data["result"])

# ✅ Step 3: 解码 log
def decode_log(log):
    abi = fetch_abi(log["address"])
    w3 = Web3()
    topic0 = log["topics"][0]
    print("topic0: ", topic0)

    print("✅ ABI 中所有事件签名如下（明文签名 => keccak256 hash）:")
    for item in abi:
        if item.get("type") == "event":
            types = ",".join(i["type"] for i in item["inputs"])
            signature = f"{item['name']}({types})"
            hash = w3.keccak(text=signature).hex()
            print(f"{signature}  =>  {hash}")

    # 匹配事件（修复比较逻辑）
    event_abi = None
    for item in abi:
        if item.get("type") == "event":
            sig = w3.keccak(text=f"{item['name']}({','.join(i['type'] for i in item['inputs'])})").hex()
            if sig == topic0.lower() or "0x" + sig == topic0.lower():
                event_abi = item
                break
    if not event_abi:
        raise Exception("❌ 找不到匹配事件")

    # ✅ 正确解码：使用 get_event_data 而不是 contract.events[]
    decoded = get_event_data(w3.codec, event_abi, log)
    print(f"\n✅ 解码成功，事件名: {decoded['event']}")
    print(json.dumps(dict(decoded["args"]), indent=2))


# ✅ Run
decode_log(log)
