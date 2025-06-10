import requests
import json
from eth_utils import keccak

API_KEY = "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
address = "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d"

url = f"https://api.etherscan.io/v2/api?chainid=8453&module=contract&action=getabi&address={address}&apikey={API_KEY}"
response = requests.get(url)
result = response.json()

if result["status"] == "1":
    print("✅ ABI 获取成功")
    abi = json.loads(result["result"])  # 解析为列表
    swap_events = [item for item in abi if item.get("type") == "event" and item.get("name") == "Swap"]

    if swap_events:
        for i, event in enumerate(swap_events, start=1):
            print(f"\n➡️ Log #{i} 命中 Swap 事件 (合约: {address})")

            # 生成签名
            inputs = event["inputs"]
            input_str = ",".join(inp["type"] for inp in inputs)
            signature = f'Swap({input_str})'
            print(signature)
            event_hash = keccak(text=signature).hex()
            print(f"事件签名哈希 (topic0): 0x{event_hash}")
    else:
        print("❌ 未找到名为 Swap 的事件")
else:
    print("❌ 失败原因：", result["result"])
