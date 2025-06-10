import requests
import json

# 替换为您要查询的 Token Mint 地址
TOKEN_MINT_ADDRESS = "YourTokenMintAddressHere"

# Solana 主网 RPC 端点
RPC_URL = "https://api.mainnet-beta.solana.com"

# 构造请求负载
payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": [
        TOKEN_MINT_ADDRESS,
        {
            "encoding": "jsonParsed"
        }
    ]
}

# 设置请求头
headers = {
    "Content-Type": "application/json"
}

# 发送 POST 请求
response = requests.post(RPC_URL, headers=headers, data=json.dumps(payload))

# 打印原始 JSON 响应
print("---- 原始返回 ----")
print(response.text)

# 解析响应
if response.status_code == 200:
    result = response.json()
    account_info = result.get("result", {}).get("value", {})
    if account_info:
        data = account_info.get("data", {})
        parsed = data.get("parsed", {})
        info = parsed.get("info", {})
        decimals = info.get("decimals")
        supply = info.get("supply")
        mint_authority = info.get("mintAuthority")
        print("\n---- 解析结果 ----")
        print(f"Decimals: {decimals}")
        print(f"Supply: {supply}")
        print(f"Mint Authority: {mint_authority}")
    else:
        print("未找到账户信息。")
else:
    print(f"请求失败，状态码：{response.status_code}")
