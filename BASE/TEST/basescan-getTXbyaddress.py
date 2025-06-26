import requests

# 替换成你的 API KEY 和地址
api_key = "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
address = "0xDD574Eed020EF302eC2CE0241E56d5F851001898"

url = (
    f"https://api.etherscan.io/v2/api"
    f"?chainid=8453"
    f"&module=account"
    f"&action=txlist"
    f"&address={address}"
    f"&startblock=0"
    f"&endblock=99999999"
    f"&page=1"
    f"&offset=10"
    f"&sort=asc"
    f"&apikey={api_key}"
)

response = requests.get(url)
data = response.json()

# 打印结果
if data.get("status") == "1":
    for tx in data["result"]:
        print(f"Hash: {tx['hash']}, From: {tx['from']}, To: {tx['to']}, Value: {tx['value']}")
else:
    print(f"Error: {data.get('message')}")
