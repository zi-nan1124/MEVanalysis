import requests
import json

BASE_RPC_URL = "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"
tx_hash = "0xb8f25fbd7eb56f946d9d658db84d939e4e5579f6b7a3a38461443106414ba0dd"

def get_transaction_by_hash(tx_hash: str):
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_hash],
        "id": 1
    }

    try:
        response = requests.post(BASE_RPC_URL, json=payload, timeout=10)

        # 打印原始返回内容（无论是否成功）
        print("原始响应内容:")
        print(response.text)

        if response.status_code == 200:
            try:
                result = response.json().get("result")
                if result:
                    print("交易信息：")
                    print(json.dumps(result, indent=2))
                else:
                    print("未找到该交易或交易未打包")
            except json.JSONDecodeError:
                print("无法解析返回的 JSON 数据")
        else:
            print("请求失败，状态码：", response.status_code)

    except requests.exceptions.RequestException as e:
        print("请求异常：", str(e))

if __name__ == "__main__":
    get_transaction_by_hash(tx_hash)
