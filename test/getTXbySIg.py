from solana.rpc.api import Client
from solders.signature import Signature
import json
from pathlib import Path


def save_pretty_json(data, filepath):
    """保存为格式化后的 JSON 文件"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ 已保存格式化 JSON 到: {filepath}")


def fetch_transaction(signature: str, output_dir: str = ".", cluster: str = "https://api.mainnet-beta.solana.com"):
    client = Client(cluster)
    print(f"⏳ 正在获取交易：{signature}")

    try:
        sig_obj = Signature.from_string(signature)
    except Exception as e:
        print(f"❌ 无效签名: {e}")
        return

    response = client.get_transaction(
        sig_obj,
        encoding="json",
        max_supported_transaction_version=0
    )

    if not response.value:
        print("❌ 获取交易失败。可能该交易尚未确认或无效。")
        return

    # to_json() 返回的是字符串，不是 dict，必须再 loads 一次！
    raw_str = response.value.to_json()
    tx_json = json.loads(raw_str)

    # 构造输出路径
    output_path = Path(output_dir) / f"{signature}_transaction.pretty.json"
    save_pretty_json(tx_json, output_path)


if __name__ == "__main__":
    # 示例签名（替换为你的）
    tx_signature = "3r93J2cCevmugwYHTBZSixxuk7Z9cy3pQgRJRVSAh1PEhCi4zxBd9fZYiieB4B2D8QQmYLXbkbWAQNWcszWismK4"
    fetch_transaction(tx_signature)
