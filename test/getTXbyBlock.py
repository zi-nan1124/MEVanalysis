from solana.rpc.api import Client
import json
from pathlib import Path


def save_pretty_decoded_json_string_list(string_list, filepath):
    """
    输入是一组字符串格式的 JSON，每一项是 json.dumps(obj)，将它 decode 并格式化保存
    """
    try:
        decoded = [json.loads(tx_str) for tx_str in string_list]
    except Exception as e:
        raise ValueError(f"❌ JSON 解码失败，请检查内容是否为字符串化 JSON 对象: {e}")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(decoded, f, indent=2, ensure_ascii=False)

    print(f"✅ 已保存格式化 JSON 文件到: {filepath}")


def fetch_and_save_split_transactions(success_limit: int = 1, fail_limit: int = 1):
    # 初始化客户端
    client = Client("https://api.mainnet-beta.solana.com")

    # 获取最新 slot
    latest_slot = client.get_slot().value
    print(f"✅ 最新 Slot: {latest_slot}")

    # 获取区块
    block_response = client.get_block(
        slot=latest_slot,
        encoding="json",
        max_supported_transaction_version=0
    )

    if not block_response.value:
        print("❌ 获取区块失败")
        return

    block_data = block_response.value
    print(f"📦 区块包含交易数: {len(block_data.transactions)}")

    # 分类保存交易
    success_txs = []
    failed_txs = []

    for tx in block_data.transactions:
        json_str = tx.to_json()
        tx_obj = json.loads(json_str)
        err = tx_obj.get("meta", {}).get("err", None)

        if err is None and len(success_txs) < success_limit:
            success_txs.append(json_str)
        elif err is not None and len(failed_txs) < fail_limit:
            failed_txs.append(json_str)

        # 如果两个都够了，就停止遍历
        if len(success_txs) >= success_limit and len(failed_txs) >= fail_limit:
            break

    # 保存结果
    if success_txs:
        save_pretty_decoded_json_string_list(
            success_txs, Path(f"block_{latest_slot}_success.pretty.json"))
    else:
        print("⚠️ 未找到成功交易")

    if failed_txs:
        save_pretty_decoded_json_string_list(
            failed_txs, Path(f"block_{latest_slot}_failed.pretty.json"))
    else:
        print("⚠️ 未找到失败交易")


if __name__ == "__main__":
    fetch_and_save_split_transactions(success_limit=5, fail_limit=5)
