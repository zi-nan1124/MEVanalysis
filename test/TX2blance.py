import json
import pandas as pd
from pathlib import Path

def extract_account_keys(tx_json):
    """
    获取完整地址映射：accountKeys + loadedAddresses
    """
    msg = tx_json.get("transaction", {}).get("message", {})
    static = msg.get("accountKeys", [])
    msg = tx_json.get("meta", {}).get("loadedAddresses", {})
    loaded_w = msg.get("writable", [])
    loaded_r = msg.get("readonly", [])
    return static + loaded_w + loaded_r

def extract_balance_changes(tx_json: dict):
    """
    提取账户的 token balance 变化（使用 uiAmount，如为 null 则视为 0.0）
    返回字段：owner, mint, pre_amount, post_amount, delta
    """
    changes = []
    meta = tx_json.get("meta", {})
    pre_bal = meta.get("preTokenBalances", [])
    post_bal = meta.get("postTokenBalances", [])

    pre_map = {entry["accountIndex"]: entry for entry in pre_bal}
    post_map = {entry["accountIndex"]: entry for entry in post_bal}

    account_keys = extract_account_keys(tx_json)

    for idx in set(pre_map) | set(post_map):
        pre = pre_map.get(idx)
        post = post_map.get(idx)

        mint = pre["mint"] if pre else post["mint"]
        owner = account_keys[idx] if idx < len(account_keys) else f"[Index {idx}]"

        pre_ui = pre["uiTokenAmount"].get("uiAmount") if pre else None
        post_ui = post["uiTokenAmount"].get("uiAmount") if post else None

        # 若 uiAmount 是 null，视为 0.0
        pre_amt = float(pre_ui) if pre_ui is not None else 0.0
        post_amt = float(post_ui) if post_ui is not None else 0.0

        if pre_amt != post_amt:
            changes.append({
                "owner": owner,
                "mint": mint,
                "pre_amount": pre_amt,
                "post_amount": post_amt,
                "delta": round(post_amt - pre_amt, 9)  # 保留高精度
            })

    return changes



def main(input_path: str, output_path: str):
    with open(input_path, "r", encoding="utf-8") as f:
        tx_data = json.load(f)

    balance_changes = extract_balance_changes(tx_data)
    if not balance_changes:
        print("⚠️ 没有发现余额变化")
        return

    # 添加 delta 字段
    for change in balance_changes:
        change["delta"] = change["post_amount"] - change["pre_amount"]

    df = pd.DataFrame(balance_changes)
    print(df)

    df.to_csv(Path(output_path).with_suffix(".csv"), index=False)
    with open(Path(output_path).with_suffix(".json"), "w", encoding="utf-8") as f:
        json.dump(balance_changes, f, ensure_ascii=False, indent=2)

    print(f"✅ 已保存到 {output_path}.json 和 .csv")



if __name__ == "__main__":
    INPUT_FILE = "3r93J2cCevmugwYHTBZSixxuk7Z9cy3pQgRJRVSAh1PEhCi4zxBd9fZYiieB4B2D8QQmYLXbkbWAQNWcszWismK4_transaction.pretty.json"
    OUTPUT_FILE_PREFIX = "balance_changes_output"
    main(INPUT_FILE, OUTPUT_FILE_PREFIX)
