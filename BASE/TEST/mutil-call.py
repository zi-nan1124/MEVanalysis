import httpx
import pandas as pd
import json
import time
from pathlib import Path
from BASE.config import *
from BASE.LIB.common import logger

def batch_get_receipts_with_progress(
    input_file: str,
    rpc_url: str,
    limit: int = 10,
    output_file: str = "batch_receipts_result.json"
):
    # === 1️⃣ 加载数据 ===
    ext = Path(input_file).suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(input_file)
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(input_file)
    else:
        logger.error(f"❌ 不支持的文件格式: {ext}")
        return

    if "hash" not in df.columns:
        logger.error(f"❌ 输入文件中没有找到 'hash' 列！请检查文件格式。")
        return

    tx_hashes = df["hash"].dropna().astype(str).head(limit).tolist()
    if not tx_hashes:
        logger.error(f"❌ 没有找到有效的交易哈希。")
        return

    logger.info(f"✅ 读取到 {len(tx_hashes)} 条交易哈希，将批量查询。")

    # === 2️⃣ 构造批量请求 ===
    payload = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash]
        }
        for i, tx_hash in enumerate(tx_hashes)
    ]

    # === 3️⃣ 发请求并流式观测 ===
    try:
        logger.info(f"🚀 正在发送批量请求，共 {len(payload)} 个交易收据 ...")
        with httpx.stream("POST", rpc_url, json=payload, timeout=30.0) as response:
            logger.info(f"✅ 请求已发送，状态码: {response.status_code}")
            chunks = []
            for chunk in response.iter_bytes():
                logger.info(f"📦 收到 {len(chunk)} 字节数据")
                chunks.append(chunk)

            body = b"".join(chunks)

        # 解析 JSON
        try:
            results = json.loads(body.decode("utf-8"))
        except Exception as e:
            logger.error(f"❌ 响应解析失败: {e}")
            return

        # 保存文件
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 已将返回数据保存至 {output_file}")

        logger.info(f"📌 返回了 {len(results)} 条结果。")
        for item in results[:3]:  # 打印前 3 条
            txid = item.get("id")
            receipt = item.get("result")
            if receipt:
                logger.info(f"🟢 txid={txid}, blockNumber={receipt.get('blockNumber')}, status={receipt.get('status')}")
            else:
                logger.warning(f"⚠️  txid={txid}，未找到收据")

    except Exception as e:
        logger.error(f"❌ 请求出错: {e}")
        return

# ========== 示例调用 ==========
if __name__ == "__main__":
    start = time.time()

    batch_get_receipts_with_progress(
        input_file=DATA + "aribi_transaction.csv",
        rpc_url=BASE_RPC_URL,
        limit=10,  # 你可以手动改这个数测最佳 batch
        output_file=DATA + "batch_receipts_result.json"
    )

    duration = time.time() - start
    logger.info(f"⏱ 总耗时: {duration:.2f} 秒")
