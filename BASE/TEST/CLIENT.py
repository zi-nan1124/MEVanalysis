import asyncio
import httpx
import pandas as pd
import time
from web3 import Web3
from web3.providers.base import JSONBaseProvider
from web3.types import RPCEndpoint, RPCResponse

from BASE.config import *
from BASE.LIB.common import logger


class MyClient(JSONBaseProvider):
    def __init__(self, rpc_url: str, max_concurrent: int = 10, http2: bool = True, timeout: float = 10.0):
        super().__init__()
        self.endpoint_uri = rpc_url
        self.client = httpx.AsyncClient(http2=http2, timeout=timeout)
        self.max_concurrent = max_concurrent
        self.w3 = Web3(self)

    async def make_request(self, method: RPCEndpoint, params: any) -> RPCResponse:
        req_data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        try:
            response = await self.client.post(
                self.endpoint_uri,
                json=req_data,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            try:
                return response.json()
            except Exception:
                logger.error(f"[AsyncReceiptFetcher][make_request] ⚠️ JSON decode failed. Raw: {response.content[:200]!r}")
                return {
                    "error": "JSON decode failed",
                    "raw": response.content.decode('utf-8', errors='ignore')
                }
        except Exception as e:
            logger.error(f"[AsyncReceiptFetcher][make_request] ❌ RPC request failed: {e}")
            return {"error": str(e)}

    async def fetch_receipt(self, tx_hash: str):
        logger.info(f"[AsyncReceiptFetcher] Fetching receipt: {tx_hash}")
        return await self.make_request("eth_getTransactionReceipt", [tx_hash])

    async def fetch_receipts(self, tx_hashes):
        sem = asyncio.Semaphore(self.max_concurrent)

        async def sem_task(tx_hash):
            async with sem:
                return await self.fetch_receipt(tx_hash)

        tasks = [sem_task(tx) for tx in tx_hashes]
        results = await asyncio.gather(*tasks)
        return results

    async def close(self):
        await self.client.aclose()


# === 类外调用 / 测试 ===
async def test_from_csv(csv_path: str, rpc_url: str, limit: int = 100, max_concurrent: int = 10):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"❌ CSV 读取失败: {e}")
        return

    if "hash" not in df.columns:
        logger.error("❌ CSV 文件中未找到 'hash' 列")
        return

    tx_hashes = df["hash"].dropna().astype(str).head(limit).tolist()
    logger.info(f"✅ 从文件读取了 {len(tx_hashes)} 条交易哈希")

    fetcher = MyClient(rpc_url, max_concurrent=max_concurrent)

    start = time.time()
    logger.info(f"🚀 开始查询收据，最大并发: {max_concurrent}")

    receipts = await fetcher.fetch_receipts(tx_hashes)

    elapsed = time.time() - start
    logger.info(f"✅ 查询完成，用时 {elapsed:.2f} 秒")

    for r in receipts[:3]:
        if "result" in r:
            tx_hash = r["result"].get("transactionHash")
            status = r["result"].get("status")
            logger.info(f"TxHash: {tx_hash}, Status: {status}")
        else:
            raw_preview = (r.get("raw") or "")[:100]
            logger.error(f"Error: {r.get('error')}, Raw: {raw_preview}")

    await fetcher.close()


if __name__ == "__main__":
    CSV_PATH = DATA + "aribi_transaction.csv"
    RPC_URL = BASE_RPC_URL
    MAX_CONCURRENT = 20

    asyncio.run(test_from_csv(CSV_PATH, RPC_URL, limit=1000, max_concurrent=MAX_CONCURRENT))
