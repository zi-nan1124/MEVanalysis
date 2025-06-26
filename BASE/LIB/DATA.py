from BASE.config import *
from BASE.LIB.common import *

def fetch_transactions_from_address(
    address: str,
    api_key: str,
    chainid: int = BASE_CHAIN_ID,
    offset: int = 1000,
    sort: str = "asc",
    sleep_sec: float = 0.01,
    startblock: int = START_BLOCK,
    endblock: int = END_BLOCK,
) -> pd.DataFrame:
    """
    按区块范围轮询获取交易数据。
    """

    all_txs = []
    current_startblock = startblock

    try:
        while True:
            url = (
                f"https://api.etherscan.io/v2/api"
                f"?chainid={chainid}"
                f"&module=account"
                f"&action=txlist"
                f"&address={address}"
                f"&startblock={current_startblock}"
                f"&endblock={endblock}"
                f"&page=1"
                f"&offset={offset}"
                f"&sort={sort}"
                f"&apikey={api_key}"
            )

            logger.info(f"{url}:请求区块范围: {current_startblock} - {endblock}")
            response = requests.get(url)

            if response.status_code != 200:
                logger.error(f"HTTP 错误: {response.status_code}")
                break

            data = response.json()
            if data.get("status") != "1" or not data.get("result"):
                logger.warn(f"请求结束或无数据: {data.get('message', 'No more data')}")
                break

            result_txs = data["result"]
            all_txs.extend(result_txs)
            logger.info(f"累计获取 {len(all_txs)} 条原始交易数据")

            # 计算下一轮 startblock，不能超过 endblock
            max_block = max(int(tx["blockNumber"]) for tx in result_txs)
            next_startblock = max_block + 1

            if next_startblock > endblock:
                logger.info(f"已到达给定 endblock: {endblock}，结束轮询。")
                break

            current_startblock = next_startblock

            # 判断是否结束分页（数据已不足 offset）
            if len(result_txs) < offset:
                logger.info("返回数据已不足 offset，结束轮询。")
                break

            time.sleep(sleep_sec)

    except requests.RequestException as e:
        logger.error(f"请求异常: {e}")
    except Exception as e:
        logger.error(f"运行异常: {e}")

    if not all_txs:
        logger.warn("没有获取到任何交易数据")
        return pd.DataFrame()

    # 转 DataFrame 并去重
    df = pd.DataFrame(all_txs)
    df = df.drop_duplicates(subset="hash")

    # 过滤 to != address 的行
    df = df[df["to"].str.lower() == address.lower()]

    logger.info(f"最终去重后交易数: {len(df)}")
    return df



def fetch_arbi_from_filter(input_file=DATA+FILTER, thread_count=15):
    output_file = os.path.join(DATA, "aribi_transaction.csv")
    if input_file is None:
        input_file = os.path.join(DATA, FILTER)

    lock = threading.Lock()

    # 读取输入文件
    if not os.path.exists(input_file):
        logger.error(f"输入文件不存在: {input_file}")
        return

    address_df = pd.read_csv(input_file)

    # 如果没有 status 列，新增并初始化为 0
    if 'status' not in address_df.columns:
        address_df['status'] = 0

    # 只处理 status == 0 的行
    pending_df = address_df[address_df['status'] == 0]

    if pending_df.empty:
        logger.info("所有地址均已处理，无需重复抓取。")
        return

    # 如果已有历史数据，读取
    if os.path.exists(output_file):
        all_df = pd.read_csv(output_file)
    else:
        all_df = pd.DataFrame()

    def worker(address, idx):
        try:
            df = fetch_transactions_from_address(address, BASE_SCAN_API_KEY())

            if df is None or df.empty:
                logger.error(f"{address}: 查询失败或返回空数据，不更新状态")
                return

            # 正常写入
            with lock:
                combined_df = pd.concat([all_df, df], ignore_index=True)
                combined_df.drop_duplicates(subset=["hash"], inplace=True)
                combined_df.to_csv(output_file, index=False)
                logger.info(f"{address}: 数据写入成功，当前总行数: {len(combined_df)}")

                # 成功写入后，更新 status
                address_df.loc[idx, 'status'] = 1
                address_df.to_csv(input_file, index=False)
                logger.info(f"{address}: 状态已更新为已处理")

        except Exception as e:
            logger.error(f"{address}: 抓取过程中发生异常: {e}")
            # 不更新 status，留给后续轮次继续尝试

    threads = []
    for idx, row in pending_df.iterrows():
        address = row['tx_to']
        t = threading.Thread(target=worker, args=(address, idx))
        threads.append(t)
        t.start()

        if len(threads) >= thread_count:
            for t in threads:
                t.join()
            threads = []

    # 收尾
    for t in threads:
        t.join()

    logger.info("所有地址交易数据抓取完成")

import httpx
import pandas as pd
from web3 import Web3
from BASE.LIB.common import logger

def batch_get_blocks(rpc_url: str, start_block: int, end_block: int) -> (bool, pd.DataFrame):
    payload = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), True]
        }
        for i, block_number in enumerate(range(start_block, end_block + 1))
    ]

    logger.info(f"{rpc_url} 开始批量请求区块 {start_block} 到 {end_block}")

    try:
        response = httpx.post(rpc_url, json=payload, timeout=30.0)
        response.raise_for_status()
        results = response.json()
        logger.info(f"{start_block}-{end_block}: 请求成功，返回 {len(results)} 条区块数据")
    except httpx.HTTPError as e:
        logger.error(f"HTTP 请求出错: {e}")
        return False, pd.DataFrame()
    except Exception as e:
        logger.error(f"其他异常: {e}")
        return False, pd.DataFrame()

    all_txs = []
    w3 = Web3()  # 用于数据转换（例如 from_wei）

    for item in results:
        block = item.get("result")
        if not block:
            logger.warn(f"区块数据缺失，id={item.get('id')}")
            continue

        for tx in block["transactions"]:
            tx_data = {
                "blockNumber": int(tx["blockNumber"], 16),
                "blockHash": tx["blockHash"],
                "hash": tx["hash"],
                "from": tx["from"],
                "to": tx["to"],
                "value_ether": w3.from_wei(int(tx["value"], 16), "ether"),
                "gas": int(tx["gas"], 16),
                "gasPrice_gwei": w3.from_wei(int(tx.get("gasPrice", "0x0"), 16), "gwei"),
                "maxFeePerGas_gwei": w3.from_wei(int(tx.get("maxFeePerGas", "0x0"), 16), "gwei") if "maxFeePerGas" in tx else None,
                "maxPriorityFeePerGas_gwei": w3.from_wei(int(tx.get("maxPriorityFeePerGas", "0x0"), 16), "gwei") if "maxPriorityFeePerGas" in tx else None,
                "nonce": int(tx["nonce"], 16),
                "type": int(tx["type"], 16) if "type" in tx else None,
                "transactionIndex": int(tx["transactionIndex"], 16),
                "input": tx["input"],
                "v": int(tx.get("v", "0x0"), 16) if tx.get("v") else None,
                "r": tx.get("r"),
                "s": tx.get("s")
            }
            all_txs.append(tx_data)

    df = pd.DataFrame(all_txs)
    logger.info(f"{start_block}-{end_block}最终生成 DataFrame，共 {len(df)} 条交易记录")
    return True, df


def batch_get_receipts(rpc_url: str, df: pd.DataFrame) -> (bool, pd.DataFrame):
    if "hash" not in df.columns:
        logger.error("输入 DataFrame 缺少 'hash' 列")
        return False, pd.DataFrame(columns=["hash", "status"])

    tx_hashes = df["hash"].dropna().astype(str).tolist()
    if not tx_hashes:
        logger.warning("没有有效的交易哈希可查询")
        return False, pd.DataFrame(columns=["hash", "status"])

    logger.info(f"{rpc_url} 开始批量请求，共 {len(tx_hashes)} 条交易哈希")

    payload = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_getTransactionReceipt",
            "params": [tx_hash]
        }
        for i, tx_hash in enumerate(tx_hashes)
    ]

    try:
        response = httpx.post(rpc_url, json=payload, timeout=30.0)
        response.raise_for_status()
        results = response.json()
        logger.info(f"请求成功，返回 {len(results)} 条数据")

    except httpx.HTTPError as e:
        logger.error(f"HTTP 请求出错: {e}")
        return False, pd.DataFrame(columns=["hash", "status"])
    except Exception as e:
        logger.error(f"其他异常: {e}")
        return False, pd.DataFrame(columns=["hash", "status"])

    simplified = []
    for item in results:
        receipt = item.get("result")
        if receipt:
            simplified.append({
                "hash": receipt.get("transactionHash"),
                "status": receipt.get("status")
            })
        else:
            logger.warning(f"收据缺失，txid={item.get('id')}")
            simplified.append({
                "hash": None,
                "status": None
            })

    df_out = pd.DataFrame(simplified)
    logger.info(f"生成 DataFrame，共 {len(df_out)} 条记录")
    return True, df_out



#-----------------------------------------------------------------------------------------------------------------------------TEST--------------------------------------------------------------------------------------------------------------------------------------------------------------------
def test_fetch_transactions():
    """
    测试 fetch_transactions_from_address 函数：
    - 拉取交易数据
    - 验证 DataFrame 正确性
    - 保存到文件
    - 打印样本数据
    """
    # 示例参数
    address = "0x80009f3b0c60edaa2dcec6ddac9d92455de922a2"
    output_file = f"{address}.csv"
    output_file = DATA + output_file

    logger.info("开始测试 fetch_transactions_from_address")

    # 调用主函数
    df = fetch_transactions_from_address(
        address=address,
        api_key=BASE_SCAN_API_KEY(),
    )

    # 检查结果
    if df.empty:
        logger.warning("测试结果为空，没有拉取到交易数据")
    else:
        logger.info(f"成功获取 {len(df)} 条交易记录")

        # 保存文件
        df.to_csv(output_file, index=False)
        logger.info(f" 测试数据已保存到 {output_file}")

    logger.info(" 测试完成")

def test_fetch_arbi_from_filter():
    # 读取前100地址
    fetch_arbi_from_filter()

def test_batch_get_blocks():
    success, df_blocks = batch_get_blocks(BASE_RPC_URL, START_BLOCK, START_BLOCK+100)

    if success:
        logger.info("批量区块请求成功")
        print(df_blocks.head())
    else:
        logger.error("批量区块请求失败")


def test_batch_get_receipts():
    df_input = pd.read_csv(DATA + "aribi_transaction.csv")
    success, df_result = batch_get_receipts(BASE_RPC_URL, df_input.head(10))
    if success:
        logger.info("批量请求成功，可以进一步处理数据")
        print(df_result)
    else:
        logger.error("批量请求失败，请检查网络或 RPC 节点状态")


if __name__ == "__main__":
    import time
    start_time = time.time()

    # test_fetch_transactions()
    # test_fetch_arbi_from_filter()
    test_batch_get_blocks()
    # test_batch_get_receipts()

    duration = time.time() - start_time
    logger.info(f"总耗时: {duration:.2f} 秒")


