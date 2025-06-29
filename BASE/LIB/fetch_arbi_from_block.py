import BASE.config as config
from BASE.LIB.common import *
from BASE.LIB.DATA import *
import threading
import traceback
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import math


def multi_thread_block_fetcher(start_block: int, end_block: int, rpc_urls: list, weights: list,
                               max_threads: int, chunk_size: int,) -> pd.DataFrame:
    blocks = list(range(start_block, end_block))
    chunks = [blocks[i:i + chunk_size] for i in range(0, len(blocks), chunk_size)]
    logger.info(f"任务总分为 {len(chunks)} 个区块段，每段最多 {chunk_size} 个区块")

    flat_urls = []
    for url, weight in zip(rpc_urls, weights):
        flat_urls.extend([url] * weight)
    rpc_count = len(flat_urls)

    results = []
    lock = threading.Lock()

    def worker(chunk_index, block_range, rpc_url):
        s_block = block_range[0]
        e_block = block_range[-1]
        thread_name = threading.current_thread().name

        retry = 10
        try:
            for attempt in range(1, retry + 1):
                success, df = batch_get_blocks(rpc_url, s_block, e_block)
                if success:
                    with lock:
                        results.append(df)
                    logger.info(f"[{thread_name}] 区块段 {s_block}-{e_block} 成功，记录数: {len(df)}")
                    return
                else:
                    logger.warn(f"[{thread_name}] 第 {attempt} 次失败: 区块段 {s_block}-{e_block}")
            logger.error(f"[{thread_name}] 最终失败: 区块段 {s_block}-{e_block}")
        except Exception as e:
            logger.error(f"[{thread_name}] 线程异常: {e}")
            logger.error(traceback.format_exc())

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            rpc_url = flat_urls[i % rpc_count]
            futures.append(executor.submit(worker, i, chunk, rpc_url))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"主线程捕获子线程异常: {e}")
                logger.error(traceback.format_exc())

    if results:
        df_all = pd.concat(results, ignore_index=True)
        logger.info(f"所有线程合并完成，共 {len(df_all)} 条记录")

        try:
            df_filter = pd.read_csv(DATA + FILTER)
            valid_tos = set(df_filter["tx_to"].dropna().str.lower())
            df_all = df_all[df_all["to"].str.lower().isin(valid_tos)].copy()
        except Exception as e:
            logger.warn(f"过滤阶段异常: {e}")

        before_dedup = len(df_all)
        df_all.drop_duplicates(subset=["hash"], inplace=True)
        after_dedup = len(df_all)
        logger.info(f"最终去重后记录数: {after_dedup}（去除 {before_dedup - after_dedup} 条重复）")

        return df_all.loc[:, ["blockNumber", "hash"]]
    else:
        logger.error("所有任务均失败，没有数据返回")
        return pd.DataFrame(columns=["blockNumber", "hash"])




def split_df_evenly(df: pd.DataFrame, num_chunks: int) -> list[pd.DataFrame]:
    """将 df 均分为 num_chunks 份"""
    n = len(df)
    chunk_size = math.ceil(n / num_chunks)
    return [df.iloc[i:i + chunk_size].copy() for i in range(0, n, chunk_size)]

def expand_rpc_urls_by_weight(rpc_urls: list, weights: list, total_chunks: int) -> list:
    """根据权重生成长度为 total_chunks 的 RPC URL 列表"""
    flat_urls = []
    for url, weight in zip(rpc_urls, weights):
        flat_urls.extend([url] * weight)

    if len(flat_urls) < total_chunks:
        full_urls = (flat_urls * (total_chunks // len(flat_urls) + 1))[:total_chunks]
    else:
        full_urls = flat_urls[:total_chunks]

    return full_urls

def multi_thread_fetch_with_receipts(start_block: int, end_block: int, rpc_urls: list, weights: list,
                                     max_threads: int, chunk_size: int) -> pd.DataFrame:
    # 第一步：并发获取区块数据，只保留 blockNumber 和 hash
    df_block = multi_thread_block_fetcher(start_block, end_block, rpc_urls, weights, max_threads, chunk_size)
    if df_block.empty:
        logger.error("区块抓取阶段无数据，终止后续流程")
        return pd.DataFrame(columns=["blockNumber", "hash", "status"])

    # 第二步：将 df_block 分为 max_threads 份
    chunks = split_df_evenly(df_block, max_threads)
    logger.info(f"{'-'*120}")
    logger.info(f"[multi_thread_fetch_with_receipts]: 收据任务共分为 {len(chunks)} 段，每段约 {len(chunks[0])} 条交易")

    # 构造加权分配的 RPC URL 列表
    rpc_assignments = expand_rpc_urls_by_weight(rpc_urls, weights, len(chunks))

    results = []
    lock = threading.Lock()

    def receipt_worker(index, df_chunk, rpc_url):
        thread_name = threading.current_thread().name
        retry = 10
        try:
            for attempt in range(1, retry + 1):
                success, df_receipts = batch_get_receipts(rpc_url, df_chunk)
                if success:
                    df_receipts["blockNumber"] = df_chunk["blockNumber"].values
                    with lock:
                        results.append(df_receipts)
                    logger.info(f"[{thread_name}] 成功处理第 {index} 段，共 {len(df_receipts)} 条")
                    return
                else:
                    logger.warn(f"[{thread_name}] 第 {attempt} 次失败: 第 {index} 段")
            logger.error(f"[{thread_name}] 最终失败: 第 {index} 段")
        except Exception as e:
            logger.error(f"[{thread_name}] 线程异常: {e}")
            logger.error(traceback.format_exc())

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for i, chunk_df in enumerate(chunks):
            rpc_url = rpc_assignments[i]
            futures.append(executor.submit(receipt_worker, i, chunk_df, rpc_url))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"主线程捕获收据子线程异常: {e}")
                logger.error(traceback.format_exc())

    if results:
        df_all = pd.concat(results, ignore_index=True)
        df_all.drop_duplicates(subset=["hash"], inplace=True)
        logger.info(f"最终收据表格合并完成，共 {len(df_all)} 条记录")
        return df_all.loc[:, ["blockNumber", "hash", "status"]]
    else:
        logger.error("所有收据任务均失败，没有结果返回")
        return pd.DataFrame(columns=["blockNumber", "hash", "status"])


def fetch_and_record_failures(start_block: int, end_block: int):
    RPC_URLS = config.BASE_RPC_URL_LIST
    WEIGHTS = config.WEIGHTS
    MAX_THREADS = config.MAX_THREADS
    CHUNK_SIZE = (end_block-start_block)//MAX_THREADS
    OUTPUT_SUMMARY_FILE = config.DATA + "fail_aribi_orderby_blocknumber.csv"

    start_time = time.time()
    logger.info(f"开始处理区块段: {start_block} - {end_block}")

    df_receipt = multi_thread_fetch_with_receipts(start_block, end_block, RPC_URLS, WEIGHTS, MAX_THREADS, CHUNK_SIZE)

    fail_arbi_number = (df_receipt["status"] == "0x0").sum()
    logger.info(f"失败交易数量: {fail_arbi_number}")

    record = {
        "start_block": start_block,
        "end_block": end_block,
        "fail_arbi_number": fail_arbi_number
    }

    try:
        if os.path.exists(OUTPUT_SUMMARY_FILE):
            df_existing = pd.read_csv(OUTPUT_SUMMARY_FILE)
            if start_block in df_existing["start_block"].values:
                logger.warn(f"记录已存在: start_block = {start_block}，跳过写入")
                return
            df_new = pd.DataFrame([record])
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_csv(OUTPUT_SUMMARY_FILE, index=False)
            logger.info(f"追加写入成功，总记录数: {len(df_combined)}")
        else:
            df_new = pd.DataFrame([record])
            df_new.to_csv(OUTPUT_SUMMARY_FILE, index=False)
            logger.info(f"已创建 summary 文件并写入首条记录")

    except Exception as e:
        logger.error(f"写入 summary 文件失败: {e}")

    logger.info(f"区块段 {start_block} - {end_block} 总耗时: {time.time() - start_time:.2f} 秒")



def get_completed_blocks(summary_path: str) -> set:
    if not os.path.exists(summary_path):
        return set()
    try:
        df = pd.read_csv(summary_path)
        df.columns = df.columns.str.strip()  # 防止列名有空格
        if "start_block" not in df.columns:
            logger.warn("❌ summary 文件中找不到 start_block 列")
            return set()

        # 强制转换为 int，防止类型不一致
        completed = set(int(x) for x in df["start_block"].dropna().unique())

        logger.info(f"[get_completed_blocks] 已加载 {len(completed)} 个已完成 start_block")
        return completed
    except Exception as e:
        logger.warn(f"⚠️ 读取 summary 文件失败，默认视为空: {e}")
        return set()



from queue import Queue

def dynamic_thread_dispatcher(start: int, end: int, step: int, max_workers: int, summary_path: str):
    all_tasks = [(i, i + step) for i in range(start, end, step) if i + step <= end]
    completed_starts = get_completed_blocks(summary_path)
    remaining_tasks = [task for task in all_tasks if task[0] not in completed_starts]

    logger.info(f"总任务 {len(all_tasks)}，已完成 {len(completed_starts)}，剩余 {len(remaining_tasks)}")
    task_queue = Queue()
    for task in remaining_tasks:
        task_queue.put(task)

    def worker():
        while not task_queue.empty():
            try:
                s, e = task_queue.get_nowait()
                fetch_and_record_failures(s, e)
                task_queue.task_done()
            except Exception as ex:
                logger.error(f"任务执行异常: {ex}")
                task_queue.task_done()

    threads = []
    for _ in range(max_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()



from concurrent.futures import ThreadPoolExecutor, as_completed

if __name__ == "__main__":
    if __name__ == "__main__":
        START = START_BLOCK
        END = END_BLOCK
        STEP = 1000
        MAX_WORKERS = 10
        SUMMARY_FILE = DATA + "fail_aribi_orderby_blocknumber.csv"

        dynamic_thread_dispatcher(START, END, STEP, MAX_WORKERS, SUMMARY_FILE)




