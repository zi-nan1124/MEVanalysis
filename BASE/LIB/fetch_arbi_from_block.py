from BASE.config import *
from BASE.LIB.common import *
from BASE.LIB.DATA import *
import threading
import traceback
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time


def multi_thread_block_fetcher(start_block: int, end_block: int, rpc_urls: list, weights: list,
                               max_threads: int, chunk_size: int, output_file: str):
    blocks = list(range(start_block, end_block + 1))
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
        #logger.info(f"[{thread_name}] 启动区块段 {s_block}-{e_block}，使用 RPC: {rpc_url}")

        retry = 3
        try:
            for attempt in range(1, retry + 1):
                success, df = batch_get_blocks(rpc_url, s_block, e_block)
                if success:
                    with lock:
                        results.append(df)
                    logger.info(f"[{thread_name}] 区块段 {s_block}-{e_block} 成功，记录数: {len(df)}")
                    return
                else:
                    logger.warning(f"[{thread_name}] 第 {attempt} 次失败: 区块段 {s_block}-{e_block}")
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
            df_all = df_all[df_all["to"].str.lower().isin(valid_tos)]
        except Exception as e:
            logger.warning(f"过滤阶段异常: {e}")

        safe_append_csv(df_all, output_file, primary_key="hash")
    else:
        logger.error("所有任务均失败，没有数据保存")


def safe_append_csv(df_new: pd.DataFrame, output_file: str, primary_key: str = "hash"):
    try:
        if os.path.exists(output_file):
            df_old = pd.read_csv(output_file)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            logger.info(f"已读取原文件，原有 {len(df_old)} 条记录，新数据 {len(df_new)} 条")
        else:
            df_combined = df_new.copy()
            logger.info(f"输出文件不存在，将创建新文件，原始新数据 {len(df_new)} 条")

        # 不论是否新建，统一执行去重
        before_dedup = len(df_combined)
        df_combined.to_csv("test.csv", index=False)
        df_combined.drop_duplicates(subset=[primary_key], inplace=True)
        after_dedup = len(df_combined)


        if os.path.exists(output_file):
            old_dedup = len(df_old)
            logger.info(f"新增 {after_dedup - old_dedup} 条记录（去除 {before_dedup - after_dedup} 条重复）")
        else:
            logger.info(f"新增 {after_dedup} 条记录（去除 {before_dedup - after_dedup} 条重复）")


        df_combined.to_csv(output_file, index=False)
        logger.info(f"已保存到 {output_file}，共 {after_dedup} 条记录")
    except Exception as e:
        logger.error(f"写入文件 {output_file} 失败: {e}")



if __name__ == "__main__":
    START_BLOCK = START_BLOCK
    END_BLOCK = START_BLOCK + 1000
    RPC_URLS = BASE_RPC_URL_LIST
    WEIGHTS = [0, 1]
    MAX_THREADS = 10
    CHUNK_SIZE = 200
    OUTPUT_FILE = DATA + "fetch_arbi_from_block.csv"

    start_time = time.time()

    multi_thread_block_fetcher(START_BLOCK, END_BLOCK, RPC_URLS, WEIGHTS, MAX_THREADS, CHUNK_SIZE, OUTPUT_FILE)

    logger.info(f"总耗时: {time.time() - start_time:.2f} 秒")
