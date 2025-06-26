from BASE.config import *
from BASE.LIB.common import *
from collections import OrderedDict
import threading
import csv
import os
import sys


class LRUCache:
    def __init__(self):
        self.capacity = 3
        self.cache = OrderedDict()
        self.lock = threading.Lock()
        self.cache_file = DATA + LRU_CACHE_CSV
        self.raw_file = DATA + FILTER
        self._load_raw_file()
        self._load_cache()

    def _load_raw_file(self):
        if not os.path.exists(self.raw_file):
            logger.error(f"原始数据文件不存在: {self.raw_file}")
            sys.exit(1)
        with open(self.raw_file, "r") as f:
            lines = set(line.strip() for line in f if line.strip())
        if not lines:
            logger.error(f"原始数据文件为空: {self.raw_file}")
            sys.exit(1)
        self.raw_file = lines
        logger.infoX(f"原始数据加载完成，共 {len(self.raw_file)} 条记录")

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if row:
                        self.cache[row[0]] = None
            logger.infoX(f"缓存文件加载完成，共 {len(self.cache)} 条记录")
        else:
            open(self.cache_file, "w").close()
            logger.infoX(f"缓存文件不存在，已创建空文件: {self.cache_file}")

    def _save_cache(self):
        with open(self.cache_file, "w", newline="") as f:
            writer = csv.writer(f)
            for key in self.cache.keys():
                writer.writerow([key])
        logger.infoX(f"缓存文件已保存，共 {len(self.cache)} 条记录")

    def get(self, key):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return True
            return False

    def put(self, key):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                if len(self.cache) >= self.capacity:
                    evicted_key, _ = self.cache.popitem(last=False)
                    logger.infoX(f"缓存已满，移除最旧记录: {evicted_key}")
                self.cache[key] = None
            self._save_cache()

    def query_tx(self, tx):
        if self.get(tx):
            logger.infoX(f"命中缓存: {tx}")
            return True
        elif tx in self.raw_file:
            logger.infoX(f"原始数据中找到记录，加入缓存: {tx}")
            self.put(tx)
            return True
        else:
            logger.infoX(f"未找到记录: {tx}")
            return False

    def save_and_close(self):
        self._save_cache()
        logger.infoX("LRUCache 已保存缓存文件 (显式调用)")


def test_query(cache):
    tx_list = [
        "0xe9bfdb2ad24db07ec9c2fbb412f7924b0ae79051",  # A
        "0x948d3119c4db00826aa4eec635efdd9fa94a2364",  # B
        "0xcb47fb64e79125288a810b11348ec312f87510af",  # C
        "0xe9bfdb2ad24db07ec9c2fbb412f7924b0ae79051",  # A hit
        "0x0066a3cb31c1e4ac620a0098e21546e4f2835a6e",  # D
        "0x71303fe3d74482faf35430a6b68b18129f798a5a",  # E
        "0xe9bfdb2ad24db07ec9c2fbb412f7924b0ae79051",  # A hit
        "0x1111111111111111111111111111111111111111"   # X not in original data
    ]
    for tx in tx_list:
        cache.query_tx(tx)


if __name__ == "__main__":
    cache = LRUCache()
    test_query(cache)
    cache.save_and_close()
