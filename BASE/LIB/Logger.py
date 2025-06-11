import os
import sys
import inspect
from BASE.config import *
from datetime import datetime
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(current_dir, "../../"))
if root_dir not in sys.path:
    sys.path.append(root_dir)

class Logger:
    def __init__(self):
        self.enabled = log_enabled
        file_name = log_file_name
        os.makedirs(output_path, exist_ok=True)
        self.log_path = os.path.join(output_path, file_name)

    def _get_prefix(self):
        frame = inspect.currentframe().f_back.f_back  # 两级返回调用者
        func_name = frame.f_code.co_name
        class_name = None
        if 'self' in frame.f_locals:
            class_name = frame.f_locals['self'].__class__.__name__
        if class_name:
            return f"[{class_name}][{func_name}]"
        return f"[{func_name}]"

    def _write(self, msg: str):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")

    def info(self, message: str):
        if not self.enabled:
            return
        prefix = self._get_prefix()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{now}] {prefix}: {message}"
        print(full_msg)
        self._write(full_msg)

    def warn(self, message: str):
        if not self.enabled:
            return
        prefix = self._get_prefix()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{now}] {prefix} [WARNING]: {message}"
        print(f"\033[91m{full_msg}\033[0m")  # 红色输出到 CLI
        self._write(full_msg)

    def error(self, message: str):
        prefix = self._get_prefix()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[{now}] {prefix} [ERROR]: {message}"
        print(f"\033[91m{full_msg}\033[0m")  # 红色输出到 CLI
        self._write(full_msg)

if __name__ == "__main__":
    logger = Logger()
    logger.info("正在连接 Solana 节点")
    logger.warn("请求失败，可能是网络问题")
    logger.error("请求失败，可能是网络问题")