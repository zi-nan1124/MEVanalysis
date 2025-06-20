from BASE.config import *
from BASE.LIB.common import *

def fetch_abi_by_address(address: str) -> dict:
    """
    从 Etherscan 获取指定地址的 ABI.csv。

    参数:
    - address: 合约地址

    返回:
    - 解析后的 ABI.csv JSON（Python dict 列表），失败则返回空列表
    """
    url = f"https://api.etherscan.io/v2/api?chainid=8453&module=contract&action=getabi&address={address}&apikey={BASE_SCAN_API_KEY}"

    try:
        logger.info(f"正在获取 ABI.csv: {address}")
        response = requests.get(url)
        if response.status_code != 200:
            logger.warn(f"HTTP 状态异常: {response.status_code}")
            return []
        result = response.json()
        if result.get("status") != "1":
            logger.warn(f"接口返回异常: {result.get('message', 'Unknown error')}")
            return []
        abi = json.loads(result["result"])
        logger.info(f"成功获取 ABI.csv: {address}")
        return abi
    except requests.exceptions.RequestException as e:
        logger.error(f"请求失败: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"ABI.csv 解析失败: {e}")
    except Exception as e:
        logger.error(f"未知错误: {e}")
    return []

def get_swap_abiandhash_by_address(pool_address: str) -> tuple:
    """
    获取指定合约中的第一个 Swap 事件 ABI 及其 topic0 哈希

    参数:
    - pool_address: 合约地址

    返回:
    - (swap_event_abi, topic0_hash) 如果找不到则返回 (None, None)
    """
    abi = fetch_abi_by_address(pool_address)
    if not abi:
        logger.warn("ABI 获取失败")
        return None, None

    swap_events = [item for item in abi if item.get("type") == "event" and item.get("name") == "Swap"]
    if not swap_events:
        logger.warn(f"{pool_address} 中未找到名为 Swap 的事件")
        return None, None

    swap_event = swap_events[0]
    inputs = swap_event["inputs"]
    signature = f'Swap({",".join(inp["type"] for inp in inputs)})'
    event_hash = keccak(text=signature).hex()

    logger.info(f"找到 Swap 事件签名: {signature}")
    logger.info(f"事件哈希: 0x{event_hash}")
    return swap_events, f"0x{event_hash}"

def test_fetch_abi_by_address():
    logger.info("===== 开始测试函数: test_fetch_abi_by_address =====")
    abi = fetch_abi_by_address("0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d")
    if abi:
        logger.info(f"ABI.csv 获取成功，共包含函数/事件数量：{len(abi)}")
    else:
        logger.warning("ABI.csv 获取失败")

def test_get_swap_abiandhash_by_address():
    logger.info("===== 开始测试函数: test_get_swap_abiandhash_by_address =====")
    pool_address = "0xF4DFb8647C3Ef75c5A71b7B0ee9240BdccCe8697"
    swap_abi, swap_hash = get_swap_abiandhash_by_address(pool_address)

    if swap_abi and swap_hash:
        logger.info("✅ Swap ABI 和 topic0 获取成功")
        logger.info(f"Swap ABI: {swap_abi}")
        logger.info(f"Swap topic0: {swap_hash}")
    else:
        logger.warning("❌ 获取失败")



if __name__ == "__main__":
    logger.info("========== 测试开始 ==========")
    logger.info("===== 开始测试函数: test_fetch_abi_by_address =====")
    test_fetch_abi_by_address()
    logger.info("===== 开始测试函数: test_get_swap_abiandhash_by_address =====")
    test_get_swap_abiandhash_by_address()
    logger.info("=========== 测试结束 ===========")
