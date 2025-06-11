from BASE.config import *
from BASE.LIB.common import *

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


if __name__ == "__main__":
    pool_address = "0xF4DFb8647C3Ef75c5A71b7B0ee9240BdccCe8697"
    swap_abi, swap_hash = get_swap_abiandhash_by_address(pool_address)

    if swap_abi and swap_hash:
        print("✅ Swap ABI 和 topic0 获取成功")
        print(swap_abi)
        print(swap_hash)
    else:
        print("❌ 获取失败")
