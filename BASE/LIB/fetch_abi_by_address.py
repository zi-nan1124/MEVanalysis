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


# 示例使用方式
if __name__ == "__main__":
    logger = Logger()
    logger.info("正在连接 Solana 节点")  # 示例用法
    abi = fetch_abi_by_address("0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d")
    if abi:
        print("ABI.csv 获取成功，共包含函数/事件数量：", len(abi))
    else:
        logger.warn("ABI.csv 获取失败")
