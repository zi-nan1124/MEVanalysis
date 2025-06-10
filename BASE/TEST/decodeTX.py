from web3 import Web3
import json

# 1. 初始化 Web3
w3 = Web3()

# 2. 路由合约 ABI（仅需包含要 decode 的函数）
router_abi = json.loads("""[
  {
    "name": "swapExactTokensForTokens",
    "type": "function",
    "inputs": [
      {"name": "amountIn", "type": "uint256"},
      {"name": "amountOutMin", "type": "uint256"},
      {"name": "path", "type": "address[]"},
      {"name": "to", "type": "address"},
      {"name": "deadline", "type": "uint256"}
    ],
    "outputs": [{"name": "amounts", "type": "uint256[]"}],
    "stateMutability": "nonpayable"
  }
]""")

# 3. 创建合约对象
checksum_address = Web3.to_checksum_address("0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24")
contract = w3.eth.contract(address=checksum_address, abi=router_abi)

# 4. 输入的 calldata
calldata = "0x38ed173900000000000000000000000000000000000000000000018a60df94e8797eed6100000000000000000000000000000000000000000000000000000000071252a500000000000000000000000000000000000000000000000000000000000000a000000000000000000000000015af5736262d0914a0851805ce9a28e93f74ee020000000000000000000000000000000000000000000000000000000068453ae400000000000000000000000000000000000000000000000000000000000000020000000000000000000000004d5cb527bd9d87c727b6dbe01114adc086e646a2000000000000000000000000833589fcd6edb6e08f4c7c32d4f71b54bda02913"

# 5. 解码
fn_obj, params = contract.decode_function_input(calldata)
print(f"调用函数: {fn_obj.fn_name}")
print("参数：")
for k, v in params.items():
    print(f"{k}: {v}")
