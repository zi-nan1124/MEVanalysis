from web3 import Web3

# ====== 1. 设置 Base RPC 与交易哈希 ======
RPC_URL = "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

# 手动设置交易哈希（不要 input）
tx_hash = "0xe270542a513ec784f77bb7565e1a65ef03bb2cd78cfdc504204880c0c4703177"

# ====== 2. 获取 Transaction Receipt ======
try:
    receipt = w3.eth.get_transaction_receipt(tx_hash)
except Exception as e:
    print(f"❌ 获取交易收据失败: {e}")
    exit(1)

logs = receipt.get("logs", [])

# ====== 3. 打印日志内容 ======
if not logs:
    print("没有日志")
else:
    print(f"交易日志共 {len(logs)} 条：\n")

    for idx, log in enumerate(logs):
        print(f"   Log #{idx + 1}")
        print(f"  - 合约地址     : {log['address']}")
        print(f"  - topics       :")
        for i, topic in enumerate(log['topics']):
            print(f"      [{i}] {topic}")
        print(f"  - data         : {log['data']}")
        print(f"  - logIndex     : {log.get('logIndex')}")
        print(f"  - blockNumber  : {log.get('blockNumber')}")
        print(f"  - txIndex      : {log.get('transactionIndex')}")
        print("")
