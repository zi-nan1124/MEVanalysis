BASE_CHAIN_ID = 8453
#BASE_RPC_URL = "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"
BASE_RPC_URL = "https://light-restless-log.base-mainnet.quiknode.pro/bd5cbe29a6f7c4a1fdceabc1fdfa443c2db51247"
BASE_RPC_URL_LIST = [
    "https://light-restless-log.base-mainnet.quiknode.pro/bd5cbe29a6f7c4a1fdceabc1fdfa443c2db51247",
    "https://wispy-wider-frog.base-mainnet.quiknode.pro/716ecf61c5c7d36792453dc1f58bfebbec14ec1a/"
]
DUNE_API_KEY = "ZG2PGNnEjNOLQQvHeBNvLp23Q3anZDvc"

log_enabled = True
output_path = "../DATA"
DATA = "../DATA/"
log_file_name = "log.txt"

FILTER = "filter202501-202502.csv"

LRU_CACHE_SIZE = 100
LRU_CACHE_CSV = "LRU_CACHE.csv"

from secrets import choice
def BASE_SCAN_API_KEY():
    api_keys = [
        "1622KYKHN7IV6ZVPWAK68SQ9H93JMT7YMF",
        "MQBVK9WGFY25HJV7VMWRY9GTJCW2WW1JAQ",
        "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM",
        "2PD7HUKTPJVZE4AZW6M6ECGNSAWMX5KT9C",
        "89MJ5EAXFJ1AMFP18MIGRD1TTCSEV2PU85",
        "ZGH8WV5B2EV1VG382BJ3CKFQFH4RFZR3SS",
        "BJ3446K621AECIYKDPFU1HXJ629NUU6Z7N",
    ]
    return choice(api_keys)

START_BLOCK = 24488284
END_BLOCK = 25815677