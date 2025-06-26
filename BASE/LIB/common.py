import requests
import json
import time
import threading
import pandas as pd
import os
import httpx

from eth_utils import keccak
from web3 import Web3
from collections import OrderedDict
from web3._utils.events import get_event_data
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from BASE.LIB.Logger import Logger
logger = Logger()


import random
def get_random_api_key() -> str:
    api_keys = [
        "1622KYKHN7IV6ZVPWAK68SQ9H93JMT7YMF",
        "MQBVK9WGFY25HJV7VMWRY9GTJCW2WW1JAQ",
        "TDCD8SGAB98B2HBVJIXXNCI8BZT371W5NM"
    ]
    return random.choice(api_keys)
