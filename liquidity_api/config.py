"""
Configuration for Liquidity API
"""
import os
from pathlib import Path
from typing import Dict, List
from web3 import Web3

class Config:
    # Project root directory
    PROJECT_ROOT = Path(__file__).parent
    
    # RPC Configuration - Free public endpoints
    ETH_RPC_URLS = [
        "https://ethereum-rpc.publicnode.com",
        "https://rpc.ankr.com/eth",
        "https://eth.drpc.org",
        "https://ethereum.blockpi.network/v1/rpc/public",
        "https://eth-mainnet.public.blastapi.io",
    ]
    ETH_RPC_URL = os.getenv("ETH_RPC_URL", ETH_RPC_URLS[0])
    
    # Uniswap V2 Contracts - Mainnet
    UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    
    # Chain Configuration
    CHAIN_ID = 1  # Ethereum Mainnet
    
    # Database Configuration (SQLite)
    DATA_DIR = PROJECT_ROOT / "data"
    DATABASE_PATH = str(DATA_DIR / "liquidity.db")
    
    # API Configuration
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))
    
    # Event Polling Configuration
    POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "12"))  # seconds
    BLOCK_BATCH_SIZE = int(os.getenv("BLOCK_BATCH_SIZE", "50"))  # blocks per batch
    START_BLOCK = os.getenv("START_BLOCK", "latest")  # or specific block number
    
    # Threading Configuration
    MIN_THREADS = int(os.getenv("MIN_THREADS", "2"))
    MAX_THREADS = int(os.getenv("MAX_THREADS", "50"))
    THREAD_SCALE_FACTOR = float(os.getenv("THREAD_SCALE_FACTOR", "0.1"))  # 10 pools per thread
    
    # Popular Uniswap V2 Pairs to monitor
    def to_checksum_addresses(addresses):
        """Convert addresses to proper EIP-55 checksum format"""
        return [Web3.to_checksum_address(addr) for addr in addresses]

    # Use lowercase addresses (easier to manage)
    _RAW_PAIRS = [
        "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",  # USDC/WETH
        "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",  # WETH/USDT  
        "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",  # DAI/WETH
        "0x3041cbd36888becc7bbcbc0045e3b1f144466f5f",  # USDC/USDT
        "0xae461ca67b15dc8dc81ce7615e0320da1a9ab8d5",  # DAI/USDC  
        "0xbb2b8038a1640196fbe3e38816f3e67cba72d940",  # WBTC/WETH
        "0x4585fe77225b41b697c938b018e2ac67ac5a20c0",  # WBTC/WETH (alt)
        "0x231b7589426ffe1b75405526fc32ac09d44364c4",  # WETH/PEPE
        "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2",  # WETH/MKR 
        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI/WETH
        "0x94b0a3d511b6ecdb17ebf877278ab030acb0a878",  # WETH/FEI 
        "0x109830a1aaad605bbf02a9dfa7b0b92ec2fb7daa",  # WETH/SHIB
    ]

    MONITORED_PAIRS = to_checksum_addresses(_RAW_PAIRS)

    
    # Token metadata for price calculations
    TOKENS = {
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": {
            "symbol": "WETH", "decimals": 18, "name": "Wrapped Ether"
        },
        "0xA0b86a33E6B9e8C3873bFc5c5E3C9dE3CeDce1a5": {
            "symbol": "USDC", "decimals": 6, "name": "USD Coin"
        },
        "0xdAC17F958D2ee523a2206206994597C13D831ec7": {
            "symbol": "USDT", "decimals": 6, "name": "Tether USD"
        },
        "0x6B175474E89094C44Da98b954EedeAC495271d0F": {
            "symbol": "DAI", "decimals": 18, "name": "Dai Stablecoin"
        },
    }
    
    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist"""
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
