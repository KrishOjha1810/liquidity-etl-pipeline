"""
Data models for Liquidity API
"""
from pydantic import BaseModel
from typing import Optional, List, Dict
from decimal import Decimal
import json

class SwapEvent(BaseModel):
    pair: str
    sender: str
    amount0In: str
    amount1In: str
    amount0Out: str
    amount1Out: str
    to: str
    block_number: int
    tx_hash: str
    log_index: int
    timestamp: int

class SyncEvent(BaseModel):
    pair: str
    reserve0: str
    reserve1: str
    block_number: int
    tx_hash: str
    log_index: int
    timestamp: int

class PairState(BaseModel):
    pair: str
    token0: str
    token1: str
    reserve0: str
    reserve1: str
    price01: str  # token0 price in token1
    price10: str  # token1 price in token0
    last_block: int
    last_updated: int
    decimals0: int = 18
    decimals1: int = 18

class LiquidityRequest(BaseModel):
    chain_id: int
    protocol: str = "uniswap_v2"
    pools: List[str]

class LiquidityResponse(BaseModel):
    pools: List[PairState]

class StreamEvent(BaseModel):
    type: str  # "sync" or "swap"
    pair: str
    data: Dict
    timestamp: int
    block_number: int
