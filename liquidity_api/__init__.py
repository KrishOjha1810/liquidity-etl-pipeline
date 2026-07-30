"""
Liquidity API - A real-time Uniswap V2 liquidity monitoring system

This package implements a 3-process architecture:
1. Ingestor: Polls blockchain events
2. Compute: Maintains real-time pair states  
3. API: Serves POST queries and SSE streams
"""

__version__ = "1.0.0"
__author__ = "DecentralLabs Team"

# Package-level imports
from .config import Config
from .models import (
    SwapEvent, SyncEvent, PairState,
    LiquidityRequest, LiquidityResponse, StreamEvent
)

__all__ = [
    "Config", "SwapEvent", "SyncEvent", "PairState",
    "LiquidityRequest", "LiquidityResponse", "StreamEvent"
]
