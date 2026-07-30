"""
API Process - FastAPI server with POST queries and SSE streaming
This file defines a FastAPI API that allows clients to query current liquidity states
and receive real-time updates via SSE (Server-Sent Events). 
It interacts with the ComputeEngine for live data.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse # SSE library for streaming events
import asyncio  # Asynchronous programming
import json     # To convert Python objects to JSON
import time     # For timestamps
from typing import Dict, List
from web3 import Web3

# Import models and config from the package
from .models import LiquidityRequest, LiquidityResponse, PairState, StreamEvent
from .config import Config

class LiquidityAPI:
    """
    API class for serving liquidity data.

    Attributes:
        app: FastAPI instance
        compute_engine: Reference to ComputeEngine (real-time pair state)
        active_streams: Tracks currently active SSE client connections
    """
    def __init__(self, compute_engine):
        # Create the FastAPI application with a title and version
        self.app = FastAPI(title="Liquidity API", version="1.0.0")
        self.compute_engine = compute_engine  # Store reference to ComputeEngine
        self.active_streams = set()          # Store active SSE stream connections
        
        # Setup API routes (endpoints)
        self.setup_routes()
    
    def setup_routes(self):
        """Define all API endpoints (routes)."""
        
        @self.app.get("/health")
        async def health_check():
            """
            Simple health check endpoint.
            Returns current status and timestamp.
            Useful for monitoring and load balancers.
            """
            return {"status": "healthy", "timestamp": int(time.time())}
        
        @self.app.post("/liquidity", response_model=LiquidityResponse)
        async def get_liquidity(request: LiquidityRequest):
            """Get current liquidity state for specified pools"""
            result = []
            
            for pool_address in request.pools:
                try:
                    # Normalize address to checksum format for lookup
                    normalized_address = Web3.to_checksum_address(pool_address)
                    pair_state = self.compute_engine.get_pair_state(normalized_address)
                    
                    if pair_state:
                        result.append({
                            "pair": pool_address,  # Return original format from request
                            "token0": pair_state.token0,
                            "token1": pair_state.token1,
                            "reserve0": pair_state.reserve0,
                            "reserve1": pair_state.reserve1,
                            "price01": pair_state.price01,
                            "price10": pair_state.price10,
                            "last_block": pair_state.last_block,
                            "last_updated": pair_state.last_updated,
                            "decimals0": pair_state.decimals0,
                            "decimals1": pair_state.decimals1
                        })
                    else:
                        # Return empty state if not found (as before)
                        result.append({
                            "pair": pool_address,
                            "token0": "",
                            "token1": "",
                            "reserve0": "0",
                            "reserve1": "0",
                            "price01": "0",
                            "price10": "0",
                            "last_block": 0,
                            "last_updated": 0,
                            "decimals0": 18,
                            "decimals1": 18
                        })
                        
                except Exception as e:
                    print(f"Error processing pool {pool_address}: {e}")
                    # Return empty state on error
                    result.append({
                        "pair": pool_address,
                        "token0": "",
                        "token1": "",
                        "reserve0": "0",
                        "reserve1": "0",
                        "price01": "0",
                        "price10": "0",
                        "last_block": 0,
                        "last_updated": 0,
                        "decimals0": 18,
                        "decimals1": 18
                    })
            
            return LiquidityResponse(pools=result)
        
        @self.app.get("/stream/{chain_id}")
        async def stream_liquidity_updates(chain_id: int):
            """
            SSE endpoint to stream real-time liquidity updates.
            
            Args:
                chain_id: Requested chain ID for the stream (must match Config.CHAIN_ID)
            
            Returns:
                EventSourceResponse: keeps the connection open and streams events
            """
            if chain_id != Config.CHAIN_ID:
                # Only support the configured chain
                raise HTTPException(status_code=400, detail=f"Unsupported chain_id: {chain_id}")
            
            # Return SSE stream
            return EventSourceResponse(self.event_stream())
        
        @self.app.get("/pairs")
        async def list_monitored_pairs():
            """
            GET endpoint to list all currently monitored pairs.
            
            Returns:
                JSON object with keys:
                  - "pairs": list of pair addresses
                  - "states": dictionary mapping pair_address -> PairState
            """
            all_states = self.compute_engine.get_all_pair_states()
            return {"pairs": list(all_states.keys()), "states": all_states}
    
    async def event_stream(self):
        """
        SSE event stream generator.
        Yields initial snapshot + live updates from ComputeEngine's stream queue.
        """
        stream_id = id(asyncio.current_task())  # Unique ID for this client connection
        self.active_streams.add(stream_id)      # Mark stream as active
        
        try:
            # --- Send initial state when client connects ---
            all_states = self.compute_engine.get_all_pair_states()
            for pair_address, state in all_states.items():
                initial_event = {
                    "type": "initial_state",  # Type of event
                    "pair": pair_address,
                    "data": state.dict(),    # Convert PairState object to dictionary
                    "timestamp": int(time.time())
                }
                # Yield as SSE event
                yield {"event": "liquidity_update", "data": json.dumps(initial_event)}
            
            # --- Stream live updates ---
            while stream_id in self.active_streams:
                try:
                    # Wait for next event from ComputeEngine (with 30s timeout)
                    stream_event = await asyncio.wait_for(
                        self.compute_engine.stream_queue.get(),
                        timeout=30.0
                    )
                    
                    # Prepare event data
                    event_data = {
                        "type": stream_event.type,          # 'swap' or 'sync'
                        "pair": stream_event.pair,
                        "data": stream_event.data,
                        "timestamp": stream_event.timestamp,
                        "block_number": stream_event.block_number
                    }
                    
                    # Yield to client
                    yield {"event": "liquidity_update", "data": json.dumps(event_data)}
                    
                except asyncio.TimeoutError:
                    # If no updates in 30s, send a keepalive to prevent connection timeout
                    yield {"event": "keepalive", "data": json.dumps({"timestamp": int(time.time())})}
                
        except Exception as e:
            # Log any error in the stream generator
            print(f"Error in event stream: {e}")
        finally:
            # Remove stream from active streams when client disconnects
            self.active_streams.discard(stream_id)
    
    def get_app(self):
        """
        Returns the FastAPI app instance.
        This is used by Uvicorn or any ASGI server to run the API.
        """
        return self.app
