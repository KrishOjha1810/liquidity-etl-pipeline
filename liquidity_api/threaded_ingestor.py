# liquidity_api/threaded_ingestor.py
import time
import asyncio 
import queue   
import threading # standard lib, handles threads (Lock, get_ident).
from concurrent.futures import as_completed #lets you wait on multiple thread results.
from web3 import Web3
from .thread_manager import AutoscalingThreadManager # custom module , manages worker threads
from .models import SwapEvent, SyncEvent
from .config import Config
import json

'''
    This class is responsible for:
    1.Spinning up threads for different Uniswap pairs.
    2.Polling Ethereum logs (Swap & Sync events).
    3.Decoding logs and pushing them into a queue for computation.
'''
class ThreadedEventIngestor:
    def __init__(self, compute_queue):
        self.compute_queue = compute_queue or queue.Queue()
        self.web3_pool = {}  # dict mapping thread id → Web3 client, avoids sharing connections across threads.
        self.thread_manager = AutoscalingThreadManager( #c reates a pool of threads that can grow/shrink depending on workload
            min_workers=Config.MIN_THREADS,
            max_workers=Config.MAX_THREADS
        )
        
        # Load ABI
        with open('liquidity_api/abis/uniswap_v2_pair.json', 'r') as f:
            self.pair_abi = json.load(f)
        
        # Block tracking
        self.last_processed_block = None
        self.block_lock = threading.Lock() # ensures only one thread updates last_processed_block safely.
    
    def get_web3_client(self) -> Web3: # Each thread gets its own Web3 connection.
        """Get thread-local web3 client"""
        thread_id = threading.get_ident() # returns thread ID.
        
        if thread_id not in self.web3_pool:
            self.web3_pool[thread_id] = Web3(Web3.HTTPProvider(Config.ETH_RPC_URL))
            print(f"🔗 Created Web3 client for thread {thread_id}")
        
        return self.web3_pool[thread_id]
    
    def poll_pair_events(self, pair_address: str, from_block: int, to_block: int):
        """Poll events for a specific pair (runs in separate thread)"""
        thread_id = threading.get_ident()
        web3 = self.get_web3_client() # Gets thread-local Web3 client.
        
        try:
            print(f"🔍 Thread {thread_id}: Polling {pair_address[:10]}... blocks {from_block}-{to_block}")
            
            # Get Swap events
            swap_logs = web3.eth.get_logs({
                'address': pair_address,
                'fromBlock': from_block,
                'toBlock': to_block,
                'topics': [web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()]
            })
            
            # Get Sync events  
            sync_logs = web3.eth.get_logs({
                'address': pair_address,
                'fromBlock': from_block,
                'toBlock': to_block,
                'topics': [web3.keccak(text="Sync(uint112,uint112)").hex()]
            })
            
            # Process events
            events_processed = 0
            for log in swap_logs:
                swap_event = self.decode_swap_log(log, pair_address)
                if swap_event:
                    self.compute_queue.put(('swap', swap_event))
                    events_processed += 1
            
            for log in sync_logs:
                sync_event = self.decode_sync_log(log, pair_address)
                if sync_event:
                    self.compute_queue.put(('sync', sync_event))
                    events_processed += 1
            
            # Update metrics
            if pair_address in self.thread_manager.active_pools:
                self.thread_manager.active_pools[pair_address].events_per_minute += events_processed
            
            if events_processed > 0:
                print(f"✅ Thread {thread_id}: {pair_address[:10]}... - {events_processed} events")
            
            return events_processed
            
        except Exception as e:
            print(f"❌ Thread {thread_id}: Error polling {pair_address[:10]}... - {e}")
            return 0
    
    async def start_polling(self):
        """Main polling loop with dynamic threading"""
        print("🚀 Starting threaded event polling...")
        
        # Initialize thread manager with pools
        self.thread_manager.add_pools(Config.MONITORED_PAIRS)
        
        # Set initial block
        if self.last_processed_block is None:
            web3 = self.get_web3_client()
            self.last_processed_block = web3.eth.block_number - 10
        
        while True:
            try:
                cycle_start = time.time()

                web3 = self.get_web3_client()
                current_block = web3.eth.block_number
                
                if current_block > self.last_processed_block:
                    end_block = min(
                        self.last_processed_block + Config.BLOCK_BATCH_SIZE,
                        current_block
                    )
                    
                    # Submit polling tasks for all pairs in parallel
                    futures = []
                    for pair_address in Config.MONITORED_PAIRS: 
                        future = self.thread_manager.submit_pool_task( # For each pair address,it submits a job to poll logs.
                            pair_address,
                            self.poll_pair_events,
                            self.last_processed_block + 1,
                            end_block
                        )
                        if future:
                            futures.append((future, pair_address))
                    
                    # Wait for all threads to complete
                    total_events = 0
                    completed_pairs = 0
                    
                    for future, pair_address in futures:
                        try:
                            events = future.result(timeout=30)  # 30 second timeout per pair, Waits for threads to complete
                            total_events += events # Each result = number of events found for that pair in that block range.
                            completed_pairs += 1
                        except Exception as e:
                            print(f"❌ Failed to poll {pair_address[:10]}...: {e}")
                    
                    with self.block_lock: # Uses a threading.Lock to prevent race conditions when updating last_processed_block.
                        self.last_processed_block = end_block

                    cycle_time = time.time() - cycle_start 
                    
                    print(f"📊 Processed blocks {self.last_processed_block}: {completed_pairs} pairs, {total_events} events in {cycle_time:.2f}s")
                
                # Sleep based on polling interval
                await asyncio.sleep(Config.POLLING_INTERVAL)
                
            except Exception as e:
                print(f"❌ Polling loop error: {e}")
                await asyncio.sleep(5)
    
    def decode_swap_log(self, log, pair_address):
        """Decode swap log (same as before)"""
        try:
            from eth_abi import decode
            
            sender = '0x' + log['topics'][1].hex()[26:]
            to = '0x' + log['topics'][2].hex()[26:]
            amounts = decode(['uint256', 'uint256', 'uint256', 'uint256'], log['data'])
            
            web3 = self.get_web3_client()
            block = web3.eth.get_block(log['blockNumber'])
            
            return SwapEvent(
                pair=pair_address,
                sender=sender,
                amount0In=str(amounts[0]),
                amount1In=str(amounts[1]),
                amount0Out=str(amounts[2]),
                amount1Out=str(amounts[3]),
                to=to,
                block_number=log['blockNumber'],
                tx_hash=log['transactionHash'].hex(),
                log_index=log['logIndex'],
                timestamp=block['timestamp']
            )
        except Exception:
            return None
    
    def decode_sync_log(self, log, pair_address):
        """Decode sync log (same as before)"""
        try:
            from eth_abi import decode
            
            reserves = decode(['uint112', 'uint112'], log['data'])
            web3 = self.get_web3_client()
            block = web3.eth.get_block(log['blockNumber'])
            
            return SyncEvent(
                pair=pair_address,
                reserve0=str(reserves[0]),
                reserve1=str(reserves[1]),
                block_number=log['blockNumber'],
                tx_hash=log['transactionHash'].hex(),
                log_index=log['logIndex'],
                timestamp=block['timestamp']
            )
        except Exception:
            return None
    
    def shutdown(self):
        """Shutdown the threaded ingestor"""
        print("🛑 Shutting down threaded ingestor...")
        self.thread_manager.shutdown()
