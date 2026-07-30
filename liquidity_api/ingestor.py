"""
Event Ingestor - Polls blockchain for Uniswap V2 events
"""
import asyncio
import json
import time
from web3 import Web3
from web3.contract import Contract
from typing import List, Dict
import sqlite3
from .config import Config
from .models import SwapEvent, SyncEvent

class EventIngestor:
    def __init__(self):
        self.web3 = Web3(Web3.HTTPProvider(Config.ETH_RPC_URL))
        self.last_processed_block = None
        self.pair_contracts = {}
        self.compute_queue = asyncio.Queue()
        
        # Load Uniswap V2 Pair ABI
        with open('liquidity_api/abis/uniswap_v2_pair.json', 'r') as f:
            self.pair_abi = json.load(f)
    
    async def initialize_database(self): #Creates SQLite database tables for swap_events and sync_events
        """Initialize SQLite database for event storage"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS swap_events (
                id INTEGER PRIMARY KEY,
                pair TEXT,
                sender TEXT,
                amount0In TEXT,
                amount1In TEXT,
                amount0Out TEXT,
                amount1Out TEXT,
                to_address TEXT,
                block_number INTEGER,
                tx_hash TEXT,
                log_index INTEGER,
                timestamp INTEGER,
                UNIQUE(tx_hash, log_index)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_events (
                id INTEGER PRIMARY KEY,
                pair TEXT,
                reserve0 TEXT,
                reserve1 TEXT,
                block_number INTEGER,
                tx_hash TEXT,
                log_index INTEGER,
                timestamp INTEGER,
                UNIQUE(tx_hash, log_index)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_pair_contract(self, pair_address: str) -> Contract: # Given a pool (pair) address, it returns a contract object that knows how to decode its events using the ABI.
        """Get or create contract instance for pair"""
        if pair_address not in self.pair_contracts:
            self.pair_contracts[pair_address] = self.web3.eth.contract( #Caches the contract so it doesn’t reload it every time.
                address=pair_address, 
                abi=self.pair_abi
            )
        return self.pair_contracts[pair_address]
    
    async def poll_events(self):
        """Main polling loop for blockchain events"""
        await self.initialize_database() # Initializes the SQLite database.
        
        if self.last_processed_block is None: #Figures out where to start (latest block or a fixed block number)
            if Config.START_BLOCK == "latest":
                self.last_processed_block = self.web3.eth.block_number - 10
            else:
                self.last_processed_block = int(Config.START_BLOCK)
        
        print(f"Starting event polling from block {self.last_processed_block}")
        
        while True:#Continuously polls Ethereum for new blocks
            try:
                current_block = self.web3.eth.block_number
                
                if current_block > self.last_processed_block: #Processes events block by block in batches
                    # Process blocks in batches
                    end_block = min(
                        self.last_processed_block + Config.BLOCK_BATCH_SIZE,
                        current_block
                    )
                    
                    await self.process_block_range(
                        self.last_processed_block + 1, 
                        end_block
                    )
                    
                    self.last_processed_block = end_block
                    print(f"Processed blocks up to {end_block}")
                
                await asyncio.sleep(Config.POLLING_INTERVAL) #Sleeps for a while (POLLING_INTERVAL) before repeating
                
            except Exception as e:
                print(f"Error in polling loop: {e}")
                await asyncio.sleep(5)
    
    async def process_block_range(self, from_block: int, to_block: int): #For each pair you’re monitoring, it queries logs for: Swap events,Sync events
        """Process events in a block range"""
        for pair_address in Config.MONITORED_PAIRS:
            # Get Swap events
            swap_filter = {
                'address': pair_address,
                'fromBlock': from_block,
                'toBlock': to_block,
                'topics': [
                    self.web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
                ]
            }
            
            swap_logs = self.web3.eth.get_logs(swap_filter)
            await self.process_swap_events(swap_logs, pair_address)
            
            # Get Sync events
            sync_filter = {
                'address': pair_address,
                'fromBlock': from_block, 
                'toBlock': to_block,
                'topics': [
                    self.web3.keccak(text="Sync(uint112,uint112)").hex()
                ]
            }
            
            sync_logs = self.web3.eth.get_logs(sync_filter)
            await self.process_sync_events(sync_logs, pair_address)
    
    async def process_swap_events(self, logs: List, pair_address: str):
        """Process Swap event logs"""
        for log in logs:
            try:
                # Use manual decoding instead of contract.events
                decoded_data = self.decode_swap_log(log)
                if not decoded_data:
                    continue
                    
                block = self.web3.eth.get_block(log['blockNumber'])
                
                swap_event = SwapEvent(
                    pair=pair_address,
                    sender=decoded_data['sender'],
                    amount0In=str(decoded_data['amount0In']),
                    amount1In=str(decoded_data['amount1In']),
                    amount0Out=str(decoded_data['amount0Out']),
                    amount1Out=str(decoded_data['amount1Out']),
                    to=decoded_data['to'],
                    block_number=log['blockNumber'],
                    tx_hash=log['transactionHash'].hex(),
                    log_index=log['logIndex'],
                    timestamp=block['timestamp']
                )
                
                # Store to database
                await self.store_swap_event(swap_event)
                
                # Send to compute process
                await self.compute_queue.put(('swap', swap_event))
                
            except Exception as e:
                print(f"Error processing swap event: {e}")

    async def process_sync_events(self, logs: List, pair_address: str):
        """Process Sync event logs"""
        for log in logs:
            try:
                # Use manual decoding instead of contract.events
                decoded_data = self.decode_sync_log(log)
                if not decoded_data:
                    continue
                    
                block = self.web3.eth.get_block(log['blockNumber'])
                
                sync_event = SyncEvent(
                    pair=pair_address,
                    reserve0=str(decoded_data['reserve0']),
                    reserve1=str(decoded_data['reserve1']),
                    block_number=log['blockNumber'],
                    tx_hash=log['transactionHash'].hex(),
                    log_index=log['logIndex'],
                    timestamp=block['timestamp']
                )
                
                # Store to database
                await self.store_sync_event(sync_event)
                
                # Send to compute process
                await self.compute_queue.put(('sync', sync_event))
                
            except Exception as e:
                print(f"Error processing sync event: {e}")
    
    async def store_swap_event(self, event: SwapEvent): #Persists events so you have a history
        """Store swap event to database"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO swap_events 
                (pair, sender, amount0In, amount1In, amount0Out, amount1Out, 
                 to_address, block_number, tx_hash, log_index, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event.pair, event.sender, event.amount0In, event.amount1In,
                event.amount0Out, event.amount1Out, event.to, event.block_number,
                event.tx_hash, event.log_index, event.timestamp
            ))
            conn.commit()
        finally:
            conn.close()
    
    async def store_sync_event(self, event: SyncEvent):
        """Store sync event to database"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO sync_events 
                (pair, reserve0, reserve1, block_number, tx_hash, log_index, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event.pair, event.reserve0, event.reserve1, event.block_number,
                event.tx_hash, event.log_index, event.timestamp
            ))
            conn.commit()
        finally:
            conn.close()

    def decode_swap_log(self, log):
        """Manually decode Swap event log"""
        try:
            from eth_abi import decode
            
            # Swap event signature
            swap_topic = self.web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)").hex()
            
            if log['topics'][0].hex() != swap_topic:
                return None
                
            # Extract indexed parameters from topics
            sender = '0x' + log['topics'][1].hex()[26:]  # Remove padding
            to = '0x' + log['topics'][2].hex()[26:]      # Remove padding
            
            # Decode non-indexed data
            amounts = decode(['uint256', 'uint256', 'uint256', 'uint256'], log['data'])
            
            return {
                'sender': sender,
                'amount0In': amounts[0],
                'amount1In': amounts[1], 
                'amount0Out': amounts[2],
                'amount1Out': amounts[3],
                'to': to
            }
            
        except Exception as e:
            print(f"Error decoding swap log: {e}")
            return None

    def decode_sync_log(self, log):
        """Manually decode Sync event log"""
        try:
            from eth_abi import decode
            
            # Sync event signature  
            sync_topic = self.web3.keccak(text="Sync(uint112,uint112)").hex()
            
            if log['topics'][0].hex() != sync_topic:
                return None
                
            # Decode data (2 uint112 values)
            reserves = decode(['uint112', 'uint112'], log['data'])
            
            return {
                'reserve0': reserves[0],
                'reserve1': reserves[1]
            }
            
        except Exception as e:
            print(f"Error decoding sync log: {e}")
            return None

