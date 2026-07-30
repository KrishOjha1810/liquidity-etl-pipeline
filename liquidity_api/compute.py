"""
Compute Process - Maintains real-time pair state
"""
import asyncio
import queue
import sqlite3
import json
from decimal import Decimal, getcontext
from typing import Dict, Optional
from .config import Config
from .models import PairState, SwapEvent, SyncEvent, StreamEvent

# Set high precision for calculations
getcontext().prec = 10

class ComputeEngine:
    def __init__(self, ingestor_queue: asyncio.Queue):
        self.ingestor_queue = ingestor_queue
        self.pair_states: Dict[str, PairState] = {}
        self.stream_queue = asyncio.Queue()
        
        # Add web3 connection for contract calls
        from web3 import Web3
        import json
        
        self.web3 = Web3(Web3.HTTPProvider(Config.ETH_RPC_URL))
        
        # Load Uniswap V2 Pair ABI for contract calls
        try:
            with open('liquidity_api/abis/uniswap_v2_pair.json', 'r') as f:
                self.pair_abi = json.load(f)
            print("✅ Loaded Uniswap V2 Pair ABI")
        except Exception as e:
            print(f"❌ Error loading pair ABI: {e}")
            self.pair_abi = []

        
    async def initialize_database(self):
        """Initialize pair states table"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pair_states (
                pair TEXT PRIMARY KEY,
                token0 TEXT,
                token1 TEXT,
                reserve0 TEXT,
                reserve1 TEXT,
                price01 TEXT,
                price10 TEXT,
                last_block INTEGER,
                last_updated INTEGER,
                decimals0 INTEGER,
                decimals1 INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Load existing states
        await self.load_pair_states()
    
    async def load_pair_states(self): # Loads all saved pair states from database into self.pair_states.
        """Load existing pair states from database"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM pair_states')
        rows = cursor.fetchall()
        
        for row in rows:
            pair_state = PairState(
                pair=row[0],
                token0=row[1],
                token1=row[2], 
                reserve0=row[3],
                reserve1=row[4],
                price01=row[5],
                price10=row[6],
                last_block=row[7],
                last_updated=row[8],
                decimals0=row[9],
                decimals1=row[10]
            )
            self.pair_states[row[0]] = pair_state
        
        conn.close()
        print(f"Loaded {len(self.pair_states)} existing pair states")
    
    async def process_events(self):
        """Main event processing loop"""
        await self.initialize_database()
        
        while True:
            try:
                # Handle regular queue (not asyncio.Queue)
                try:
                    event_type, event_data = self.ingestor_queue.get_nowait()
                    
                    if event_type == 'sync':
                        await self.process_sync_event(event_data)
                    elif event_type == 'swap':
                        await self.process_swap_event(event_data)
                    
                    self.ingestor_queue.task_done()
                    
                except queue.Empty:
                    # No events available, sleep briefly
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                print(f"Error processing event: {e}")
                await asyncio.sleep(1)
    
    async def process_sync_event(self, event: SyncEvent): # Updates reserves of a pool when a Sync log appears.
        """Process Sync event and update pair state"""
        try:
            # Get or create pair state
            if event.pair not in self.pair_states:
                await self.initialize_pair_state(event.pair)
            
            pair_state = self.pair_states[event.pair]
            
            # Update reserves
            pair_state.reserve0 = event.reserve0
            pair_state.reserve1 = event.reserve1
            pair_state.last_block = event.block_number
            pair_state.last_updated = event.timestamp
            
            # Calculate prices
            await self.calculate_prices(pair_state) # Calculates new prices (token0 per token1 and vice versa).
            
            # Save to database
            await self.save_pair_state(pair_state)
            
            # Create stream event
            stream_event = StreamEvent(
                type="sync",
                pair=event.pair,
                data={
                    "reserve0": event.reserve0,
                    "reserve1": event.reserve1,
                    "price01": pair_state.price01,
                    "price10": pair_state.price10
                },
                timestamp=event.timestamp,
                block_number=event.block_number
            )
            
            await self.stream_queue.put(stream_event)
            print(f"Updated pair {event.pair[:10]}... - R0: {event.reserve0[:10]}, R1: {event.reserve1[:10]}")
            
        except Exception as e:
            print(f"Error processing sync event: {e}")
    
    async def process_swap_event(self, event: SwapEvent):
        """Process Swap event"""
        try:
            # Simply converts Swap events into stream events for real-time updates.
            stream_event = StreamEvent(
                type="swap",
                pair=event.pair,
                data={
                    "amount0In": event.amount0In,
                    "amount1In": event.amount1In,
                    "amount0Out": event.amount0Out,
                    "amount1Out": event.amount1Out,
                    "sender": event.sender,
                    "to": event.to
                },
                timestamp=event.timestamp,
                block_number=event.block_number
            )
            
            await self.stream_queue.put(stream_event)
            
        except Exception as e:
            print(f"Error processing swap event: {e}")
    
    async def initialize_pair_state(self, pair_address: str):
        """Initialize a new pair state by calling pair and token contracts"""
        try:
            print(f"Initializing pair {pair_address}...")
            
            # Create pair contract instance
            pair_contract = self.web3.eth.contract(
                address=pair_address, 
                abi=self.pair_abi
            )
            
            # Get token addresses from pair contract
            token0_address = pair_contract.functions.token0().call()
            token1_address = pair_contract.functions.token1().call()
            
            print(f"Token0: {token0_address}, Token1: {token1_address}")
            
            # ERC-20 ABI (minimal - just decimals, symbol)
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol", 
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                }
            ]
            
            # Get token0 and token1 contracts
            token0_contract = self.web3.eth.contract(
                address=token0_address,
                abi=erc20_abi
            )
            
            token1_contract = self.web3.eth.contract(
                address=token1_address, 
                abi=erc20_abi
            )
            
            # Fetch decimals with error handling
            try:
                decimals0 = token0_contract.functions.decimals().call()
                symbol0 = token0_contract.functions.symbol().call()
            except Exception as e:
                print(f"Error fetching token0 metadata: {e}")
                decimals0 = 18
                symbol0 = "UNKNOWN"
                
            try:
                decimals1 = token1_contract.functions.decimals().call()
                symbol1 = token1_contract.functions.symbol().call()
            except Exception as e:
                print(f"Error fetching token1 metadata: {e}")
                decimals1 = 18
                symbol1 = "UNKNOWN"
            
            print(f"Token0: {symbol0} ({decimals0} decimals)")
            print(f"Token1: {symbol1} ({decimals1} decimals)")
            
            # Create pair state with real data
            self.pair_states[pair_address] = PairState(
                pair=pair_address,
                token0=token0_address,
                token1=token1_address,
                reserve0="0",
                reserve1="0", 
                price01="0",
                price10="0",
                last_block=0,
                last_updated=0,
                decimals0=decimals0,
                decimals1=decimals1
            )
            
            # Save to database immediately
            await self.save_pair_state(self.pair_states[pair_address])
            
            print(f"✅ Initialized pair {pair_address[:10]}... with real token data")
            
        except Exception as e:
            print(f"❌ Error initializing pair {pair_address}: {e}")
            # Fallback to placeholder
            self.pair_states[pair_address] = PairState(
                pair=pair_address,
                token0="0x0000000000000000000000000000000000000000",
                token1="0x0000000000000000000000000000000000000000",
                reserve0="0",
                reserve1="0", 
                price01="0",
                price10="0",
                last_block=0,
                last_updated=0,
                decimals0=18,
                decimals1=18
            )
    
    async def calculate_prices(self, pair_state: PairState):
        """Calculate token prices from reserves with correct decimal adjustment"""
        try:
            reserve0 = Decimal(pair_state.reserve0)
            reserve1 = Decimal(pair_state.reserve1)
            
            if reserve0 > 0 and reserve1 > 0:
                # Apply decimal adjustments
                decimal_factor0 = Decimal(10) ** pair_state.decimals0
                decimal_factor1 = Decimal(10) ** pair_state.decimals1
                
                # Normalize reserves to actual token amounts
                normalized_r0 = reserve0 / decimal_factor0
                normalized_r1 = reserve1 / decimal_factor1
                
                # Calculate prices: price01 = token0 price in token1
                pair_state.price01 = str(normalized_r1 / normalized_r0)
                # price10 = token1 price in token0  
                pair_state.price10 = str(normalized_r0 / normalized_r1)
                
                # Debug output
                print(f"💰 Price update {pair_state.pair[:10]}...")
                print(f"   Decimals: {pair_state.decimals0}/{pair_state.decimals1}")
                print(f"   Price01: {pair_state.price01[:12]}... Price10: {pair_state.price10[:12]}...")
                
            else:
                pair_state.price01 = "0"
                pair_state.price10 = "0"
                    
        except Exception as e:
            print(f"❌ Error calculating prices: {e}")
            pair_state.price01 = "0"
            pair_state.price10 = "0"

    
    async def save_pair_state(self, pair_state: PairState):
        """Save pair state to database"""
        conn = sqlite3.connect(Config.DATABASE_PATH)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO pair_states
                (pair, token0, token1, reserve0, reserve1, price01, price10, 
                 last_block, last_updated, decimals0, decimals1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pair_state.pair, pair_state.token0, pair_state.token1,
                pair_state.reserve0, pair_state.reserve1, pair_state.price01,
                pair_state.price10, pair_state.last_block, pair_state.last_updated,
                pair_state.decimals0, pair_state.decimals1
            ))
            conn.commit()
        finally:
            conn.close()
    
    def get_pair_state(self, pair_address: str) -> Optional[PairState]:
        """Get current pair state"""
        return self.pair_states.get(pair_address)
    
    def get_all_pair_states(self) -> Dict[str, PairState]:
        """Get all current pair states"""
        return self.pair_states.copy()
