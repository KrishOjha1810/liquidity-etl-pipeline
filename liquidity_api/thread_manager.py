import threading # built-in Python module for creating and controlling threads manually.
import time
from concurrent.futures import ThreadPoolExecutor, as_completed # manages a pool of worker threads, lets you submit tasks easily.
from typing import Dict, List, Set
import queue
from dataclasses import dataclass
from .config import Config

@dataclass
class PoolMetrics: # Represents statistics about one liquidity pool
    pair_address: str
    last_activity: float
    events_per_minute: int
    thread_id: int = None

class AutoscalingThreadManager:
    def __init__(self, min_workers=2, max_workers=50):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.current_workers = min_workers
        
        # Thread pool that can resize
        self.executor = ThreadPoolExecutor(max_workers=max_workers) # At most max_workers threads will run simultaneously.
        
        # Pool tracking
        self.active_pools: Dict[str, PoolMetrics] = {} # dictionary mapping pool_address → PoolMetrics
        self.pool_queue = queue.Queue() # queue for assigning pools to workers.
        self.shutdown_event = threading.Event()
        
        # Metrics, Tracks total number of pools + active thread identifiers.
        self.total_pools = 0
        self.active_threads = set()
        
        # Starts a background monitoring thread that periodically checks activity and decides if scaling is needed.
        self.monitor_thread = threading.Thread(target=self._monitor_and_scale)
        self.monitor_thread.daemon = True # thread will exit automatically when the program exits.
        self.monitor_thread.start()
    
    '''
        Loops through pool_addresses.
        If not already tracked → create a PoolMetrics object.
        Add it to active_pools and push into the work queue.
    '''
    def add_pools(self, pool_addresses: List[str]):
        """Add new pools to monitor"""
        for addr in pool_addresses:
            if addr not in self.active_pools:
                metrics = PoolMetrics(
                    pair_address=addr,
                    last_activity=time.time(),
                    events_per_minute=0
                )
                self.active_pools[addr] = metrics
                self.pool_queue.put(addr)
        
        self.total_pools = len(self.active_pools)
        print(f"📊 Total pools: {self.total_pools}")
    
    def remove_inactive_pools(self, inactive_threshold=300):  # 5 minutes
        """Remove pools with no activity"""
        current_time = time.time()
        inactive_pools = []
        
        for addr, metrics in self.active_pools.items():
            if current_time - metrics.last_activity > inactive_threshold:
                inactive_pools.append(addr)
        
        for addr in inactive_pools:
            del self.active_pools[addr]
            print(f"🗑️  Removed inactive pool: {addr[:10]}...")
        
        if inactive_pools:
            self.total_pools = len(self.active_pools)
    
    def _calculate_optimal_threads(self) -> int:
        """Calculate optimal thread count based on pool activity"""
        active_pools = len(self.active_pools)
        
        if active_pools == 0: # If no pools → just use the minimum workers.
            return self.min_workers
        
        # Factors for thread calculation
        base_threads = min(active_pools // 10, self.max_workers // 2)  # Baseline: 1 thread for every 10 pools, capped at half of max_workers.
        high_activity_pools = sum(1 for m in self.active_pools.values() # Counts pools with high activity (events_per_minute > 10)
                                 if m.events_per_minute > 10)
        activity_bonus = min(high_activity_pools // 5, 10)  # Adds extra threads: 1 per 5 busy pools, up to +10 threads.
        
        optimal = max( # Calculates final optimal thread count (between min and max).
            self.min_workers,
            min(base_threads + activity_bonus, self.max_workers)
        )
        
        return optimal
    
    def _monitor_and_scale(self): 
        """Background thread for monitoring and autoscaling"""
        while not self.shutdown_event.is_set(): # Runs forever until shutdown event is triggered.
            try:
                # Remove inactive pools
                self.remove_inactive_pools()
                
                # Calculate optimal thread count
                optimal_threads = self._calculate_optimal_threads()
                
                if optimal_threads != self.current_workers:
                    print(f"🔄 Scaling: {self.current_workers} → {optimal_threads} threads")
                    self.current_workers = optimal_threads
                
                # Print metrics
                active_count = len([t for t in self.active_threads if t])
                print(f"📈 Pools: {len(self.active_pools)} | Active Threads: {active_count} | Target: {optimal_threads}")
                
                time.sleep(30)  # Waits 30 seconds before checking again.
                
            except Exception as e:
                print(f"❌ Monitor error: {e}")
                time.sleep(5)
    
    def submit_pool_task(self, pool_address: str, task_func, *args):
        """Submit a task for a specific pool"""
        if pool_address in self.active_pools:
            # Update activity
            self.active_pools[pool_address].last_activity = time.time()
            
            # Submit to thread pool
            future = self.executor.submit(task_func, pool_address, *args)
            return future
        
        return None
    
    def shutdown(self):
        """Graceful shutdown"""
        print("🛑 Shutting down thread manager...")
        self.shutdown_event.set()
        self.executor.shutdown(wait=True)
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        print("✅ Thread manager shutdown complete")
