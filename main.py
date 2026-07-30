"""
Main orchestration - Runs all 3 processes with PROPER shutdown handling
"""
import asyncio
import uvicorn
import os
import sys
import signal
import queue 
# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from liquidity_api.threaded_ingestor import ThreadedEventIngestor  
from liquidity_api.compute import ComputeEngine  
from liquidity_api.api import LiquidityAPI
from liquidity_api.config import Config

# Global shutdown event
shutdown_event = asyncio.Event()
tasks = []

def signal_handler(signum, frame):
    print(f"\n🛑 Received signal {signum}. Initiating shutdown...")
    shutdown_event.set()

async def shutdown_tasks():
    """Properly shutdown all background tasks"""
    global tasks
    
    if not tasks:
        return
        
    print("🔄 Cancelling background tasks...")
    
    # Cancel all tasks
    for i, task in enumerate(tasks):
        print(f"   Cancelling task {i+1}/{len(tasks)}: {task.get_name()}")
        task.cancel()
    
    # Wait for cancellation with timeout
    try:
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=3.0
        )
        print("✅ All tasks cancelled successfully")
    except asyncio.TimeoutError:
        print("⚠️  Some tasks didn't respond to cancellation")
    
    tasks.clear()

async def main():
    """Main entry point with bulletproof shutdown"""
    global tasks
    
    print("🚀 Starting Liquidity API...")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Ensure data directory exists
        Config.ensure_directories()
        
        # Initialize components
        ingestor = ThreadedEventIngestor(compute_queue=queue.Queue())
        compute_engine = ComputeEngine(ingestor.compute_queue)
        api = LiquidityAPI(compute_engine)
        
        print(f"🌐 Starting API server on {Config.HOST}:{Config.PORT}")
        
        # Create FastAPI server
        config = uvicorn.Config(
            app=api.get_app(),
            host=Config.HOST,
            port=Config.PORT,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        # Create background tasks with names
        tasks = [
            asyncio.create_task(ingestor.start_polling(), name="threaded_ingestor"),
            asyncio.create_task(compute_engine.process_events(), name="compute"),
            asyncio.create_task(server.serve(), name="fastapi_server")
        ]
        
        print("✅ All services started. Press Ctrl+C to stop gracefully.")
        
        # Main loop - wait for shutdown signal
        while not shutdown_event.is_set():
            await asyncio.sleep(0.1)
            
            # Check if any critical task failed
            for task in tasks:
                if task.done() and not task.cancelled():
                    try:
                        await task  # This will raise any exception
                    except Exception as e:
                        print(f"💥 Task {task.get_name()} failed: {e}")
                        shutdown_event.set()
                        break
        
        print("🛑 Shutdown signal received...")
        
    except Exception as e:
        print(f"💥 Fatal error: {e}")
    finally:
        print("🔄 Cleaning up...")
        await shutdown_tasks()
        print("👋 Shutdown complete. Goodbye!")

if __name__ == "__main__":
    try:
        # Handle KeyboardInterrupt gracefully
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
    finally:
        # Force exit if anything is still hanging
        print("🔚 Force exit")
        os._exit(0)
