# liquidity-etl-pipeline

A real-time ETL pipeline that watches Uniswap V2 pairs on Ethereum mainnet,
keeps their liquidity state current, and serves it over a query API and a
Server-Sent Events stream.

It reads live on-chain data from public RPC endpoints. There is no mock data
layer: the numbers it serves come from Swap and Sync events as they land on
mainnet.

## Why it exists

Reading a pair's reserves on demand means an RPC round trip per query, and the
answer is stale the moment a swap lands. This keeps a warm in-memory state per
pair, updated from the event stream, so a query is a dictionary lookup and
subscribers get pushed the change instead of polling for it.

## Architecture

Three components run under one supervisor (`main.py`) and talk over asyncio
queues.

```
Ethereum mainnet (public RPC)
          |
          v
   Threaded ingestor  ---- SQLite (swap_events, sync_events)
          |
     asyncio.Queue
          |
          v
    Compute engine     ---- in-memory PairState per pair
          |
     asyncio.Queue
          |
          v
    FastAPI server     ---- POST query  +  SSE stream
```

**Ingestor** (`ingestor.py`, `threaded_ingestor.py`) polls for new blocks on a
12 second interval, pulls Swap and Sync logs for the pairs it tracks in batches
of 50 blocks, decodes them against the Uniswap V2 pair ABI, and writes them to
SQLite before handing them on. Block fetching runs on a worker pool rather than
serially, because the per batch cost is almost entirely RPC wait.

**Thread manager** (`thread_manager.py`) owns that pool and resizes it. A
monitor thread watches queue depth and scales workers between 2 and 50, so a
quiet chain does not hold 50 idle threads and a backfill burst is not
serialised behind 2.

**Compute engine** (`compute.py`) consumes events in order and maintains a
`PairState` per pair: reserves, derived price, and last seen block. Reserve
maths uses `Decimal` with precision set explicitly rather than floats, since
token amounts are large integers where float rounding is a correctness bug, not
a cosmetic one.

**API** (`api.py`) serves the computed state. It never touches the chain
itself, so a query cannot be slowed down or failed by RPC latency.

## API

| Method | Path | What it does |
|---|---|---|
| `GET` | `/health` | Liveness and a server timestamp |
| `POST` | `/liquidity` | Current state for the requested pools |
| `GET` | `/stream/{chain_id}` | SSE stream, one message per state change |
| `GET` | `/pairs` | The pairs currently being monitored |

`POST /liquidity` takes the pools you want state for:

```json
{
  "chain_id": 1,
  "protocol": "uniswap_v2",
  "pools": ["0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"]
}
```

and returns a `PairState` per pool. `GET /stream/{chain_id}` is the same data
pushed as it changes.

## Running it

```bash
pip install -r requirements.txt
python main.py
```

Serves on `http://0.0.0.0:8000`. No API key is needed: the defaults are public
RPC endpoints.

Configuration is environment variables, all optional, defaults in `config.py`:

| Variable | Default | Meaning |
|---|---|---|
| `ETH_RPC_URL` | publicnode | RPC endpoint. Four alternates are listed in `config.py` |
| `POLLING_INTERVAL` | `12` | Seconds between polls, roughly one Ethereum block |
| `BLOCK_BATCH_SIZE` | `50` | Blocks fetched per batch |
| `START_BLOCK` | `latest` | Or a block number, to backfill from a point |
| `MIN_THREADS` / `MAX_THREADS` | `2` / `50` | Worker pool bounds |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | Bind address |

## Shutdown

`main.py` installs SIGINT and SIGTERM handlers, cancels the background tasks,
and waits up to 3 seconds for them to unwind before exiting, so a Ctrl-C during
a batch does not leave half written rows behind. If a task does not respond in
time it says so rather than hanging silently.

## Honest limits

These are known and deliberate for a single node project, not oversights:

- **SQLite, one node.** State is in process memory and events are on local
  disk. There is no horizontal scaling story and no leader election.
- **Polling, not subscriptions.** It polls on an interval rather than holding a
  websocket subscription, which costs up to one interval of latency. Public RPC
  endpoints vary in how well they support `eth_subscribe`, so polling was the
  portable choice.
- **Public RPC endpoints rate limit.** The alternate list in `config.py` is
  swapped by hand, not automatically.
- **Uniswap V2 only.** V3 concentrated liquidity has a different state model
  and is not handled.
- **Reorgs are not handled.** A reorg deeper than the current batch would leave
  stale rows in the events table.

## Stack

Python, web3.py, FastAPI, asyncio, SQLite, threading.
