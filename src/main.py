import asyncio
import os
import json
import logging
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import sys

import aiohttp
import websockets
from aiolimiter import AsyncLimiter
import clickhouse_connect

# ------------------------
# Global Configuration
# ------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# Set to False to enable detailed API request/response logging to 'api_requests.log'
# Corresponds to the --disable-api-log flag in the Rust version
DISABLE_API_LOG = True

# Binance API and WebSocket Configuration
MAX_STREAMS_PER_CONNECTION = 100
MAX_RECONNECT_ATTEMPTS = 5
BASE_RECONNECT_DELAY_S = 1
MAX_TRADES_PER_REQUEST = 1000

# Rate limiters (requests per second)
# Note: Binance weights are complex. These are simplified limits.
# Check https://binance-docs.github.io/apidocs/spot/en/#limits
RATE_LIMIT_FUTURES = AsyncLimiter(2, 1)  # A safe, low number
RATE_LIMIT_SPOT = AsyncLimiter(25, 1) # A generous limit for multiple requests

# Data Processing Configuration
MAX_BUFFERED_RECORDS = 1_000_000  # Max size for the internal asyncio.Queue
FINALIZATION_BUFFER_SECONDS = 3  # How long to wait after a minute ends before finalizing the aggregate
GAP_CHECK_INTERVAL_S = 180 # How often to query ClickHouse for historical gaps
SYMBOL_REFRESH_INTERVAL_S = 3600 # 1 hour

# ------------------------
# Data Models
# ------------------------

@dataclass
class AggTrade:
    """Represents a single aggregated trade from Binance."""
    symbol: str
    price: float
    quantity: float
    is_market_maker: bool
    agg_trade_id: int
    trade_timestamp_ms: int
    market_type: str # 'spot' or 'futures'

@dataclass
class AggTradeAggregate:
    """Represents one minute of aggregated trade data for a symbol."""
    symbol: str
    net_flow: float = 0.0
    start_atid: int | None = None
    end_atid: int | None = None
    count: int = 0
    
@dataclass
class NetflowRow:
    """Represents a row to be inserted into ClickHouse."""
    symbol: str
    timestamp: int # ms
    start_atid: int
    end_atid: int
    net_flow: float

# ------------------------
# Logging and Utility
# ------------------------

async def log_api_request(message: str):
    """Asynchronously logs API requests if enabled."""
    if DISABLE_API_LOG:
        return
    try:
        # This is a simplified async file write. For high performance,
        # a dedicated logging library like 'aiologger' would be better.
        with open("api_requests.log", "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} - {message}\n")
    except IOError as e:
        logging.error(f"Failed to write to api_requests.log: {e}")

# ------------------------
# ClickHouse & Database Logic
# ------------------------

def get_clickhouse_client() -> clickhouse_connect.driver.Client:
    """
    Initializes and returns a ClickHouse client using credentials
    from a 'secret' folder specified by the SECRETFOLDER environment variable.
    """
    secret_folder = os.environ.get("SECRETFOLDER")
    if not secret_folder:
        raise ValueError("SECRETFOLDER environment variable not set.")

    ip_path = os.path.join(secret_folder, "CLICKHOUSEIP.txt")
    pass_path = os.path.join(secret_folder, "CLICKHOUSEPASS.txt")

    try:
        with open(ip_path, 'r') as f:
            host = f.read().strip()
        with open(pass_path, 'r') as f:
            password = f.read().strip()
            
        if not host or not password:
            raise ValueError("ClickHouse IP or password file is empty.")

        return clickhouse_connect.get_client(
            host=host,
            port=8123,
            user="default",
            password=password
        )
    except FileNotFoundError as e:
        logging.error(f"Could not find ClickHouse credential file: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to initialize ClickHouse client: {e}")
        raise

async def handle_gap_filling(market_type: str, client: clickhouse_connect.driver.Client, session: aiohttp.ClientSession):
    """
    Periodically queries ClickHouse to find gaps in trade data and fetches
    the missing data from Binance REST API.
    """
    rate_limiter = RATE_LIMIT_FUTURES if market_type == 'futures' else RATE_LIMIT_SPOT
    table_suffix = "_futures" if market_type == 'futures' else "_spot"
    table_name = f'binance_NETFLOWS{table_suffix}_base'
    
    # This query is a direct translation of the one in the Rust code.
    # It finds adjacent rows where the next start_atid is not end_atid + 1.
    gap_query = f"""
    WITH data_with_prev_end_atid AS (
        SELECT
            symbol,
            any(end_atid) OVER (PARTITION BY symbol ORDER BY start_atid ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_end_atid,
            start_atid
        FROM {table_name}
        WHERE start_atid != 0 AND end_atid != 0 AND timestamp >= now() - INTERVAL 2 MONTH
        ORDER BY symbol, timestamp DESC
    )
    SELECT * 
    FROM data_with_prev_end_atid
    WHERE start_atid - prev_end_atid > 1 AND prev_end_atid != 0
    ORDER BY symbol, start_atid;
    """

    while True:
        try:
            logging.info(f"[{market_type.upper()}] Checking for data gaps in {table_name}...")
            gaps_cursor = client.query(gap_query)
            gaps = gaps_cursor.result_rows
            
            if not gaps:
                logging.info(f"[{market_type.upper()}] No gaps found.")
            else:
                logging.info(f"[{market_type.upper()}] Found {len(gaps)} gaps to fill.")
                all_missing_trades = []
                for symbol, start_id, end_id in gaps:
                    try:
                        trades = await fetch_missing_agg_trades(
                            session, symbol.upper(), market_type, start_id, end_id, rate_limiter
                        )
                        all_missing_trades.extend(trades)
                        logging.info(f"[{market_type.upper()}] Fetched {len(trades)} trades for gap in {symbol.upper()} ({start_id}-{end_id}).")
                    except Exception as e:
                        logging.error(f"[{market_type.upper()}] Error fetching gap for {symbol.upper()}: {e}")

                if all_missing_trades:
                    # Aggregate and insert the backfilled data
                    await process_backfilled_trades(all_missing_trades, client, market_type)

        except Exception as e:
            logging.error(f"[{market_type.upper()}] An error occurred during gap filling: {e}")
        
        await asyncio.sleep(GAP_CHECK_INTERVAL_S)

async def process_backfilled_trades(trades: list[AggTrade], client: clickhouse_connect.driver.Client, market_type: str):
    """Aggregates and inserts trades that were fetched to fill gaps."""
    aggregates = defaultdict(lambda: AggTradeAggregate(symbol=""))
    
    for trade in trades:
        trade_minute_ts = (trade.trade_timestamp_ms // 1000 // 60) * 60
        agg_key = (trade.symbol.upper(), trade_minute_ts)
        
        agg = aggregates[agg_key]
        agg.symbol = trade.symbol.upper()
        agg.count += 1
        agg.net_flow += trade.price * trade.quantity * (-1 if trade.is_market_maker else 1)
        
        if agg.start_atid is None or trade.agg_trade_id < agg.start_atid:
            agg.start_atid = trade.agg_trade_id
        if agg.end_atid is None or trade.agg_trade_id > agg.end_atid:
            agg.end_atid = trade.agg_trade_id
            
    if not aggregates:
        return
        
    rows_to_insert = [
        NetflowRow(
            symbol=agg.symbol.upper(),
            timestamp=key[1] * 1000, # to ms
            start_atid=agg.start_atid,
            end_atid=agg.end_atid,
            net_flow=agg.net_flow
        ) for key, agg in aggregates.items()
    ]
    
    try:
        table_suffix = "_futures" if market_type == 'futures' else "_spot"
        table_name = f'binance_NETFLOWS{table_suffix}_base'
        
        column_names = ['symbol', 'timestamp', 'start_atid', 'end_atid', 'net_flow']
        data = [list(row.__dict__.values()) for row in rows_to_insert]
        
        client.insert(table_name, data, column_names=column_names)
        logging.info(f"[{market_type.upper()}] Inserted {len(rows_to_insert)} backfilled records into {table_name}.")
    except Exception as e:
        logging.error(f"[{market_type.upper()}] Failed to insert backfilled data into ClickHouse: {e}")

# ------------------------
# Binance API & WebSocket Functions
# ------------------------

async def fetch_usdt_symbols(session: aiohttp.ClientSession, market_type: str) -> list[str]:
    """Fetches all actively trading USDT-margined symbols."""
    is_futures = market_type == 'futures'
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo" if is_futures else "https://api.binance.com/api/v3/exchangeInfo"
    
    try:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            data = await response.json()
            symbols = [
                s['symbol'].lower() for s in data['symbols']
                if s.get('quoteAsset') == 'USDT' and s.get('status') == 'TRADING' and '_' not in s.get('symbol', '')
            ]
            logging.info(f"Fetched {len(symbols)} symbols for {market_type} market.")
            return symbols
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching symbols for {market_type}: {e}")
        return []

async def fetch_missing_agg_trades(
    session: aiohttp.ClientSession,
    symbol: str,
    market_type: str,
    start_id: int,
    end_id: int,
    rate_limiter: AsyncLimiter,
) -> list[AggTrade]:
    """Fetches a range of historical aggregated trades from the REST API."""
    all_trades = []
    current_start = start_id
    
    base_url = "https://fapi.binance.com/fapi/v1/aggTrades" if market_type == 'futures' else "https://api.binance.com/api/v3/aggTrades"

    while current_start <= end_id:
        async with rate_limiter:
            params = {'symbol': symbol.upper(), 'fromId': current_start, 'limit': MAX_TRADES_PER_REQUEST}
            url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
            await log_api_request(f"Sending request: {url}")
            
            try:
                async with session.get(url) as response:
                    status = response.status
                    text = await response.text()
                    await log_api_request(f"Received response for {url}: status {status} Body: {text}")

                    if not response.ok:
                        logging.error(f"HTTP error {status} fetching trades for {symbol.upper()}: {text}")
                        # Simple break on error, could be enhanced with retries
                        break
                    
                    trades_data = json.loads(text)
                    if not trades_data:
                        break # No more trades in this range

                    for trade in trades_data:
                        agg_id = int(trade['a'])
                        if agg_id > end_id:
                            # We've over-fetched, so stop.
                            # The logic below will filter out the excess.
                            break

                        all_trades.append(AggTrade(
                            symbol=symbol.upper(),
                            price=float(trade['p']),
                            quantity=float(trade['q']),
                            is_market_maker=trade['m'],
                            agg_trade_id=agg_id,
                            trade_timestamp_ms=int(trade['T']),
                            market_type=market_type
                        ))

                    if not all_trades or int(trades_data[-1]['a']) >= end_id:
                        break # Last trade fetched is past our target, we are done
                    
                    current_start = int(trades_data[-1]['a']) + 1
            
            except (aiohttp.ClientError, json.JSONDecodeError) as e:
                logging.error(f"Error during missing trade fetch for {symbol.upper()}: {e}")
                await asyncio.sleep(5) # Wait before retrying loop
                continue

    # Filter one last time to ensure we don't include trades beyond the requested end_id
    return [t for t in all_trades if start_id <= t.agg_trade_id <= end_id]


async def run_ws_connection(
    market_type: str,
    symbols: list[str],
    queue: asyncio.Queue,
    stop_event: asyncio.Event
):
    """Manages a single WebSocket connection for a chunk of symbols."""
    is_futures = market_type == 'futures'
    base_url = "wss://fstream.binance.com/stream?streams=" if is_futures else "wss://stream.binance.com:9443/stream?streams="
    stream_names = "/".join([f"{s}@aggTrade" for s in symbols])
    url = f"{base_url}{stream_names}"

    reconnect_attempts = 0
    while not stop_event.is_set():
        try:
            async with websockets.connect(url) as websocket:
                logging.info(f"[{market_type.upper()}] WebSocket connected for {len(symbols)} symbols.")
                reconnect_attempts = 0 # Reset on successful connection
                
                # Create two tasks: one for listening to messages, one for checking the stop event
                consumer_task = asyncio.create_task(websocket_consumer(websocket, market_type, queue))
                stopper_task = asyncio.create_task(stop_event.wait())
                
                # Wait for either the consumer to finish (due to error) or the stop event to be set
                done, pending = await asyncio.wait(
                    [consumer_task, stopper_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                for task in pending:
                    task.cancel() # Clean up the other task
                
                if stop_event.is_set():
                    logging.info(f"[{market_type.upper()}] Stop event received, closing connection for {len(symbols)} symbols.")
                    break

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            logging.warning(f"[{market_type.upper()}] WebSocket error: {e}. Attempting to reconnect...")
            if reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
                delay = BASE_RECONNECT_DELAY_S * (2 ** reconnect_attempts)
                reconnect_attempts += 1
                logging.info(f"Reconnect attempt {reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS}. Waiting {delay}s.")
                await asyncio.sleep(delay)
            else:
                logging.error(f"[{market_type.upper()}] Max reconnect attempts reached. Aborting for this chunk.")
                break
        except Exception as e:
            logging.error(f"[{market_type.upper()}] An unexpected error occurred in WebSocket connection: {e}")
            break


async def websocket_consumer(websocket, market_type: str, queue: asyncio.Queue):
    """Listens for messages on the websocket and puts them in the queue."""
    async for message in websocket:
        try:
            data = json.loads(message)['data']
            trade = AggTrade(
                symbol=data['s'].upper(),
                price=float(data['p']),
                quantity=float(data['q']),
                is_market_maker=data['m'],
                agg_trade_id=int(data['a']),
                trade_timestamp_ms=int(data['T']),
                market_type=market_type
            )
            await queue.put(trade)
        except (json.JSONDecodeError, KeyError) as e:
            logging.warning(f"[{market_type.upper()}] Could not parse trade message: {message[:100]}... Error: {e}")


# ------------------------
# Core Logic and Task Management
# ------------------------

class SymbolManager:
    """Handles refreshing symbols and managing WebSocket connections."""
    def __init__(self, market_type: str, queue: asyncio.Queue, session: aiohttp.ClientSession):
        self.market_type = market_type
        self.queue = queue
        self.session = session
        self.active_symbols = set()
        self.ws_tasks = []
        self.stop_events = []

    async def run(self):
        """Main loop to periodically refresh symbols and restart connections."""
        while True:
            await self.refresh_symbols()
            await asyncio.sleep(SYMBOL_REFRESH_INTERVAL_S)

    async def refresh_symbols(self):
        """Fetches the latest symbols, compares with current ones, and restarts connections if needed."""
        logging.info(f"[{self.market_type.upper()}] Refreshing symbol list...")
        new_symbols_list = await fetch_usdt_symbols(self.session, self.market_type)
        if not new_symbols_list:
            logging.warning(f"[{self.market_type.upper()}] Failed to fetch new symbols. Skipping refresh.")
            return

        new_symbols_set = set(new_symbols_list)
        if new_symbols_set == self.active_symbols:
            logging.info(f"[{self.market_type.upper()}] Symbol list is unchanged.")
            return

        logging.info(f"[{self.market_type.upper()}] Symbol list changed. Restarting WebSocket connections.")
        
        # Stop existing WebSocket tasks
        for event in self.stop_events:
            event.set()
        await asyncio.gather(*self.ws_tasks, return_exceptions=True) # Wait for tasks to finish
        
        self.ws_tasks = []
        self.stop_events = []
        self.active_symbols = new_symbols_set
        
        # Start new connections with the updated symbol list
        for i in range(0, len(new_symbols_list), MAX_STREAMS_PER_CONNECTION):
            chunk = new_symbols_list[i:i + MAX_STREAMS_PER_CONNECTION]
            stop_event = asyncio.Event()
            self.stop_events.append(stop_event)
            task = asyncio.create_task(
                run_ws_connection(self.market_type, chunk, self.queue, stop_event)
            )
            self.ws_tasks.append(task)
        logging.info(f"[{self.market_type.upper()}] Spawned {len(self.ws_tasks)} new WebSocket tasks.")


async def trade_processor(queue: asyncio.Queue, client: clickhouse_connect.driver.Client):
    """
    The central coroutine that processes trades from the queue, aggregates them,
    and flushes them to ClickHouse.
    """
    # { (symbol, market_type, minute_timestamp_sec): AggTradeAggregate }
    aggregates = {}
    
    # { (symbol, market_type): last_agg_trade_id }
    last_trade_ids = {}

    while True:
        try:
            # Main processing loop
            await process_queue_and_gaps(queue, aggregates, last_trade_ids)
            
            # Flush completed aggregates
            await flush_finalized_aggregates(aggregates, client)

            # Short sleep to prevent a busy loop when the queue is empty
            await asyncio.sleep(0.1)

        except Exception as e:
            logging.error(f"FATAL error in trade_processor: {e}")
            await asyncio.sleep(5) # Avoid rapid-fire restarts on a persistent error


async def process_queue_and_gaps(queue: asyncio.Queue, aggregates: dict, last_trade_ids: dict):
    """Process items from the queue and check for live data gaps."""
    # Process as many items as are available in the queue without blocking
    for _ in range(queue.qsize()):
        trade = await queue.get()
        
        # --- Gap Detection ---
        # Note: This is a simplified live gap detection. The more robust `handle_gap_filling`
        # handles historical gaps from the database. This is for immediate gaps.
        tracker_key = (trade.symbol.upper(), trade.market_type)
        last_id = last_trade_ids.get(tracker_key)
        
        if last_id and trade.agg_trade_id > last_id + 1:
            missing_start = last_id + 1
            missing_end = trade.agg_trade_id - 1
            logging.warning(
                f"[{trade.market_type.upper()}] LIVE GAP DETECTED for {trade.symbol.upper()}: "
                f"Missing trades from {missing_start} to {missing_end}. Will be filled by periodic backfiller."
            )
        
        last_trade_ids[tracker_key] = trade.agg_trade_id

        # --- Aggregation ---
        trade_minute_ts = (trade.trade_timestamp_ms // 1000 // 60) * 60
        agg_key = (trade.symbol.upper(), trade.market_type, trade_minute_ts)
        
        if agg_key not in aggregates:
            aggregates[agg_key] = AggTradeAggregate(symbol=trade.symbol.upper())

        agg = aggregates[agg_key]
        agg.count += 1
        agg.net_flow += trade.price * trade.quantity * (-1 if trade.is_market_maker else 1)
        
        if agg.start_atid is None or trade.agg_trade_id < agg.start_atid:
            agg.start_atid = trade.agg_trade_id
        if agg.end_atid is None or trade.agg_trade_id > agg.end_atid:
            agg.end_atid = trade.agg_trade_id
        
        queue.task_done()


async def flush_finalized_aggregates(aggregates: dict, client: clickhouse_connect.driver.Client):
    """Finds aggregates for past minutes and writes them to ClickHouse."""
    now_unix = int(time.time())
    
    # An aggregate is "finalized" if its time window ended more than FINALIZATION_BUFFER_SECONDS ago.
    finalized_keys = [
        key for key in aggregates
        if key[2] + 60 + FINALIZATION_BUFFER_SECONDS < now_unix
    ]

    if not finalized_keys:
        return
        
    spot_rows = []
    futures_rows = []

    for key in finalized_keys:
        agg = aggregates.pop(key)
        _, market_type, minute_ts = key
        
        row_data = (
            agg.symbol.upper(),
            minute_ts * 1000, # to ms
            agg.start_atid or 0,
            agg.end_atid or 0,
            agg.net_flow
        )
        if market_type == 'spot':
            spot_rows.append(row_data)
        else:
            futures_rows.append(row_data)

    column_names = ['symbol', 'timestamp', 'start_atid', 'end_atid', 'net_flow']
    
    try:
        if futures_rows:
            client.insert('binance_NETFLOWS_futures_base', futures_rows, column_names=column_names)
            logging.info(f"Flushed {len(futures_rows)} records to futures table.")
        if spot_rows:
            client.insert('binance_NETFLOWS_spot_base', spot_rows, column_names=column_names)
            logging.info(f"Flushed {len(spot_rows)} records to spot table.")
            
    except Exception as e:
        logging.error(f"ClickHouse insert failed: {e}")
        # A robust implementation would re-queue the failed data.
        # For simplicity, we log the error and move on.


async def main():
    """The main entry point for the application."""
    logging.info("Initializing Binance USDT aggregated trade data stream...")
    
    # Initialize shared resources
    trade_queue = asyncio.Queue(maxsize=MAX_BUFFERED_RECORDS)
    
    try:
        clickhouse_client = get_clickhouse_client()
        clickhouse_client.command("SELECT 1") # Test connection
        logging.info("Successfully connected to ClickHouse.")
    except Exception as e:
        logging.fatal(f"Could not connect to ClickHouse. Exiting. Error: {e}")
        return

    async with aiohttp.ClientSession() as session:
        # Create symbol managers for each market type
        spot_manager = SymbolManager('spot', trade_queue, session)
        futures_manager = SymbolManager('futures', trade_queue, session)

        # Create the main tasks
        tasks = [
            # Task to process trades from the queue
            asyncio.create_task(trade_processor(trade_queue, clickhouse_client)),
            
            # Tasks to manage symbols and WebSocket connections
            asyncio.create_task(spot_manager.run()),
            asyncio.create_task(futures_manager.run()),
            
            # Tasks for historical gap filling
            asyncio.create_task(handle_gap_filling('spot', clickhouse_client, session)),
            asyncio.create_task(handle_gap_filling('futures', clickhouse_client, session)),
        ]
        
        # Run forever
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Application shutting down.")