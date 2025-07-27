import asyncio
import os
import json
import time

import aiohttp
import websockets
from aiolimiter import AsyncLimiter
import clickhouse_connect

import logging
from structs import AggTradeAggregate, AggTrade

from backfill_flows import handle_gap_filling
# Binance API and WebSocket Configuration
MAX_STREAMS_PER_CONNECTION = 100
MAX_RECONNECT_ATTEMPTS = 5
BASE_RECONNECT_DELAY_S = 1
MAX_TRADES_PER_REQUEST = 1000

# Data Processing Configuration
MAX_BUFFERED_RECORDS = 1_000_000  # Max size for the internal asyncio.Queue
FINALIZATION_BUFFER_SECONDS = 3  # How long to wait after a minute ends before finalizing the aggregate
SYMBOL_REFRESH_INTERVAL_S = 3600 # 1 hour


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
        self.active_symbols = set(await fetch_usdt_symbols(self.session, self.market_type))
        while True:
            try:
                tasks = []
                tasks.append(self.refresh_symbols())
                tasks.append(self.run_ws())
                await asyncio.gather(*tasks)
            except Exception as e:
                print(f"Error encountered: {e}")
                await asyncio.sleep(30) # sleep for 30 seconds if anything happens then try again
    
    async def run_ws(self):
        while True:
            try:
                # the while loop ends if an "end" event is detected or if exception is encountered
                # end event is triggered when a symbol refresh indicates changes
                self.ws_tasks = []
                self.stop_events = []
                
                # Start new connections with the updated symbol list
                for i in range(0, len(self.active_symbols), MAX_STREAMS_PER_CONNECTION):
                    chunk = list(self.active_symbols)[i:i + MAX_STREAMS_PER_CONNECTION]
                    stop_event = asyncio.Event()
                    self.stop_events.append(stop_event)
                    self.ws_tasks.append(run_ws_connection(self.market_type, chunk, self.queue, stop_event))
                logging.info(f"[{self.market_type.upper()}] Spawned {len(self.ws_tasks)} new WebSocket tasks.")
                await asyncio.gather(*self.ws_tasks, return_exceptions=True)
            except Exception as e:
                print(f"Error encountered in websocket: {e}")
                await asyncio.sleep(30)

    async def refresh_symbols(self):
        while True:
            try:
                """Fetches the latest symbols, compares with current ones, and restarts connections if needed."""
                await asyncio.sleep(SYMBOL_REFRESH_INTERVAL_S)
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
                    
                self.active_symbols = new_symbols_set
            except Exception as e:
                print(f"Error encountered while refreshing symbols: {e}")


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
            await asyncio.sleep(10) # Avoid rapid-fire restarts on a persistent error


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
        if trade.is_market_maker:
            agg.pos_flow += trade.price * trade.quantity
            agg.pos_qty += trade.quantity
        else:
            agg.neg_flow += trade.price * trade.quantity
            agg.neg_qty += trade.quantity
        
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
            agg.pos_flow,
            agg.pos_qty,
            agg.neg_flow,
            agg.neg_qty
        )
        if market_type == 'spot':
            spot_rows.append(row_data)
        else:
            futures_rows.append(row_data)

    column_names = ['symbol', 'timestamp', 'start_atid', 'end_atid', 'pos_flow', 'pos_qty', 'neg_flow', 'neg_qty']
    
    try:
        if futures_rows:
            client.insert('binance_FLOWS_futures_base', futures_rows, column_names=column_names)
            logging.info(f"Flushed {len(futures_rows)} records to futures table.")
    except Exception as e:
        logging.error(f"ClickHouse insert failed: {e}")
    try:
        if spot_rows:
            client.insert('binance_FLOWS_spot_base', spot_rows, column_names=column_names)
            logging.info(f"Flushed {len(spot_rows)} records to spot table.")
    except Exception as e:
        logging.error(f"ClickHouse insert failed: {e}")


async def main():
    """The main entry point for the application."""
    logging.info("Initializing Binance USDT aggregated trade data stream...")
    
    # Initialize shared resources
    trade_queue = asyncio.Queue(maxsize=MAX_BUFFERED_RECORDS)
    
    try:
        clickhouse_client = get_clickhouse_client()
        # clickhouse_client.command("SELECT 1") # Test connection
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