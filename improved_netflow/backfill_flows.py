import aiohttp
import json
from structs import AggTrade, FlowRow, AggTradeAggregate
from aiolimiter import AsyncLimiter
import asyncio
import logging
import clickhouse_connect
from logging_utils import log_api_request

from collections import defaultdict

GAP_CHECK_INTERVAL_S = 180 # How often to query ClickHouse for historical gaps
RATE_LIMIT_FUTURES = AsyncLimiter(2, 1)  # A safe, low number
RATE_LIMIT_SPOT = AsyncLimiter(25, 1) # A generous limit for multiple requests
MAX_TRADES_PER_REQUEST = 1000

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

async def handle_gap_filling(market_type: str, client: clickhouse_connect.driver.Client, session: aiohttp.ClientSession):
    """
    Periodically queries ClickHouse to find gaps in trade data and fetches
    the missing data from Binance REST API.
    """
    rate_limiter = RATE_LIMIT_FUTURES if market_type == 'futures' else RATE_LIMIT_SPOT
    table_name = f'binance_FLOWS_{market_type}_base'
    
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
    ORDER BY symbol, start_atid
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
                        # await process_backfilled_trades(trades, client, market_type)
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
    table_name = f'binance_FLOWS_{market_type}_base'
    
    column_names = ['symbol', 'timestamp', 'start_atid', 'end_atid', 'pos_flow', 'pos_qty', 'neg_flow', 'neg_qty']
    
    # for each trade, convert timestamp to start of minute
    # fetch that trade from clickhouse if not already fetched
    # add to the aggregates for that row
    # if timestamp falls on new minute, flush the row to clickhouse
    for trade in trades:
        trade_minute_ts = (trade.trade_timestamp_ms // 1000 // 60) * 60
        if not (trade.symbol.upper(), trade_minute_ts) in aggregates:
            # fetch new row
            row_buffer = client.query(f"""SELECT * FROM {table_name} WHERE symbol = '{trade.symbol.upper()}' AND timestamp = {trade_minute_ts}""").result_rows
            if len(row_buffer) > 0:
                row_buffer = row_buffer[0]
                aggregates[(trade.symbol.upper(), trade_minute_ts)] = AggTradeAggregate(symbol=trade.symbol.upper(), start_atid=row_buffer[2], 
                                                                                    end_atid=row_buffer[3], pos_flow=row_buffer[4], 
                                                                                    pos_qty=row_buffer[5], neg_flow=row_buffer[6], 
                                                                                    neg_qty=row_buffer[7])
            else:
                aggregates[(trade.symbol.upper(), trade_minute_ts)] = AggTradeAggregate(symbol=trade.symbol.upper())
        agg_key = (trade.symbol.upper(), trade_minute_ts)
        
        agg = aggregates[agg_key]
        agg.symbol = trade.symbol.upper()
        if trade.is_market_maker:
            agg.neg_flow += trade.price * trade.quantity
            agg.neg_qty += trade.quantity
        else:
            agg.pos_flow += trade.price * trade.quantity
            agg.pos_qty += trade.quantity
        
        if agg.start_atid is None or trade.agg_trade_id < agg.start_atid:
            agg.start_atid = trade.agg_trade_id
        if agg.end_atid is None or trade.agg_trade_id > agg.end_atid:
            agg.end_atid = trade.agg_trade_id
            
    if not aggregates:
        return
        
    rows_to_insert = [
        FlowRow(
            symbol=agg.symbol.upper(),
            timestamp=key[1] * 1000, # to ms
            start_atid=agg.start_atid,
            end_atid=agg.end_atid,
            pos_flow=agg.pos_flow,
            pos_qty=agg.pos_qty,
            neg_flow=agg.neg_flow,
            neg_qty=agg.neg_qty
        ) for key, agg in aggregates.items()
    ]
    
    try:
        data = [list(row.__dict__.values()) for row in rows_to_insert]
        client.insert(table_name, data, column_names=column_names)
        logging.info(f"[{market_type.upper()}] Inserted {len(rows_to_insert)} backfilled records into {table_name}.")
    except Exception as e:
        logging.error(f"[{market_type.upper()}] Failed to insert backfilled data into ClickHouse: {e}")