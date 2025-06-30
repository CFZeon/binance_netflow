use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs::{self};//, OpenOptions};
// use std::io::Write;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use futures::StreamExt;
// use futures::future::join_all;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crossterm::{
    cursor::MoveTo,
    ExecutableCommand,
    terminal::{Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, size as terminal_size},
};
use std::io::stdout;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use std::path::PathBuf; // For better path manipulation
use clickhouse::sql::Identifier;
use clickhouse::{Client, Row}; // Import Inserter


#[derive(Row, Deserialize, Serialize)]
struct NetflowRow {
    symbol: String,
    timestamp: u64,
    start_atid: u64, 
    end_atid: u64,
    net_flow: f64
}

#[derive(Row, Deserialize, Serialize)]
struct NetflowGapsRow {
    symbol: String,
    timestamp_first: u64,
    start_atid_first: u64, 
    end_atid_first: u64,
    net_flow_first: f64,
    timestamp_second: u64,
    start_atid_second: u64, 
    end_atid_second: u64,
    net_flow_second: f64
}

// ------------------------
// Global constants & types
// ------------------------

static API_LOG_ENABLED: AtomicBool = AtomicBool::new(false);

const MAX_STREAMS_PER_CONNECTION: usize = 100;
const MAX_RECONNECT_ATTEMPTS: u32 = 5;
const BASE_RECONNECT_DELAY: u64 = 1;
const MAX_BUFFERED_RECORDS: usize = 1_000_000;
const MAX_TRADES_PER_REQUEST: u64 = 1000;
const RATE_LIMIT_REQUESTS_PER_SECOND_FUTURES: usize = 2;
const RATE_LIMIT_REQUESTS_PER_SECOND_SPOT: usize = 25;
const FINALIZATION_BUFFER_SECONDS: u64 = 3;

#[derive(Debug, Deserialize)]
struct AggTrade {
    s: String, // symbol
    p: String, // price (string)
    q: String, // quantity (string)
    m: bool,   // market maker flag
    #[serde(rename = "a")]
    agg_trade_id: u64,
    #[serde(rename = "T")]
    trade_timestamp: u64,
    #[serde(flatten)]
    _extra: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct WsAggTradeWrapper {
    data: AggTrade,
}

#[derive(Debug, Deserialize)]
struct RestAggTrade {
    #[serde(rename = "a")]
    agg_trade_id: u64,
    #[serde(rename = "p")]
    p: String,
    #[serde(rename = "q")]
    q: String,
    #[serde(rename = "T")]
    trade_timestamp: u64,
    #[serde(rename = "m")]
    m: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum MarketType {
    Futures,
    Spot,
}

#[derive(Debug, Clone)]
struct AggTradeAggregate {
    symbol: String,
    net_flow: f64,
    start_atid: Option<u64>,
    end_atid: Option<u64>,
    count: usize,
}

struct AggTradeTracker {
    last_trade_ids: HashMap<(String, MarketType), u64>,
}

impl AggTradeTracker {
    fn new() -> Self {
        Self {
            last_trade_ids: HashMap::new(),
        }
    }

    fn check_and_update(&mut self, symbol: &str, market: MarketType, agg_trade_id: u64) -> Option<(u64, u64)> {
        let key = (symbol.to_string(), market);
        let last_trade = self.last_trade_ids.entry(key).or_insert(agg_trade_id);
        if agg_trade_id > *last_trade + 1 {
            let start = *last_trade + 1;
            let end = agg_trade_id - 1;
            *last_trade = agg_trade_id;
            Some((start, end))
        } else {
            *last_trade = agg_trade_id.max(*last_trade);
            None
        }
    }
}

struct Metrics {
    current_batch_records: AtomicUsize,
    last_batch_records: AtomicUsize,
    last_batch_processing_time: Mutex<Duration>,
    start_time: Instant,
    missing_gaps: AtomicUsize,
    gaps_in_queue: AtomicUsize,
}

// ------------------------
// Logging Support & Utility Functions
// ------------------------

async fn push_log(log_buffer: &Arc<Mutex<VecDeque<String>>>, msg: String) {
    let mut buf = log_buffer.lock().await;
    buf.push_back(msg);
    if buf.len() > 100 {
        buf.pop_front();
    }
}

fn aggregate_missing_trades(trades: &[AggTrade]) -> BTreeMap<u64, AggTradeAggregate> {
    let mut aggregates = BTreeMap::new();
    for trade in trades {
        let trade_time_sec = trade.trade_timestamp / 1000;
        let minute = (trade_time_sec / 60) * 60;
        let price: f64 = trade.p.parse().unwrap_or(0.0);
        let quantity: f64 = trade.q.parse().unwrap_or(0.0);
        let net_flow = if trade.m { -price * quantity } else { price * quantity };

        let entry = aggregates.entry(minute).or_insert(AggTradeAggregate {
            symbol: trade.s.clone(),
            net_flow: 0.0,
            start_atid: None,
            end_atid: None,
            count: 0,
        });
        entry.net_flow += net_flow;
        entry.count += 1;
        if entry.start_atid.is_none() || trade.agg_trade_id < entry.start_atid.unwrap() {
            entry.start_atid = Some(trade.agg_trade_id);
        }
        if entry.end_atid.is_none() || trade.agg_trade_id > entry.end_atid.unwrap() {
            entry.end_atid = Some(trade.agg_trade_id);
        }
    }
    aggregates
}

async fn update_clickhouse(mut clickhouse_inserter: clickhouse::inserter::Inserter<NetflowRow>, new_aggregates: &BTreeMap<u64, AggTradeAggregate>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for (&minute, agg) in new_aggregates {
            clickhouse_inserter.write(&NetflowRow { 
                symbol: agg.symbol.clone(),
                timestamp: minute * 1000,
                start_atid: agg.start_atid.unwrap_or(0),
                end_atid: agg.end_atid.unwrap_or(0),
                net_flow: agg.net_flow,
            }).unwrap();
    }
    let stats = clickhouse_inserter.commit().await.unwrap();
    
    if stats.rows > 0 {
        println!(
            "backfilling: {} bytes, {} rows, {} transactions have been inserted",
            stats.bytes, stats.rows, stats.transactions,
        );
    }
    Ok(())
}

async fn handle_gaps_in_data(market: MarketType, clickhouse_client: clickhouse::Client, http_client: &reqwest::Client, rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>) {
    loop {
        // fetch new data from clickhouse between rows where the gap is > 1
        let market_type = market_str(market);
        let query_str = format!("SELECT
                    symbol,
                    first.1 AS timestamp_first,
                    first.2 AS start_atid_first,
                    first.3 AS end_atid_first,
                    first.4 AS net_flow_first,
                    second.1 AS timestamp_second,
                    second.2 AS start_atid_second,
                    second.3 AS end_atid_second,
                    second.4 AS net_flow_second
                FROM (
                    SELECT
                        symbol,
                        groupArray((timestamp, start_atid, end_atid, net_flow)) AS rows
                    FROM binance_NETFLOWS{}_base
                    GROUP BY symbol
                ) AS grouped
                ARRAY JOIN
                    arraySlice(rows, 1, length(rows) - 1) AS first,
                    arraySlice(rows, 2, length(rows) - 1) AS second
                WHERE second.2 - first.3 > 1 AND symbol = 'BTCUSDT'", market_type);
        let mut cursor = clickhouse_client
            .query(&query_str)
            .fetch::<NetflowGapsRow>().unwrap();

        // for each row, push it into an array
        let mut gaps = Vec::new();
        while let Some(row) = cursor.next().await.unwrap() {
            gaps.push((row.symbol, row.end_atid_first+1, row.start_atid_second-1));
        }
        let mut combined_missing_trades = Vec::new();
        for (symbol, missing_start, missing_end) in gaps {
            // push_log(&log_buffer, format!("Identified data gaps in {} {}: {} to {}", market_str_short(market), symbol, missing_start, missing_end)).await;
            let mut retry_delay = Duration::from_secs(5);
            loop {
                match fetch_missing_agg_trades(&http_client, &symbol, market, missing_start, missing_end, rate_limiter.clone()).await {
                    Ok(trades) => {
                        println!(
                            "[MEMORY_DEBUG] GAP FILL: Fetched {} trades for gap in symbol {}. About to extend Vec.",
                            trades.len(),
                            symbol
                        );
                        combined_missing_trades.extend(trades);
                        break;
                    }
                    Err(e) => {
                        // push_log(&log_buffer, format!("Error fetching missing trades for {} {}: {}. Retrying in {} seconds...", market_str(market), symbol, e, retry_delay.as_secs())).await;
                        if e.to_string().contains("429") {
                            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
                        } else {
                            retry_delay = Duration::from_secs(5);
                        }
                        tokio::time::sleep(retry_delay).await;
                    }
                }
            }
        }
        let new_aggregates = aggregate_missing_trades(&combined_missing_trades);
        
        let clickhouse_inserter: clickhouse::inserter::Inserter<NetflowRow>;
        if market == MarketType::Futures {
            clickhouse_inserter = clickhouse_client.inserter::<NetflowRow>("binance_NETFLOWS_futures_base").unwrap()
                .with_timeouts(Some(Duration::from_secs(30)), Some(Duration::from_secs(30)))
                .with_max_bytes(50_000_000)
                .with_max_rows(750_000)
                .with_period(Some(Duration::from_secs(30)));
        }
        else {
            clickhouse_inserter = clickhouse_client.inserter::<NetflowRow>("binance_NETFLOWS_spot_base").unwrap()
                .with_timeouts(Some(Duration::from_secs(30)), Some(Duration::from_secs(30)))
                .with_max_bytes(50_000_000)
                .with_max_rows(750_000)
                .with_period(Some(Duration::from_secs(30)));
        }

        // println!("{:#?}", new_aggregates);
        update_clickhouse(clickhouse_inserter, &new_aggregates).await.unwrap();
        // let (new_checkpoint, _) = scan_csv_for_gaps(&file_path)?;

        // sleep for 30 minutes before checking again
        tokio::time::sleep(Duration::from_secs(180)).await;
    }
}

async fn log_api_request(message: &str) {
    if !API_LOG_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    use tokio::io::AsyncWriteExt;
    let timestamp = Utc::now().to_rfc3339();
    let log_line = format!("{} - {}\n", timestamp, message);
    if let Ok(mut file) = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open("api_requests.log")
        .await
    {
        let _ = file.write_all(log_line.as_bytes()).await;
    }
}

// ------------------------
// Data Fetching & WebSocket Handling Functions
// ------------------------

async fn fetch_missing_agg_trades(
    client: &reqwest::Client,
    symbol: &str,
    market: MarketType,
    start_id: u64,
    end_id: u64,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
) -> Result<Vec<AggTrade>, Box<dyn std::error::Error + Send + Sync>> {
    let mut records = Vec::new();
    let mut current_start = start_id;
    while current_start <= end_id {
        rate_limiter.until_ready().await;
        let url = match market {
            MarketType::Futures => format!(
                "https://fapi.binance.com/fapi/v1/aggTrades?symbol={}&fromId={}&limit={}",
                symbol, current_start, MAX_TRADES_PER_REQUEST
            ),
            MarketType::Spot => format!(
                "https://api.binance.com/api/v3/aggTrades?symbol={}&fromId={}&limit={}",
                symbol, current_start, MAX_TRADES_PER_REQUEST
            ),
        };
        log_api_request(&format!("Sending request: {}", url)).await;
        let response = client.get(&url).send().await?;
        let status = response.status();
        let text = response.text().await?;
        log_api_request(&format!("Received response for {}: status {} Body: {}", url, status, text)).await;
        if !status.is_success() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP error {}: {}", status, text),
            )));
        }
        let json_value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to parse JSON: {}", e)))?;
        if !json_value.is_array() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Unexpected JSON format: {}", json_value),
            )));
        }
        let rest_records: Vec<RestAggTrade> = serde_json::from_value(json_value)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to deserialize agg trades: {}", e)))?;
        if rest_records.is_empty() {
            break;
        }
        if let Some(last_trade) = rest_records.last() {
            if last_trade.agg_trade_id > end_id {
                let filtered: Vec<RestAggTrade> = rest_records
                    .into_iter()
                    .filter(|r| r.agg_trade_id <= end_id)
                    .collect();
                for r in filtered {
                    records.push(AggTrade {
                        s: symbol.to_string(),
                        p: r.p,
                        q: r.q,
                        m: r.m,
                        agg_trade_id: r.agg_trade_id,
                        trade_timestamp: r.trade_timestamp,
                        _extra: serde_json::Value::Null,
                    });
                }
                break;
            }
            current_start = last_trade.agg_trade_id + 1;
        } else {
            break;
        }
        for r in rest_records {
            records.push(AggTrade {
                s: symbol.to_string(),
                p: r.p,
                q: r.q,
                m: r.m,
                agg_trade_id: r.agg_trade_id,
                trade_timestamp: r.trade_timestamp,
                _extra: serde_json::Value::Null,
            });
        }
    }
    Ok(records)
}

async fn fetch_usdt_symbols(client: &reqwest::Client, is_futures: bool) -> Result<Vec<String>, reqwest::Error> {
    let url = if is_futures {
        "https://fapi.binance.com/fapi/v1/exchangeInfo"
    } else {
        "https://api.binance.com/api/v3/exchangeInfo"
    };

    let exchange_info: serde_json::Value = client
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .json()
        .await?;

    Ok(exchange_info["symbols"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| {
            s["quoteAsset"].as_str() == Some("USDT")
                && s["status"].as_str() == Some("TRADING")
                && !s["symbol"].as_str().unwrap().contains("_")
        })
        .map(|s| s["symbol"].as_str().unwrap().to_lowercase())
        .collect())
}

fn market_str(market: MarketType) -> &'static str {
    match market {
        MarketType::Futures => "_futures",
        MarketType::Spot => "_spot",
    }
}

fn is_market_spot(market: MarketType) -> bool {
    match market {
        MarketType::Futures => false,
        MarketType::Spot => true,
    }
}

async fn refresh_symbols(
    market: MarketType,
    http_client: &reqwest::Client,
    current_symbols: Arc<Mutex<Vec<String>>>,
    ws_version_tx: &tokio::sync::watch::Sender<usize>,
    trade_sender: mpsc::Sender<(MarketType, AggTrade)>,
    websocket_semaphore: Arc<tokio::sync::Semaphore>,
    log_buffer: Arc<Mutex<VecDeque<String>>>,
) {
    let updated_symbols = match market {
        MarketType::Futures => fetch_usdt_symbols(http_client, true).await,
        MarketType::Spot => fetch_usdt_symbols(http_client, false).await,
    };

    if let Ok(updated_symbols) = updated_symbols {
        let mut symbols_lock = current_symbols.lock().await;
        if *symbols_lock != updated_symbols {
            let old_symbols = std::mem::replace(&mut *symbols_lock, updated_symbols.clone());

            let new_symbols: Vec<String> = symbols_lock
                .iter()
                .filter(|symbol| !old_symbols.contains(symbol))
                .cloned()
                .collect();
            let removed_symbols: Vec<String> = old_symbols
                .iter()
                .filter(|symbol| !symbols_lock.contains(symbol))
                .cloned()
                .collect();

            let new_version = ws_version_tx.borrow().wrapping_add(1);
            ws_version_tx.send(new_version).ok();

            spawn_ws_connections(market, updated_symbols, trade_sender.clone(), websocket_semaphore.clone(), ws_version_tx, log_buffer.clone()).await;

            if !new_symbols.is_empty() {
                push_log(&log_buffer, format!("Market {:?} - Found new symbols: {:?}", market, new_symbols)).await;
            }
            if !removed_symbols.is_empty() {
                push_log(&log_buffer, format!("Market {:?} - Removed symbols: {:?}", market, removed_symbols)).await;
            }
        }
    }
}

async fn run_ws_connection(
    market: MarketType,
    symbol_list: Vec<String>,
    trade_sender: mpsc::Sender<(MarketType, AggTrade)>,
    connection_semaphore: Arc<tokio::sync::Semaphore>,
    symbol_version_rx: tokio::sync::watch::Receiver<usize>,
    current_symbol_version: usize,
    log_buffer: Arc<Mutex<VecDeque<String>>>,
) {
    let mut reconnect_attempts = 0;
    let stream_paths = symbol_list
        .iter()
        .map(|symbol| format!("{}@aggTrade", symbol))
        .collect::<Vec<_>>()
        .join("/");
    let url = match market {
        MarketType::Futures => format!("wss://fstream.binance.com/stream?streams={}", stream_paths),
        MarketType::Spot => format!("wss://stream.binance.com:9443/stream?streams={}", stream_paths),
    };

    loop {
        if *symbol_version_rx.borrow() != current_symbol_version {
            break;
        }
        let permit = match connection_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        match connect_async(&url).await {
            Ok((stream, _)) => {
                reconnect_attempts = 0;
                let (_, mut incoming_messages) = stream.split();
                while let Some(message) = incoming_messages.next().await {
                    if *symbol_version_rx.borrow() != current_symbol_version {
                        break;
                    }
                    match message {
                        Ok(Message::Text(text)) => {
                            if let Ok(wrapper) = serde_json::from_str::<WsAggTradeWrapper>(&text) {
                                if let Err(e) = trade_sender.send((market, wrapper.data)).await {
                                    push_log(&log_buffer, format!("Trade channel send error: {}", e)).await;
                                }
                            } else {
                                push_log(&log_buffer, format!("WebSocket parse error: {}", text)).await;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            push_log(&log_buffer, format!("WebSocket error: {}", e)).await;
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                push_log(&log_buffer, format!("WebSocket connection failed: {} (attempt {})", e, reconnect_attempts)).await;
                let delay = BASE_RECONNECT_DELAY * 2u64.pow(reconnect_attempts);
                tokio::time::sleep(Duration::from_secs(delay)).await;
                reconnect_attempts = (reconnect_attempts + 1).min(MAX_RECONNECT_ATTEMPTS);
            }
        }
        drop(permit);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn spawn_ws_connections(
    market: MarketType,
    symbols: Vec<String>,
    ws_trade_sender: mpsc::Sender<(MarketType, AggTrade)>,
    websocket_semaphore: Arc<tokio::sync::Semaphore>,
    ws_version_tx: &tokio::sync::watch::Sender<usize>,
    log_buffer: Arc<Mutex<VecDeque<String>>>,
) {
    let version_rx = ws_version_tx.subscribe();
    let current_version = *version_rx.borrow();
    for symbols_chunk in symbols.chunks(MAX_STREAMS_PER_CONNECTION * 5) {
        let symbols_chunk = symbols_chunk.to_vec();
        let trade_sender_inner = ws_trade_sender.clone();
        let ws_semaphore_inner = websocket_semaphore.clone();
        let version_rx_clone = ws_version_tx.subscribe();
        let log_buf_clone = log_buffer.clone();

        tokio::spawn(async move {
            run_ws_connection(
                market,
                symbols_chunk,
                trade_sender_inner,
                ws_semaphore_inner,
                version_rx_clone,
                current_version,
                log_buf_clone,
            )
            .await;
        });
    }
}

// ------------------------
// UI Rendering Task
// ------------------------

// async fn ui_render_loop(
//     metrics: Arc<Metrics>,
//     futures_symbols_list: Arc<Mutex<Vec<String>>>,
//     spot_symbols_list: Arc<Mutex<Vec<String>>>,
//     next_symbol_refresh_time: Arc<Mutex<SystemTime>>,
//     log_buffer: Arc<Mutex<VecDeque<String>>>,
// ) -> crossterm::Result<()> {
//     let mut stdout = stdout();
//     loop {
//         let (cols, rows) = terminal_size()?;
//         stdout.execute(Clear(ClearType::All))?;
//         let logs = {
//             let buf = log_buffer.lock().await;
//             let total = buf.len();
//             let start = if total > (rows as usize - 1) { total - (rows as usize - 1) } else { 0 };
//             buf.iter().skip(start).cloned().collect::<Vec<_>>()
//         };
//         for (i, line) in logs.iter().enumerate() {
//             stdout.execute(MoveTo(0, i as u16))?;
//             write!(stdout, "{:<width$}", line, width = cols as usize)?;
//         }
//         let elapsed = Instant::now().duration_since(metrics.start_time);
//         let formatted_elapsed = format_duration_seconds(elapsed);
//         let last_batch = metrics.last_batch_records.load(Ordering::Relaxed);
//         let proc_time = *metrics.last_batch_processing_time.lock().await;
//         let formatted_proc_time = format_duration_millis(proc_time);
//         let missing = metrics.missing_gaps.load(Ordering::Relaxed);
//         let gaps = metrics.gaps_in_queue.load(Ordering::Relaxed);
//         let next_refresh_time = {
//             let lock = next_symbol_refresh_time.lock().await;
//             *lock
//         };
//         let now_sys = SystemTime::now();
//         let countdown = if next_refresh_time > now_sys {
//             next_refresh_time.duration_since(now_sys).unwrap()
//         } else {
//             Duration::from_secs(0)
//         };
//         let mins = countdown.as_secs() / 60;
//         let secs = countdown.as_secs() % 60;
//         let futures_count = futures_symbols_list.lock().await.len();
//         let spot_count = spot_symbols_list.lock().await.len();
//         let status_line = format!(
//             "Elapsed: {} | Last Batch: {} records | Proc Time: {} | Missing Gaps: {} | Gaps in Queue: {} | Refresh in: {:02}m:{:02}s | Futures: {} | Spot: {}",
//             formatted_elapsed, last_batch, formatted_proc_time, missing, gaps, mins, secs, futures_count, spot_count
//         );
//         stdout.execute(MoveTo(0, rows - 1))?;
//         write!(stdout, "{:<width$}", status_line, width = cols as usize)?;
//         stdout.flush()?;
//         tokio::time::sleep(Duration::from_millis(500)).await;
//     }
// }

// ------------------------
// Main Entry Point
// ------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let mut stdout = stdout();
    // stdout.execute(EnterAlternateScreen)?;
    
    // Create shared log buffer.
    let log_buffer = Arc::new(Mutex::new(VecDeque::new()));
    // let ui_log_buffer = log_buffer.clone();

    // Process command-line arguments.
    let args: Vec<String> = env::args().collect();
    let mut disable_api_log = true;
    for arg in args.iter().skip(1) {
        match arg.as_str() {
            "--disable-api-log" => disable_api_log = true,
            _ => {
                eprintln!("Error: unrecognized argument: {}", arg);
                eprintln!("Usage: {} [--disable-api-log]", args[0]);
                std::process::exit(1);
            }
        }
    }
    if disable_api_log {
        API_LOG_ENABLED.store(false, Ordering::Relaxed);
    }

    push_log(&log_buffer, "Initializing Binance USDT aggregated trade data stream...".to_string()).await;

    let process_start_unix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let next_minute_boundary = ((process_start_unix / 60) + 1) * 60;
    push_log(&log_buffer, format!("Process start UNIX time: {}. Next full minute boundary: {}", process_start_unix, next_minute_boundary)).await;

    let (trade_data_sender, mut trade_data_receiver) = mpsc::channel::<(MarketType, AggTrade)>(MAX_BUFFERED_RECORDS);
    let trade_aggregates = Arc::new(Mutex::new(BTreeMap::<(String, MarketType, u64), AggTradeAggregate>::new()));
    let agg_trade_tracker = Arc::new(Mutex::new(AggTradeTracker::new()));
    let http_client = reqwest::Client::new();

    let spot_rate_limiter = Arc::new(RateLimiter::direct(
        Quota::per_second(NonZeroU32::new(RATE_LIMIT_REQUESTS_PER_SECOND_SPOT as u32).unwrap())
    ));
    let futures_rate_limiter = Arc::new(RateLimiter::direct(
        Quota::per_second(NonZeroU32::new(RATE_LIMIT_REQUESTS_PER_SECOND_FUTURES as u32).unwrap())
    ));

    let missing_trade_sender = trade_data_sender.clone();

    let metrics = Arc::new(Metrics {
        current_batch_records: AtomicUsize::new(0),
        last_batch_records: AtomicUsize::new(0),
        last_batch_processing_time: Mutex::new(Duration::from_secs(0)),
        start_time: Instant::now(),
        missing_gaps: AtomicUsize::new(0),
        gaps_in_queue: AtomicUsize::new(0),
    });

    let futures_symbols_list = Arc::new(Mutex::new(Vec::<String>::new()));
    let spot_symbols_list = Arc::new(Mutex::new(Vec::<String>::new()));
    let (futures_ws_version_tx, _) = tokio::sync::watch::channel(0);
    let (spot_ws_version_tx, _) = tokio::sync::watch::channel(0);
    let next_symbol_refresh_time = Arc::new(Mutex::new(SystemTime::now() + Duration::from_secs(3600)));

    // Spawn UI rendering loop.
    // {
    //     let metrics_clone = metrics.clone();
    //     let futures_symbols_list_clone = futures_symbols_list.clone();
    //     let spot_symbols_list_clone = spot_symbols_list.clone();
    //     let next_symbol_refresh_time_clone = next_symbol_refresh_time.clone();
    //     let ui_log_buffer_clone = ui_log_buffer.clone();
    //     tokio::spawn(async move {
    //         if let Err(e) = ui_render_loop(metrics_clone, futures_symbols_list_clone, spot_symbols_list_clone, next_symbol_refresh_time_clone, ui_log_buffer_clone).await {
    //             eprintln!("UI render loop error: {}", e);
    //         }
    //     });
    // }
    // 1. Read the secret folder path from environment variable
    // Using std::env::var which returns a Result
    let secret_folder_path_str = env::var("SECRETFOLDER")
        .map_err(|e| format!("Failed to read SECRETFOLDER env var: {}", e))?;

    // Convert the string path to a PathBuf for easier joining
    let secret_folder_path = PathBuf::from(secret_folder_path_str);

    // 2. Read the IP address file
    let ip_file_path = secret_folder_path.join("CLICKHOUSEIP.txt");
    let ip_address = fs::read_to_string(&ip_file_path)
        .map_err(|e| format!("Unable to read IP file {:?}: {}", ip_file_path, e))?
        .trim() // Remove leading/trailing whitespace (like newlines)
        .to_string(); // Convert back to String

    if ip_address.is_empty() {
        return Err("IP address read from file is empty after trimming".into());
    }

    // 3. Read the password file
    let pass_file_path = secret_folder_path.join("CLICKHOUSEPASS.txt");
    let clickhouse_password = fs::read_to_string(&pass_file_path)
        .map_err(|e| format!("Unable to read password file {:?}: {}", pass_file_path, e))?
        .trim() // Remove leading/trailing whitespace
        .to_string(); // Convert back to String

    // Use format! for cleaner string construction
    let url = format!("http://{}:8123", ip_address);

    let clickhouse_client = Client::default()
        .with_url(url) // Use the correctly constructed URL
        .with_user("default")
        .with_password(clickhouse_password); // Use the trimmed password

    // Spawn task for processing trades.
    {
        let start_minute_boundary = next_minute_boundary;
        let trade_aggregates = trade_aggregates.clone();
        // let csv_writers = csv_writers.clone();
        let metrics_clone = metrics.clone();
        let agg_trade_tracker = agg_trade_tracker.clone();
        let http_client = http_client.clone();
        let futures_rate_limiter = futures_rate_limiter.clone();
        let spot_rate_limiter = spot_rate_limiter.clone();
        let missing_trade_sender = missing_trade_sender.clone();
        let proc_log_buffer = log_buffer.clone();


        let mut clickhouse_inserter = clickhouse_client.inserter::<NetflowRow>("binance_NETFLOWS_futures_base")?
            .with_timeouts(Some(Duration::from_secs(30)), Some(Duration::from_secs(30)))
            .with_max_bytes(50_000_000)
            .with_max_rows(750_000)
            .with_period(Some(Duration::from_secs(30)));

        let mut clickhouse_inserter_spot = clickhouse_client.inserter::<NetflowRow>("binance_NETFLOWS_spot_base")?
            .with_timeouts(Some(Duration::from_secs(30)), Some(Duration::from_secs(30)))
            .with_max_bytes(50_000_000)
            .with_max_rows(750_000)
            .with_period(Some(Duration::from_secs(30)));
        
        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(Duration::from_secs(6));
            loop {
                tokio::select! {
                    Some((market, trade_data)) = trade_data_receiver.recv() => {
                        // Destructure the incoming trade to avoid repeated cloning of the symbol.
                        let AggTrade { s, p, q, m, agg_trade_id, trade_timestamp, _extra } = trade_data;
                        let trade_time_sec = trade_timestamp / 1000;
                        let trade_minute = (trade_time_sec / 60) * 60;
                        if trade_minute < start_minute_boundary {
                            continue;
                        }
                        let price: f64 = p.parse().unwrap_or(0.0);
                        let quantity: f64 = q.parse().unwrap_or(0.0);
                        let net_flow = if m { -price * quantity } else { price * quantity };

                        // Clone the symbol once for use as a key.
                        let symbol_for_key = s.clone();
                        {
                            let mut aggregates_lock = trade_aggregates.lock().await;
                            let aggregate_entry = aggregates_lock.entry((symbol_for_key, market, trade_minute))
                                .or_insert(AggTradeAggregate {
                                    symbol: s.clone(),
                                    net_flow: 0.0,
                                    start_atid: None,
                                    end_atid: None,
                                    count: 0,
                                });
                            aggregate_entry.net_flow += net_flow;
                            aggregate_entry.count += 1;
                            if aggregate_entry.start_atid.is_none() || agg_trade_id < aggregate_entry.start_atid.unwrap() {
                                aggregate_entry.start_atid = Some(agg_trade_id);
                            }
                            if aggregate_entry.end_atid.is_none() || agg_trade_id > aggregate_entry.end_atid.unwrap() {
                                aggregate_entry.end_atid = Some(agg_trade_id);
                            }
                        }
                        metrics_clone.current_batch_records.fetch_add(1, Ordering::Relaxed);

                        let mut tracker_lock = agg_trade_tracker.lock().await;
                        if let Some((missing_start, missing_end)) = tracker_lock.check_and_update(&s, market, agg_trade_id) {
                            push_log(&proc_log_buffer, format!("Detected gap in {} {}: {} to {}", market_str(market), s, missing_start, missing_end)).await;
                            metrics_clone.missing_gaps.fetch_add(1, Ordering::Relaxed);
                            metrics_clone.gaps_in_queue.fetch_add(1, Ordering::Relaxed);
                            let http_client_inner = http_client.clone();
                            let symbol_for_fetch = s.clone();
                            let rate_limiter = match market {
                                MarketType::Futures => futures_rate_limiter.clone(),
                                MarketType::Spot => spot_rate_limiter.clone(),
                            };
                            let missing_trade_sender_clone = missing_trade_sender.clone();
                            let metrics_inner = metrics_clone.clone();
                            let proc_log_buffer_inner = proc_log_buffer.clone();
                            tokio::spawn(async move {
                                let mut retry_delay = Duration::from_secs(5);
                                loop {
                                    match fetch_missing_agg_trades(&http_client_inner, &symbol_for_fetch, market, missing_start, missing_end, rate_limiter.clone()).await {
                                        Ok(trades) => {
                                            for trade in trades {
                                                if let Err(e) = missing_trade_sender_clone.send((market, trade)).await {
                                                    push_log(&proc_log_buffer_inner, format!("Failed to send missing trade: {}", e)).await;
                                                }
                                            }
                                            metrics_inner.gaps_in_queue.fetch_sub(1, Ordering::Relaxed);
                                            metrics_inner.missing_gaps.fetch_sub(1, Ordering::Relaxed);
                                            break;
                                        },
                                        Err(e) => {
                                            push_log(&proc_log_buffer_inner, format!("Failed to fetch missing trades: {}. Retrying in {} seconds...", e, retry_delay.as_secs())).await;
                                            if e.to_string().contains("429") {
                                                retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(60));
                                            } else {
                                                retry_delay = Duration::from_secs(5);
                                            }
                                            tokio::time::sleep(retry_delay).await;
                                        }
                                    }
                                }
                            });
                        }
                    },
                    _ = flush_interval.tick() => {
                        let channel_len = trade_data_receiver.len();
                        // Log only if the buffer is getting noticeably full to avoid spam
                        if channel_len > 100_000 {
                            println!(
                                "[MEMORY_DEBUG] MPSC channel buffer is high: {} / {}",
                                channel_len, MAX_BUFFERED_RECORDS
                            );
                        }

                        { // Scoped lock to check map size
                            let aggregates_lock = trade_aggregates.lock().await;
                            let map_size = aggregates_lock.len();
                            if map_size > 50_000 { // Log if map size is unusually large
                                println!("[MEMORY_DEBUG] Aggregates BTreeMap size is high: {}", map_size);
                            }
                        }
                        let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        let mut aggregates_to_flush = Vec::new();
                        {
                            let aggregates_lock = trade_aggregates.lock().await;
                            for (key, _) in aggregates_lock.iter() {
                                let &(_, _, minute_ts) = key;
                                if minute_ts + 60 + FINALIZATION_BUFFER_SECONDS <= now_unix {
                                    aggregates_to_flush.push(key.clone());
                                }
                            }
                        }
                        if !aggregates_to_flush.is_empty() {
                            let flush_start = Instant::now();
                            let mut batch_record_count = 0;
                            for key in aggregates_to_flush {
                                if let Some(aggregate) = trade_aggregates.lock().await.remove(&key) {
                                    batch_record_count += aggregate.count;
                                    let (symbol, market, minute_ts) = key;
                                    let is_spot = is_market_spot(market);
                                    if is_spot {
                                        clickhouse_inserter_spot.write(&NetflowRow { 
                                            symbol: symbol,
                                            timestamp: minute_ts * 1000, 
                                            start_atid: aggregate.start_atid.unwrap_or(0), 
                                            end_atid: aggregate.end_atid.unwrap_or(0), 
                                            net_flow: aggregate.net_flow}).unwrap();
                                    }
                                    else {
                                        clickhouse_inserter.write(&NetflowRow { 
                                            symbol: symbol, 
                                            timestamp: minute_ts * 1000,
                                            start_atid: aggregate.start_atid.unwrap_or(0), 
                                            end_atid: aggregate.end_atid.unwrap_or(0), 
                                            net_flow: aggregate.net_flow}).unwrap();
                                    }
                                }
                            }
                            let stats = clickhouse_inserter.commit().await.unwrap();
                            if stats.rows > 0 {
                                println!(
                                    "{} bytes, {} rows, {} transactions have been inserted",
                                    stats.bytes, stats.rows, stats.transactions,
                                );
                            }
                            let spot_stats = clickhouse_inserter_spot.commit().await.unwrap();
                            if spot_stats.rows > 0 {
                                println!(
                                    "{} bytes, {} rows, {} transactions have been inserted",
                                    spot_stats.bytes, spot_stats.rows, spot_stats.transactions,
                                );
                            }
                            let flush_duration = Instant::now().duration_since(flush_start);
                            *metrics_clone.last_batch_processing_time.lock().await = flush_duration;
                            metrics_clone.last_batch_records.store(batch_record_count, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }

    let (ws_trade_sender, mut ws_trade_receiver) = mpsc::channel(MAX_BUFFERED_RECORDS);
    let websocket_semaphore = Arc::new(tokio::sync::Semaphore::new(10));

    {
        // probably the thread for updating and removing symbols
        let http_client = http_client.clone();
        let futures_symbols_list = futures_symbols_list.clone();
        let spot_symbols_list = spot_symbols_list.clone();
        let futures_ws_version_tx = futures_ws_version_tx.clone();
        let spot_ws_version_tx = spot_ws_version_tx.clone();
        let ws_log_buffer = log_buffer.clone();
        push_log(&ws_log_buffer, "Fetching futures symbols...".to_string()).await;
        let futures_symbols_fetched = fetch_usdt_symbols(&http_client, true).await.unwrap_or_else(|e| {
            futures::executor::block_on(push_log(&ws_log_buffer, format!("Error fetching futures symbols: {}", e)));
            vec![]
        });
        {
            let mut futures_list_lock = futures_symbols_list.lock().await;
            *futures_list_lock = futures_symbols_fetched.clone();
        }
        push_log(&ws_log_buffer, "Fetching spot symbols...".to_string()).await;
        let spot_symbols_fetched = fetch_usdt_symbols(&http_client, false).await.unwrap_or_else(|e| {
            futures::executor::block_on(push_log(&ws_log_buffer, format!("Error fetching spot symbols: {}", e)));
            vec![]
        });
        {
            let mut spot_list_lock = spot_symbols_list.lock().await;
            *spot_list_lock = spot_symbols_fetched.clone();
        }
        spawn_ws_connections(MarketType::Futures, futures_symbols_fetched, ws_trade_sender.clone(), websocket_semaphore.clone(), &futures_ws_version_tx, ws_log_buffer.clone()).await;
        spawn_ws_connections(MarketType::Spot, spot_symbols_fetched, ws_trade_sender.clone(), websocket_semaphore.clone(), &spot_ws_version_tx, ws_log_buffer.clone()).await;
    }

    {
        let http_client = http_client.clone();
        let futures_symbols_list = futures_symbols_list.clone();
        let spot_symbols_list = spot_symbols_list.clone();
        let futures_ws_version_tx = futures_ws_version_tx.clone();
        let spot_ws_version_tx = spot_ws_version_tx.clone();
        let next_symbol_refresh_time = next_symbol_refresh_time.clone();
        let trade_sender_clone = ws_trade_sender.clone();
        let websocket_semaphore = websocket_semaphore.clone();
        let refresh_log_buffer = log_buffer.clone();
        tokio::spawn(async move {
            loop {
                let now = SystemTime::now();
                let now_secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
                let next_hour = ((now_secs / 3600) + 1) * 3600;
                let refresh_time = SystemTime::UNIX_EPOCH + Duration::from_secs(next_hour);
                {
                    let mut refresh_lock = next_symbol_refresh_time.lock().await;
                    *refresh_lock = refresh_time;
                }
                let wait_duration = refresh_time.duration_since(SystemTime::now()).unwrap_or(Duration::from_secs(0));
                tokio::time::sleep(wait_duration).await;
                refresh_symbols(MarketType::Futures, &http_client, futures_symbols_list.clone(), &futures_ws_version_tx, trade_sender_clone.clone(), websocket_semaphore.clone(), refresh_log_buffer.clone()).await;
                refresh_symbols(MarketType::Spot, &http_client, spot_symbols_list.clone(), &spot_ws_version_tx, trade_sender_clone.clone(), websocket_semaphore.clone(), refresh_log_buffer.clone()).await;
            }
        });
    }

    {
        let temp_clickhouse_client = clickhouse_client.clone();
        let temp_http_client = http_client.clone();
        // this part here will be for fetching gaps in the data, fetching the data from binance, then updating the rows
        tokio::spawn(async move {
            loop {
                let futures_rate_limiter = futures_rate_limiter.clone();
                handle_gaps_in_data(MarketType::Futures, temp_clickhouse_client.clone(), &temp_http_client, futures_rate_limiter).await;

                // sleep for 30 minutes before checking again
                tokio::time::sleep(Duration::from_secs(180)).await;
            }
        });
    }

    {
        // this part here will be for fetching gaps in the data, fetching the data from binance, then updating the rows
        tokio::spawn(async move {
            loop {
                let spot_rate_limiter = spot_rate_limiter.clone();
                handle_gaps_in_data(MarketType::Spot, clickhouse_client.clone(), &http_client, spot_rate_limiter).await;

                // sleep for 30 minutes before checking again
                tokio::time::sleep(Duration::from_secs(180)).await;
            }
        });
    }

    while let Some((market, trade_data)) = ws_trade_receiver.recv().await {
        if let Err(e) = trade_data_sender.send((market, trade_data)).await {
            push_log(&log_buffer, format!("Trade data channel error: {}", e)).await;
        }
    }

    // stdout.execute(LeaveAlternateScreen)?;
    Ok(())
}
