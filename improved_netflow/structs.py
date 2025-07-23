from dataclasses import dataclass

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
    pos_flow: float = 0.0
    pos_qty: float = 0.0
    neg_flow: float = 0.0
    neg_qty: float = 0.0
    start_atid: int | None = None
    end_atid: int | None = None
    count: int = 0
    
@dataclass
class FlowRow:
    """Represents a row to be inserted into ClickHouse."""
    symbol: str
    timestamp: int # ms
    start_atid: int
    end_atid: int
    pos_flow: float # net flow is price * qty, we store the whole qty to rederive average price
    pos_qty: float
    neg_flow: float
    neg_qty: float