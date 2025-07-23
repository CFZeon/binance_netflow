def createFlowsTableWithSymbol(client, source, marketType, timezone='UTC'):
    # Create the table
    query = f"""
    CREATE TABLE IF NOT EXISTS {source}_FLOWS_{marketType}_base (
        symbol String,
        timestamp DateTime64(3, '{timezone}'),
        start_atid Int64, 
        end_atid Int64,
        pos_flow Float64,
        pos_qty Float64,
        neg_flow Float64,
        neg_qty Float64
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toStartOfWeek(timestamp)
    ORDER BY (symbol, timestamp);
    """

    client.command(query)
    return