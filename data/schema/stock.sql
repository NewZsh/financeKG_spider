CREATE TABLE IF NOT EXISTS stocks (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    board TEXT NOT NULL,
    is_st INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stocks_board_is_st
    ON stocks(board, is_st);

CREATE TABLE IF NOT EXISTS latest_market_value (
    code TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL,
    amount_wan REAL,
    float_mv_yi REAL NOT NULL,
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE TABLE IF NOT EXISTS daily_bars (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (code, trade_date),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE INDEX IF NOT EXISTS idx_daily_bars_code_date
    ON daily_bars(code, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_bars_trade_date
    ON daily_bars(trade_date DESC);

CREATE TABLE IF NOT EXISTS intraday_bars (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    trade_time TEXT NOT NULL,
    trade_timestamp TEXT NOT NULL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    avg_price REAL,
    volume REAL,
    amount REAL,
    change_pct REAL,
    change_amount REAL,
    PRIMARY KEY (code, trade_timestamp),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE INDEX IF NOT EXISTS idx_intraday_bars_code_date_time
    ON intraday_bars(code, trade_date DESC, trade_timestamp DESC);

CREATE TABLE IF NOT EXISTS daily_price_distributions (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    buy_sell_bins_json TEXT NOT NULL,
    price_histogram_json TEXT NOT NULL,
    PRIMARY KEY (code, trade_date),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_distributions_code_date
    ON daily_price_distributions(code, trade_date DESC);

CREATE TABLE IF NOT EXISTS indicator_snapshots (
    run_date TEXT NOT NULL,
    code TEXT NOT NULL,
    score REAL NOT NULL,
    cross_type TEXT,
    cross_type_cn TEXT,
    vol_ratio REAL,
    angle REAL,
    rsi REAL,
    bias REAL,
    space_to_high REAL,
    ma5 REAL,
    ma20 REAL,
    bullish_alignment INTEGER NOT NULL DEFAULT 0,
    signals TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_date, code),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE INDEX IF NOT EXISTS idx_indicator_snapshots_run_date_score
    ON indicator_snapshots(run_date, score DESC, code ASC);

CREATE TABLE IF NOT EXISTS review_runs (
    review_date TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    universe_count INTEGER,
    candidate_count INTEGER,
    analyzed_count INTEGER,
    signal_count INTEGER,
    markdown_path TEXT,
    csv_path TEXT,
    notes TEXT
);
