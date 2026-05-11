-- 股票基础信息表，存储股票代码、名称、板块等基本信息
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS code_sync_status;

CREATE TABLE IF NOT EXISTS stocks (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    board TEXT NOT NULL,
    is_st INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

-- 股票表的板块和ST状态联合索引
CREATE INDEX IF NOT EXISTS idx_stocks_board_st ON stocks(board, is_st);

-- 股票最新市值表，存储每只股票的最新流通市值等信息
CREATE TABLE IF NOT EXISTS latest_market_value (
    code TEXT PRIMARY KEY,
    trade_date TEXT NOT NULL,
    float_mv_yi REAL NOT NULL,
    FOREIGN KEY (code) REFERENCES stocks(code)
);

-- 日线行情表，仅保留前复权口径
CREATE TABLE IF NOT EXISTS daily_bars (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    amount REAL,
    PRIMARY KEY (code, trade_date),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

CREATE INDEX IF NOT EXISTS idx_daily_bars_code_date
    ON daily_bars(code, trade_date DESC);

-- 分时行情表，存储每只股票的分时价格、成交量等信息
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

-- 分时行情表的索引，便于按股票、日期、时间倒序查询
CREATE INDEX IF NOT EXISTS idx_intraday_bars_code_date_time
    ON intraday_bars(code, trade_date DESC, trade_timestamp DESC);

-- 每日价格分布表，存储每日的价格分布、买卖盘分布等信息
CREATE TABLE IF NOT EXISTS daily_price_distributions (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    buy_sell_bins_json TEXT NOT NULL,
    price_histogram_json TEXT NOT NULL,
    PRIMARY KEY (code, trade_date),
    FOREIGN KEY (code) REFERENCES stocks(code)
);

-- 每日价格分布表的索引，便于按股票、日期倒序查询
CREATE INDEX IF NOT EXISTS idx_daily_price_distributions_code_date
    ON daily_price_distributions(code, trade_date DESC);






-- 指标快照表，存储每日各类技术指标的快照信息
CREATE TABLE IF NOT EXISTS indicator_snapshots (
    run_date TEXT NOT NULL,
    code TEXT NOT NULL,
    score REAL NOT NULL,
    cross_type TEXT NOT NULL,
    cross_type_cn TEXT NOT NULL,
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

-- 指标快照表的索引，便于按日期、分数倒序查询
CREATE INDEX IF NOT EXISTS idx_indicator_snapshots_run_date_score
    ON indicator_snapshots(run_date, score DESC);

-- 回测运行记录表，存储每次回测的运行状态、结果等信息
CREATE TABLE IF NOT EXISTS review_runs (
    review_date TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    universe_count INTEGER,
    candidate_count INTEGER,
    analyzed_count INTEGER,
    signal_count INTEGER,
    markdown_path TEXT,
    csv_path TEXT,
    notes TEXT
);