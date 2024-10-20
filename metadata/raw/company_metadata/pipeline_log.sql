DROP TABLE IF EXISTS pipeline_log;
CREATE TABLE pipeline_log (
    ticker TEXT NOT NULL,
    last_run DATETIME NOT NULL,
    UNIQUE(ticker, last_run)
);