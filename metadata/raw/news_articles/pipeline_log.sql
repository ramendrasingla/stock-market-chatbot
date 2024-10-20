DROP TABLE IF EXISTS pipeline_log;
CREATE TABLE pipeline_log (
    ticker TEXT NOT NULL,
    oldest_published_date DATETIME NOT NULL,
    latest_published_date DATETIME NOT NULL,
    UNIQUE(ticker, oldest_published_date, latest_published_date)
);