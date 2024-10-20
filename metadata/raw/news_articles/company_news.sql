DROP TABLE IF EXISTS company_news;
CREATE TABLE company_news (
    ticker TEXT NOT NULL,
    ticker_id TEXT NOT NULL,
    title TEXT,
    content TEXT,
    published_date DATETIME NOT NULL,
    UNIQUE(title, published_date)
);