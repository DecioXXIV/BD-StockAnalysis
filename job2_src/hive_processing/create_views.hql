DROP TABLE IF EXISTS fl_dates;

CREATE TABLE fl_dates AS
SELECT p.ticker, i.industry, i.sector, p.year, MIN(p.`date`) as first_date, MAX(p.`date`) as last_date
FROM prices p 
JOIN infos i ON p.ticker = i.ticker
WHERE i.industry <> '' AND i.sector <> ''
GROUP BY p.ticker, i.industry, i.sector, p.year;

DROP TABLE IF EXISTS fl_prices;

CREATE TABLE fl_prices AS
SELECT d.ticker, d.industry, d.sector, d.year, p1.close as first_close, p2.close as last_close
FROM fl_dates d
JOIN prices p1 ON d.ticker = p1.ticker AND d.first_date = p1.`date`
JOIN prices p2 ON d.ticker = p2.ticker AND d.last_date = p2.`date`;

DROP TABLE IF EXISTS stock_growths;

CREATE TABLE stock_growths AS
SELECT ticker, industry, sector, year, 100*(last_close-first_close)/first_close as stock_growth
FROM fl_prices;

DROP TABLE IF EXISTS stock_volumes;

CREATE TABLE stock_volumes AS
SELECT p.ticker, i.industry, i.sector, p.year, SUM(p.volume) as total_volume
FROM prices p
JOIN infos i ON p.ticker = i.ticker
WHERE i.industry <> 'N/A' AND i.sector <> 'N/A'
GROUP BY p.ticker, i.industry, i.sector, p.year;



