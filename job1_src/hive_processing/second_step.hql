CREATE TABLE first_closing_prices AS
SELECT 
    p.ticker, 
    p.year, 
    p.close as first_close
FROM 
    stock_prices p 
JOIN 
    (SELECT 
        ticker, 
        year, 
        MIN(`date`) as first_date
    FROM 
        stock_prices
    GROUP BY 
        ticker, 
        year) subquery
ON 
    p.ticker = subquery.ticker 
    AND p.year = subquery.year 
    AND p.`date` = subquery.first_date;

CREATE TABLE last_closing_prices AS
SELECT 
    p.ticker, 
    p.year, 
    p.close as last_close
FROM 
    stock_prices p 
JOIN 
    (SELECT 
        ticker, 
        year, 
        MAX(`date`) as last_date
    FROM 
        stock_prices
    GROUP BY 
        ticker, 
        year) subquery
ON 
    p.ticker = subquery.ticker 
    AND p.year = subquery.year 
    AND p.`date` = subquery.last_date;

DROP TABLE IF EXISTS second_step;

CREATE TABLE second_step AS
SELECT 
    fcp.ticker, 
    fcp.year, 
    (100 * (lcp.last_close - fcp.first_close) / fcp.first_close) as perc_dif
FROM 
    first_closing_prices fcp 
JOIN 
    last_closing_prices lcp 
ON 
    fcp.ticker = lcp.ticker 
    AND fcp.year = lcp.year;

DROP TABLE first_closing_prices;
DROP TABLE last_closing_prices;
