DROP TABLE IF EXISTS first_step;

CREATE TABLE first_step AS
SELECT 
    p.ticker, 
    i.name, 
    p.year, 
    MIN(p.low) as min_price, 
    MAX(p.high) as max_price, 
    AVG(p.volume) as avg_volume
FROM 
    stock_prices p 
JOIN 
    stock_infos i 
ON 
    p.ticker = i.ticker
GROUP BY 
    p.ticker, 
    i.name, 
    p.year;
