DROP TABLE IF EXISTS final_results;
CREATE TABLE final_results AS
SELECT 
    fs.ticker, 
    fs.name, 
    fs.year, 
    ss.perc_dif, 
    fs.min_price, 
    fs.max_price, 
    fs.avg_volume
FROM 
    first_step fs 
JOIN 
    second_step ss 
ON 
    fs.ticker = ss.ticker 
    AND fs.year = ss.year;
