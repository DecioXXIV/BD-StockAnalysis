DROP TABLE IF EXISTS results;

CREATE TABLE results AS
SELECT s1.industry, s1.sector, s1.year, s1.industry_growth, s2.best_grown_stock, s2.stock_growth, s3.best_volume_stock, s3.volume
FROM first_step s1
JOIN second_step s2 ON s1.industry = s2.industry AND s1.sector = s2.sector AND s1.year = s2.year
JOIN third_step s3 ON s1.industry = s3.industry AND s1.sector = s3.sector AND s1.year = s3.year
ORDER BY s1.industry, s1.sector, s1.year;