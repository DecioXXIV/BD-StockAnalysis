DROP TABLE IF EXISTS third_step;

CREATE TABLE third_step AS
SELECT v.industry, v.sector, v.year, v.ticker as best_volume_stock, v.total_volume as volume
FROM stock_volumes v JOIN
    (SELECT industry, sector, year, MAX(total_volume) as max_total_volume
    FROM stock_volumes
    GROUP BY industry, sector, year) subquery
ON v.industry = subquery.industry
AND v.sector = subquery.sector
AND v.year = subquery.year
AND v.total_volume = subquery.max_total_volume
ORDER BY v.industry, v.sector, v.year;