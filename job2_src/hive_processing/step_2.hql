DROP TABLE IF EXISTS second_step;

CREATE TABLE second_step AS
SELECT g.industry, g.sector, g.year, g.ticker as best_grown_stock, g.stock_growth as stock_growth
FROM stock_growths g JOIN
    (SELECT industry, sector, year, MAX(stock_growth) as max_growth
    FROM stock_growths
    GROUP BY industry, sector, year) subquery
ON g.industry = subquery.industry
AND g.sector = subquery.sector
AND g.year = subquery.year
AND stock_growth = max_growth
ORDER BY g.industry, g.sector, g.year;