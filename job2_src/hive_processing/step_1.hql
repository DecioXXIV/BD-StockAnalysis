DROP TABLE IF EXISTS first_step;

CREATE TABLE first_step AS
SELECT industry, sector, year, 100*(SUM(last_close)-SUM(first_close))/SUM(first_close) as industry_growth
FROM fl_prices
GROUP BY industry, sector, year
ORDER BY industry, sector, year;