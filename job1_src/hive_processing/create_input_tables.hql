DROP TABLE IF EXISTS stock_infos;

CREATE TABLE stock_infos (
    `ticker` STRING,
    `exchange` STRING,
    `name` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/historical_stocks.csv' INTO TABLE stock_infos;

DROP TABLE IF EXISTS stock_prices;

CREATE TABLE stock_prices (
    `ticker` STRING,
    `open` FLOAT,
    `close` FLOAT,
    `low` FLOAT,
    `high` FLOAT,
    `volume` INT,
    `date` STRING,
    `year` INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/historical_stock_prices_post_processed.csv' INTO TABLE stock_prices;
