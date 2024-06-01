DROP TABLE IF EXISTS infos;

CREATE TABLE infos (
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

LOAD DATA INPATH '/user/historical_stocks.csv' INTO TABLE infos;

DROP TABLE IF EXISTS prices;

CREATE TABLE prices (
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

LOAD DATA INPATH '/user/historical_stock_prices.csv' INTO TABLE prices;