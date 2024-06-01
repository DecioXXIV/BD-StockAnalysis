#!/home/xodiec/.pyenv/shims/python

import argparse
from pyspark.sql import SparkSession
from datetime import datetime

### SPARK APPLICATION SETUP ###
parser = argparse.ArgumentParser()
parser.add_argument("--stock_infos", type=str, help="Stock Infos File")
parser.add_argument("--stock_prices", type=str, help="Stock Prices File")

args = parser.parse_args()
stock_infos = args.stock_infos
stock_prices = args.stock_prices

spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()

# Input CSV Datasets
stockinfo_DF = spark.read.options(delimiter=';', header=True).csv(stock_infos).cache()         # Schema -> ticker, exchange, name, sector, industry
stockprices_DF = spark.read.options(delimiter=';', header=True).csv(stock_prices).cache()      # Schema -> ticker, open, close, adj_close, low, high, volume, date, year

stockinfo_DF.createOrReplaceTempView("infos")
stockprices_DF.createOrReplaceTempView("prices")

print("INIZIO DELL'ELABORAZIONE...")
start = datetime.now()

### ########################### ###
### CREAZIONE VISTE DI SUPPORTO ###
### ########################### ###
fl_dates = spark.sql(
    """
    SELECT p.ticker, i.industry, i.sector, p.year, MIN(p.date) as first_date, MAX(p.date) as last_date
    FROM prices p 
    JOIN infos i ON p.ticker = i.ticker
    WHERE i.industry <> 'N/A' AND i.sector <> 'N/A'
    GROUP BY p.ticker, i.industry, i.sector, p.year;
    """
)
fl_dates.createOrReplaceTempView("fl_dates")

fl_prices = spark.sql(
    """
    SELECT d.ticker, d.industry, d.sector, d.year, p1.close as first_close, p2.close as last_close
    FROM fl_dates d
    JOIN prices p1 ON d.ticker = p1.ticker AND d.first_date = p1.date
    JOIN prices p2 ON d.ticker = p2.ticker AND d.last_date = p2.date;
    """
)
fl_prices.createOrReplaceTempView("fl_prices")

stock_growths = spark.sql(
    """
    SELECT ticker, industry, sector, year, 100*(last_close-first_close)/first_close as stock_growth
    FROM fl_prices;
    """
)
stock_growths.createOrReplaceTempView("stock_growths")

stock_volumes = spark.sql(
    """
    SELECT p.ticker, i.industry, i.sector, p.year, SUM(p.volume) as total_volume
    FROM prices p
    JOIN infos i ON p.ticker = i.ticker
    WHERE i.industry <> 'N/A' AND i.sector <> 'N/A'
    GROUP BY p.ticker, i.industry, i.sector, p.year;
    """
)
stock_volumes.createOrReplaceTempView("stock_volumes")


### ########## ###
### FIRST STEP ###
### ########## ###
print("FIRST STEP")
# Per ogni tripla (industry, sector, year) si calcola la crescita annuale dell'Industria (in quel settore) in un certo anno

first_DF = spark.sql(
    """
    SELECT industry, sector, year, 100*(SUM(last_close)-SUM(first_close))/SUM(first_close) as industry_growth
    FROM fl_prices
    GROUP BY industry, sector, year
    ORDER BY industry, sector, year;
    """
)
first_DF.createOrReplaceTempView("step1")

print("STEP 1: Esecuzione Completata!\n")

### ########### ###
### SECOND STEP ###
### ########### ###
print("SECOND STEP")
# Per ogni tripla (industry, sector, year) si calcola l'azione che è cresciuta di più ogni anno per ogni Industria (in quel settore)

second_DF = spark.sql(
    """
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
    """
)
second_DF.createOrReplaceTempView("step2")

print("STEP 2: Elaborazione Completata!\n")

### ########## ###
### THIRD STEP ###
### ########## ###
print("THIRD STEP")
# Per ogni tripla (industry, sector, year) si calcola il volume annuale totale di un Industria (in quel settore)

third_DF = spark.sql(
    """
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
    """
)
third_DF.createOrReplaceTempView("step3")

print("STEP 3: Elaborazione Completata!\n")

### ########## ###
### FINAL STEP ###
### ########## ###
print("FINAL STEP")
# Si uniscono i risultati dei tre step precedenti per ottenere il risultato finale

results_DF = spark.sql(
    """
    SELECT s1.industry, s1.sector, s1.year, s1.industry_growth, s2.best_grown_stock, s2.stock_growth, s3.best_volume_stock, s3.volume
    FROM step1 s1
    JOIN step2 s2 ON s1.industry = s2.industry AND s1.sector = s2.sector AND s1.year = s2.year
    JOIN step3 s3 ON s1.industry = s3.industry AND s1.sector = s3.sector AND s1.year = s3.year
    ORDER BY s1.industry, s1.sector, s1.year;
    """
)

results_DF.show()

print("FINE DELL'ELABORAZIONE! Tempo impiegato:", datetime.now()-start)