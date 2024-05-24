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

print("INIZIO DELL'ELABORAZIONE...")
start = datetime.now()

### ########## ###
### FIRST STEP ###
### ########## ###
print("STEP 1")
#   Per ogni azione, identificata da 'ticker' e 'name' vengono calcolati anno per anno:
    # - Il prezzo minimo dell'azione
    # - Il prezzo massimo dell'azione
    # - Il volume medio

stockinfo_DF.createOrReplaceTempView("infos")
stockprices_DF.createOrReplaceTempView("prices")

first_DF = spark.sql(
    """
    SELECT prices.ticker, name, year, MIN(low) as min_price, MAX(high) as max_price, AVG(volume) as avg_volume
    FROM prices JOIN infos 
    ON prices.ticker = infos.ticker
    GROUP BY prices.ticker, name, year
    """
)

print("STEP 1: Esecuzione Completata!\n")

### ########### ###
### SECOND STEP ###
### ########### ###
print("SECOND STEP")
# Vengono utilizzate due viste per calcolare annualmente per ogni azione (identificata da 'ticker'):
    # - Il primo prezzo di chiusura
    # - L'ultimo prezzo di chiusura

firstdate_view = spark.sql(
    """
    SELECT p.ticker, p.year, p.close as first_close
    FROM prices p JOIN
    (SELECT ticker, year, MIN(date) as first_date
    FROM prices
    GROUP BY ticker, year) subquery
    ON p.ticker = subquery.ticker and p.year = subquery.year AND p.date = subquery.first_date
    """
)

lastdate_view = spark.sql(
    """
    SELECT p.ticker, p.year, p.close as last_close
    FROM prices p JOIN
    (SELECT ticker, year, MAX(date) as last_date
    FROM prices
    GROUP BY ticker, year) subquery
    ON p.ticker = subquery.ticker and p.year = subquery.year AND p.date = subquery.last_date
    """
)

firstdate_view.createOrReplaceTempView("firstdate_view")
lastdate_view.createOrReplaceTempView("lastdate_view")

# Ottenute le due viste descritte sopra, per ogni azione si calcola la differenza percentuale tra l'ultimo ed il primo prezzo di chiusura in ogni anno

second_DF = spark.sql(
    """
    SELECT firstdate_view.ticker, firstdate_view.year, (100*(last_close-first_close)/first_close) as perc_dif
    FROM firstdate_view JOIN lastdate_view
    ON firstdate_view.ticker = lastdate_view.ticker AND firstdate_view.year = lastdate_view.year
    """
)
print("STEP 2: Elaborazione Completata!\n")

### ########## ###
### FINAL STEP ###
### ########## ###
print("FINAL STEP")
# Si hanno a disposizione due Tabelle:
    # Tabella 1: ticker, nome, anno, prezzo minimo dell'azione, prezzo massimo dell'azione, volume medio
    # Tabella 2: ticker, anno, differenza percentuale tra ultimo e primo prezzo di chiusura in un certo anno

# Le due Tabelle vengono joinate sui valori di 'ticker' ed 'anno' ottenendo il seguente schema:
    # ticker        -> simbolo dell'azione
    # name          -> nome dell'azione
    # year          -> anno a cui fanno riferimento le statistiche successive
    # perc_dif      -> differenza percentuale tra l'ultimo ed il primo prezzo di chiusura
    # min_price     -> prezzo minimo dell'azione
    # max_price     -> prezzo massimo dell'azione
    # avg_volume    -> volume medio

first_DF.createOrReplaceTempView("first")
second_DF.createOrReplaceTempView("second")

results_DF = spark.sql(
    """
    SELECT first.ticker, name, first.year, perc_dif, min_price, max_price, avg_volume
    FROM first JOIN second
    ON first.ticker = second.ticker AND first.year = second.year
    """
)

results_DF.show()

print("FINE DELL'ELABORAZIONE! Tempo impiegato:", datetime.now()-start)