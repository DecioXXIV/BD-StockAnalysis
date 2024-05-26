#!/home/xodiec/.pyenv/shims/python

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import asc
from datetime import datetime

### #################### ###
### FUNZIONI DI SUPPORTO ###
### #################### ###
def process_infos_line(line):
    line = line.split(';')
    if len(line) != 5:
        print("ERRORE: ", line)
    ticker, exchange, name, sector, industry = line
    return (ticker, (industry, sector))

def process_prices_line(line):
    line = line.split(';')
    ticker, open_price, close_price, low, high, volume, date, year = line
    return (ticker, (float(close_price), date, int(year), int(volume)))

def process_joined_line(line):
    ticker, values = line
    infos, stats = values

    industry, sector = infos
    close_price, date, year, volume = stats

    return ((industry, sector, year), (ticker, close_price, date, volume))

def compute_industry_yearly_metrics(records):
    stats = dict()

    for record in records:
        ticker, close_price, date, volume = record
        if ticker not in stats:
            inner_dict = dict()
            inner_dict['first_date'] = date
            inner_dict['first_price'] = close_price
            inner_dict['last_date'] = date
            inner_dict['last_price'] = close_price
            inner_dict['volume'] = volume
        
        else:
            inner_dict = stats[ticker]

            if date < inner_dict['first_date']:
                inner_dict['first_date'] = date
                inner_dict['first_price'] = close_price
            
            if date > inner_dict['last_date']:
                inner_dict['last_date'] = date
                inner_dict['last_price'] = close_price
            
            inner_dict['volume'] += volume
        
        stats[ticker] = inner_dict
    
    industry_first_price, industry_last_price = 0, 0
    stock_max_growth = None
    max_growth = float('-inf')
    stock_max_volume = None
    max_volume = 0

    for ticker in stats:
        inner_dict = stats[ticker]

        industry_first_price += inner_dict['first_price']
        industry_last_price += inner_dict['last_price']

        growth = 100 * (inner_dict['last_price'] - inner_dict['first_price']) / inner_dict['first_price']
        if growth > max_growth:
            stock_max_growth = ticker
            max_growth = growth
        
        if inner_dict['volume'] > max_volume:
            stock_max_volume = ticker
            max_volume = inner_dict['volume']
    
    industry_growth = 100 * (industry_last_price - industry_first_price) / industry_first_price

    return (industry_growth, stock_max_growth, max_growth, stock_max_volume, max_volume)

### #### ###
### MAIN ###
### #### ###

### Spark Application Setup
parser = argparse.ArgumentParser()
parser.add_argument("--stock_infos", type=str, help="Stock Infos File")
parser.add_argument("--stock_prices", type=str, help="Stock Prices File")

args = parser.parse_args()
stock_infos, stock_prices = args.stock_infos, args.stock_prices

spark = SparkSession.builder.appName("Stocks").getOrCreate()
sc = spark.sparkContext
start = datetime.now()

''' STEP 1: Caricamento dei Dati e pre-processing -> gli RDD contengono le informazioni necessarie per l'elaborazione '''
infos_RDD = sc.textFile(stock_infos)
infos_header = infos_RDD.first()
infos_RDD = infos_RDD.filter(lambda line: line != infos_header).map(lambda line: process_infos_line(line))
infos_RDD = infos_RDD.filter(lambda line: line[1][0] != '' and line[1][1] != '')
# Ora infos_RDD contiene righe con la seguente struttura: [ticker, (industry, sector)] -> 'ticker' è la chiave

prices_RDD = spark.sparkContext.textFile(stock_prices)
prices_header = prices_RDD.first()
prices_RDD = prices_RDD.filter(lambda line: line != prices_header).map(lambda line: process_prices_line(line))
# prices_RDD contiene righe con la seguente struttura: [ticker, (close_price, date, year, volume)] -> 'ticker' è la chiave

''' STEP 2: Join dei due RDD -> il RDD ottenuto ora contiene tutte le informazioni necessarie per l'elaborazione '''
joined_RDD = infos_RDD.join(prices_RDD)
# joined_RDD contiene righe con la seguente struttura: [ticker, ((industry, sector) ,(close_price, low, high, volume, date, year))] -> 'ticker' è la chiave

processed_RDD = joined_RDD.map(lambda line: process_joined_line(line))
# processed_RDD contiene righe con la seguente struttura: [(industry, sector, year), (ticker, close_price, date, volume)] -> ('industry', 'sector', 'year') è la chiave

''' STEP 3: Elaborazione dei dati -> si effettuano le aggregazioni richieste (con 'compute_industry_yearly_metrics') e si produce il report finale '''

final_RDD = processed_RDD.groupByKey().mapValues(compute_industry_yearly_metrics)
final_RDD = final_RDD.map(lambda line: (line[0][0], line[0][1], line[0][2], line[1][0], line[1][1], line[1][2], line[1][3], line[1][4]))
# final_RDD contiene righe con la seguente struttura: [industry, sector, year, industry_growth_%, best_grown_stock, stock_growth_%, best_volume_stock, volume]
print("Fine dell'elaborazione. Tempo impiegato:", datetime.now()-start)

# VISUALIZZAZIONE
final_DF = final_RDD.toDF(["industry", "sector", "year", "industry_growth_%", "best_grown_stock", "stock_growth_%", "best_volume_stock", "volume"])
final_DF.sort(asc("industry"), asc("sector"), asc("year")).show()