#!/home/xodiec/.pyenv/shims/python

import argparse
from pyspark.sql import SparkSession
from datetime import datetime

### #################### ###
### FUNZIONI DI SUPPORTO ###
### #################### ###
def process_infos_line(line):
    line = line.split(';')
    if len(line) != 5:
        print("ERRORE: ", line)
    ticker, exchange, name, sector, industry = line
    return (ticker, name)

def process_prices_line(line):
    line = line.split(';')
    ticker, open_price, close_price, low, high, volume, date, year = line
    return (ticker, (float(close_price), float(low), float(high), int(volume), date, int(year)))

def process_joined_line(line):
    ticker, values = line
    name, stats = values
    close_price, low, high, volume, date, year = stats
    return ((ticker, year), (name, close_price, low, high, volume, date))

def aggregate_yearly_data(data):
    name = data[0][0]
    closes = [x[1] for x in data]
    lows = [x[2] for x in data]
    highs = [x[3] for x in data]
    volumes = [x[4] for x in data]
    variation = 100*(closes[-1]-closes[0])/closes[0]

    return (name, variation, min(lows), max(highs), sum(volumes)/len(volumes))

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
infos_RDD = infos_RDD.filter(lambda line: line != infos_header).map(lambda line: process_infos_line(line)).cache()
# Ora infos_RDD contiene righe con la seguente struttura: [ticker, name] -> 'ticker' è la chiave

prices_RDD = spark.sparkContext.textFile(stock_prices)
prices_header = prices_RDD.first()
prices_RDD = prices_RDD.filter(lambda line: line != prices_header).map(lambda line: process_prices_line(line)).cache()
# prices_RDD contiene righe con la seguente struttura: [ticker, (close_price, low, high, volume, date, year)] -> 'ticker' è la chiave

''' STEP 2: Join dei due RDD -> il RDD ottenuto ora contiene tutte le informazioni necessarie per l'elaborazione '''
joined_RDD = infos_RDD.join(prices_RDD)
# joined_RDD contiene righe con la seguente struttura: [ticker, (name, (close_price, low, high, volume, date, year))] -> 'ticker' è la chiave

processed_RDD = joined_RDD.map(lambda line: process_joined_line(line))
# processed_RDD contiene righe con la seguente struttura: [(ticker, year), (name, close_price, low, high, volume, date)] -> ('ticker', 'year') è la chiave

''' STEP 3: Elaborazione dei dati -> si effettuano le aggregazioni richieste e si produce il report finale '''
final_RDD = processed_RDD.groupByKey().mapValues(lambda values: aggregate_yearly_data(list(values)))
# final_RDD contiene righe con la seguente struttura: [(ticker, year), (name, variation, min(low), max(high), avg(volume))] -> ('ticker', 'year') è la chiave

final_RDD = final_RDD.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4])).sortBy(lambda x: (x[0], x[1]))
# Lo scopo di quest'ultima 'map' è ottenere righe con la seguente struttura: [ticker, year, name, variation, min(low), max(high), avg(volume)]

for i in final_RDD.take(20):
    print(i)

print("Fine dell'elaborazione. Tempo impiegato:", datetime.now()-start)