#!/home/xodiec/.pyenv/shims/python

import sys

stats_2_key = dict()
name_2_ticker = dict()

for i, line in enumerate(sys.stdin):
    line = line.strip()

    key, value = line.split('\t')

    if '-' in key:                      
        # Coppia proveniente da "historical_stock_prices.csv"
        # Struttura della Coppia: "ticker"-"year" \t "date", "close_price", "low", "high", "volume"
        ticker, year = key.split('-')
        year = int(year)

        date, close_price, low, high, volume = value.split(',')
        close_price = float(close_price)
        low = float(low)
        high = float(high)
        volume = int(volume)

        # Per ogni Chiave "ticker"-"year" creiamo un Dizionario che memorizza i seguenti valori:
            # first_date: Data della Prima Quotazione nell'Anno "year"
            # first_close_price: Prezzo di Chiusura della Prima Quotazione
            # last_date: Data dell'Ultima Quotazione nell'Anno "year"
            # last_close_price: Prezzo di Chiusura dell'Ultima Quotazione
            # low: Prezzo Minimo Registrato nell'Anno "year"
            # high: Prezzo Massimo Registrato nell'Anno "year"
            # volume: Volume Totale Scambiato nell'Anno "year"

        if key not in stats_2_key:      
            # Se la Chiave non è presente nel Dizionario "esterno", la aggiungiamo ed inizializziamo il Dizionario "interno" 
            inner_dict = dict()
            inner_dict['first_date'] = date
            inner_dict['first_close_price'] = close_price
            inner_dict['last_date'] = date
            inner_dict['last_close_price'] = close_price
            inner_dict['low'] = low
            inner_dict['high'] = high
            inner_dict['volume'] = volume
            inner_dict['count'] = 1
    
        else:
            # Se la Chiave è già presente nel Dizionario "esterno", aggiorniamo il Dizionario "interno"
            inner_dict = stats_2_key[key]

            if date < inner_dict['first_date']:
                inner_dict['first_date'] = date
                inner_dict['first_close_price'] = close_price
        
            if date > inner_dict['last_date']:
                inner_dict['last_date'] = date
                inner_dict['last_close_price'] = close_price

            if low < inner_dict['low']:
                inner_dict['low'] = low
        
            if high > inner_dict['high']:
                inner_dict['high'] = high

            inner_dict['volume'] += volume
            inner_dict['count'] += 1
        
        stats_2_key[key] = inner_dict

    else:                   
        # Coppia proveniente da "historical_stocks.csv"
        # Struttura della Coppia: "ticker" \t "name"
        name_2_ticker[key] = value

# Dato il ticker presente in una Chiave "ticker"-"year", si aggiorna il Dizionario "interno" aggiungendo il Nome della Società
all_keys = list(stats_2_key.keys())

for ticker in name_2_ticker:
    name = name_2_ticker[ticker]
    valid_keys = [key for key in all_keys if key.startswith(ticker)]

    for key in valid_keys:
        inner_dict = stats_2_key[key]

        inner_dict['name'] = name
        stats_2_key[key] = inner_dict

# Creazione del Report Finale
print("ticker, name, year, perc_diff, low, high, avg_volume")       # Intestazione del Report
for key in stats_2_key:
    inner_dict = stats_2_key[key]

    ticker, year = key.split('-')
    year = int(year)

    first_close_price = inner_dict['first_close_price']
    last_close_price = inner_dict['last_close_price']
    perc_diff = 100*(last_close_price - first_close_price)/first_close_price

    name = inner_dict.get('name')
    low = inner_dict.get('low')
    high = inner_dict.get('high')
    volume = inner_dict.get('volume')
    count = inner_dict.get('count')

    print(f"{ticker}, {name}, {year}, {perc_diff}, {low}, {high}, {volume/count}")