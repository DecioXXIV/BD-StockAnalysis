#!/home/xodiec/.pyenv/shims/python

import sys

stats_2_key = dict()
infos_2_key = dict()

for i, line in enumerate(sys.stdin):
    line = line.strip()

    key, value = line.split('\t')

    if '-' in key:                      
        # Coppia proveniente da "historical_stock_prices.csv"
        # Struttura = "ticker"-"year" \t "date", "close_price", "volume"
        ticker, year = key.split('-')
        year = int(year)

        date, close_price, volume = value.split(',')
        close_price = float(close_price)
        volume = int(volume)

        # Per ogni Chiave "ticker"-"year" creiamo un Dizionario che memorizza i seguenti valori:
            # first_date: Data della Prima Quotazione nell'Anno "year"
            # first_close_price: Prezzo di Chiusura della Prima Quotazione
            # last_date: Data dell'Ultima Quotazione nell'Anno "year"
            # last_close_price: Prezzo di Chiusura dell'Ultima Quotazione
            # volume: Volume Totale Scambiato nell'Anno "year"

        if key not in stats_2_key:      
            # Se la Chiave non è presente nel Dizionario "esterno", la aggiungiamo ed inizializziamo il Dizionario "interno" 
            inner_dict = dict()
            inner_dict['first_date'] = date
            inner_dict['first_close_price'] = close_price
            inner_dict['last_date'] = date
            inner_dict['last_close_price'] = close_price
            inner_dict['volume'] = volume
    
        else:
            # Se la Chiave è già presente nel Dizionario "esterno", aggiorniamo il Dizionario "interno"
            inner_dict = stats_2_key[key]

            if date < inner_dict['first_date']:
                inner_dict['first_date'] = date
                inner_dict['first_close_price'] = close_price
        
            if date > inner_dict['last_date']:
                inner_dict['last_date'] = date
                inner_dict['last_close_price'] = close_price
            
            inner_dict['volume'] += volume
        
        stats_2_key[key] = inner_dict

    else:             
        # Coppia proveniente da "historical_stocks.csv"
        # Struttura della Coppia: "ticker" \t "industry", "sector"
        if key not in infos_2_key:
            inner_dict = dict()
            industry, sector = value.split('--')
            inner_dict['industry'], inner_dict['sector'] = industry, sector
            infos_2_key[key] = inner_dict
            
# Dato il ticker presente in una Chiave "ticker"-"year", si aggiorna il Dizionario "interno" aggiungendo il Nome della Società
all_keys = list(stats_2_key.keys())

for ticker in infos_2_key:
    industry = infos_2_key[ticker]['industry']
    sector = infos_2_key[ticker]['sector']
    valid_keys = [key for key in all_keys if key.startswith(ticker)]

    for key in valid_keys:
        inner_dict = stats_2_key[key]

        inner_dict['industry'] = industry
        inner_dict['sector'] = sector
        stats_2_key[key] = inner_dict

# Creazione del Report Finale
print("industry;sector;year;ticker;first_close_price;last_close_price;volume")      # Intestazione del Report
for key in stats_2_key:
    inner_dict = stats_2_key[key]

    ticker, year = key.split('-')
    year = int(year)

    industry = inner_dict['industry']
    sector = inner_dict['sector']

    first_close_price = inner_dict['first_close_price']
    last_close_price = inner_dict['last_close_price']

    volume = inner_dict['volume']

    print(f"{industry};{sector};{year};{ticker};{first_close_price};{last_close_price};{volume}")