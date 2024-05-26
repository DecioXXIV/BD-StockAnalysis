#!/home/xodiec/.pyenv/shims/python

import sys

for line in sys.stdin:
    if line.startswith('ticker'):       # Skippiamo le Righe di Intestazione delle due Tabelle
        continue

    line = line.strip()
    parts = line.split(';')

    if len(parts) == 5:                 # Riga proveniente da "historical_stocks.csv"
        ticker, exchange, name, sector, industry = parts

        if industry == '' and sector == '':
            print(f"{ticker}\t" + "Industry:None--Sector:None")
        elif industry == '' and sector != '':
            print(f"{ticker}\t" + f"Industry:None--{sector}")
        elif industry != '' and sector == '':
            print(f"{ticker}\t" + f"{industry}--Sector:None")
        else:
            print(f"{ticker}\t{industry}--{sector}")

    elif len(parts) == 8:               # Riga proveniente da "historical_stock_prices.csv"
        ticker, open_price, close_price, low, high, volume, date, year = parts
        print(f"{ticker}-{year}\t{date},{close_price},{volume}")

    else:
        sys.stderr.write(f"Formato della riga non valido: {line}\n")