#!/home/xodiec/.pyenv/shims/python

import sys

for line in sys.stdin:
    if line.startswith('ticker'):       # Skippiamo le Righe di Intestazione delle due Tabelle
        continue

    line = line.strip()
    parts = line.split(';')

    if len(parts) == 5:                 # Riga proveniente da "historical_stocks.csv"
        ticker, exchange, name, sector, industry = parts
        print(f"{ticker}\t{name}")

    elif len(parts) == 8:               # Riga proveniente da "historical_stock_prices.csv"
        ticker, open_price, close_price, low, high, volume, date, year = parts
        print(f"{ticker}-{year}\t{date},{close_price},{low},{high},{volume}")

    else:
        sys.stderr.write(f"Formato della riga non valido: {line}\n")