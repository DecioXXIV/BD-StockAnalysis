#!/home/xodiec/.pyenv/shims/python

import sys

for line in sys.stdin:
    if line.startswith('industry'):       # Skippiamo le Righe di Intestazione delle due Tabelle
        continue
    if line.startswith('Industry:None') or 'Sector:None' in line:       # Filtraggio: Skippiamo le Righe per cui mancano i valori di Industry o Sector
        continue

    line = line.strip()
    parts = line.split(';')

    industry, sector, year, ticker, first_close_price, last_close_price, volume = parts

    year = int(year)
    first_close_price = float(first_close_price)
    last_close_price = float(last_close_price)
    volume = int(volume)

    print(f"{industry}--{sector}--{year}\t{ticker}-{first_close_price}-{last_close_price}-{volume}")