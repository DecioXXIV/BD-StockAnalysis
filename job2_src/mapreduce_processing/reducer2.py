#!/home/xodiec/.pyenv/shims/python

import sys

stats_2_key = dict()

for line in sys.stdin:
    key, value = line.strip().split('\t')

    ticker, first_close_price, last_close_price, volume = value.split('-')
    first_close_price = float(first_close_price)
    last_close_price = float(last_close_price)
    volume = int(volume)

    # Per ogni Chiave "industry--sector--year" si crea un Dizionario che memorizza i seguenti valori:
        # industry_fcp: somma dei primi prezzi di chiusura delle azioni associate alla tripla (industry, sector, year)
        # industry_lcp: somma degli ultimi prezzi di chiusura delle azioni associate alla tripla (industry, sector, year)
        # best_grown_stock: azione associata alla tripla (industry, sector,year) che ha avuto la massima crescita percentuale
        # growth_%: percentuale di crescita della 'best_grown_stock'
        # best_volume_stock: azione associata alla tripla (industry, sector,year) che ha avuto il volume maggiore
        # volume: volume della 'best_volume_stock'

    if key not in stats_2_key:
        # Se la Chiave (industry--sector--year) non è presente nel Dizionario Esterno, si crea un nuovo Dizionario Interno
        inner_dict = dict()

        inner_dict['industry_fcp'] = first_close_price
        inner_dict['industry_lcp'] = last_close_price

        inner_dict['best_grown_stock'] = ticker
        inner_dict['growth_%'] = 100*(last_close_price - first_close_price) / first_close_price

        inner_dict['best_volume_stock'] = ticker
        inner_dict['volume'] = volume
    
    else:
        # Se la Chiave (industry--sector--year) è presente nel Dizionario Esterno, si aggiornano i valori del Dizionario Interno
        inner_dict = stats_2_key[key]

        inner_dict['industry_fcp'] += first_close_price
        inner_dict['industry_lcp'] += last_close_price

        current_growth = 100*(last_close_price - first_close_price) / first_close_price
        if current_growth > inner_dict['growth_%']:
            inner_dict['best_grown_stock'] = ticker
            inner_dict['growth_%'] = current_growth
        
        current_volume = volume
        if current_volume > inner_dict['volume']:
            inner_dict['best_volume_stock'] = ticker
            inner_dict['volume'] = current_volume
    
    stats_2_key[key] = inner_dict

# Creazione del Report Finale
print("industry;sector;year;industry_growth_%;best_grown_stock;stock_growth_%;best_volume_stock;volume")    # Intestazione del Report
for key in stats_2_key:
    inner_dict = stats_2_key[key]

    industry, sector, year = key.split('--')
    industry_growth = 100*(inner_dict['industry_lcp'] - inner_dict['industry_fcp']) / inner_dict['industry_fcp']
    best_grown_stock = inner_dict['best_grown_stock']
    stock_growth = inner_dict['growth_%']
    best_volume_stock = inner_dict['best_volume_stock']
    volume = inner_dict['volume']

    print(f"{industry};{sector};{year};{industry_growth};{best_grown_stock};{stock_growth};{best_volume_stock};{volume}")
