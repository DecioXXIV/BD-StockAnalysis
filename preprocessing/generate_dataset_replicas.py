import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

CWD = os.getcwd()
STOCK_INFO = pd.read_csv(CWD + "/datasets/historical_stocks.csv", header=0) 
STOCK_PRICES = pd.read_csv(CWD + "/datasets/historical_stock_prices_post_processed.csv", header=0)

TICKERS = STOCK_INFO['ticker'].unique()
YEARS = STOCK_PRICES['year'].unique()

def create_synthetic_row(ticker, stock_prices_to_enhance, years, result_queue):
    ticker_dates = stock_prices_to_enhance[stock_prices_to_enhance['ticker'] == ticker]['date'].unique()

    date = None
    while date is None or date in ticker_dates:
        year = np.random.choice(years)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29 if month == 2 else (31 if month in [4, 6, 9, 11] else 32))
        date = f"{year}-{str(month)}-{str(day)}"

    open_price = np.random.uniform(stock_prices_to_enhance['open'].min(), stock_prices_to_enhance['open'].max()+0.01)
    close = np.random.uniform(stock_prices_to_enhance['close'].min(), stock_prices_to_enhance['close'].max()+0.01)
    low = np.random.uniform(stock_prices_to_enhance['low'].min(), stock_prices_to_enhance['low'].max()+0.01)
    high = np.random.uniform(stock_prices_to_enhance['high'].min(), stock_prices_to_enhance['high'].max()+0.01)
    volume = np.random.randint(stock_prices_to_enhance['volume'].min(), stock_prices_to_enhance['volume'].max()+1)

    row = pd.Series([ticker, open_price, close, low, high, volume, date, year], index=stock_prices_to_enhance.columns)
    result_queue.put(row)

### #### ###
### MAIN ###
### #### ###
if __name__ == '__main__':

    print("DATASET LOADED!\n")

    # Size == 0.5
    stock_prices_reduced = STOCK_PRICES.sample(frac=0.5, random_state=24)
    stock_prices_reduced.to_csv(CWD + f"/datasets/historical_stock_prices_0.5.csv", header=True, index=False)
    print("Dataset Replica with Size 0.5 generated!\n")

    # Size == 0.75
    stock_prices_reduced = STOCK_PRICES.sample(frac=0.75, random_state=24)
    stock_prices_reduced.to_csv(CWD + f"/datasets/historical_stock_prices_0.75.csv", header=True, index=False)
    print("Dataset Replica with Size 0.75 generated!\n")

    # Size == 1.25
    stock_prices_to_enhance = STOCK_PRICES.copy()
    final_rows = int(len(STOCK_PRICES) * 1.25)
    rows_to_generate = final_rows - len(stock_prices_to_enhance)
    
    print(f"Generating Synthetic Rows for Dataset Replica with Size 1.25x: {rows_to_generate} rows have to be generated...")

    result_queue = Queue()
    with tqdm(total=rows_to_generate) as pbar:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for _ in range(rows_to_generate):
                picked_ticker = np.random.choice(TICKERS)
                futures.append(executor.submit(create_synthetic_row, picked_ticker, stock_prices_to_enhance, YEARS, result_queue))

            for future in as_completed(futures):
                pbar.update(1)
    
    while not result_queue.empty():
        row = result_queue.get()
        stock_prices_enhanced = pd.concat([stock_prices_to_enhance, pd.DataFrame([row])], axis=0)

    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.25.csv", header=True, index=False)
    print("Dataset Replica with Size 1.25 generated!\n")

    # Size == 1.33
    stock_prices_to_enhance = stock_prices_enhanced.copy()
    final_rows = int(len(STOCK_PRICES) * 1.33)
    rows_to_generate = final_rows - len(stock_prices_to_enhance)
    
    print(f"Generating Synthetic Rows for Dataset Replica with Size 1.33x: {rows_to_generate} rows have to be generated...")

    result_queue = Queue()
    with tqdm(total=rows_to_generate) as pbar:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for _ in range(rows_to_generate):
                picked_ticker = np.random.choice(TICKERS)
                futures.append(executor.submit(create_synthetic_row, picked_ticker, stock_prices_to_enhance, YEARS, result_queue))

            for future in as_completed(futures):
                pbar.update(1)
    
    while not result_queue.empty():
        row = result_queue.get()
        stock_prices_enhanced = pd.concat([stock_prices_to_enhance, pd.DataFrame([row])], axis=0)

    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.33.csv", header=True, index=False)
    print("Dataset Replica with Size 1.33 generated!\n")

    # Size == 1.5
    stock_prices_to_enhance = stock_prices_enhanced.copy()
    final_rows = int(len(STOCK_PRICES) * 1.5)
    rows_to_generate = final_rows - len(stock_prices_to_enhance)
    
    print(f"Generating Synthetic Rows for Dataset Replica with Size 1.5x: {rows_to_generate} rows have to be generated...")

    result_queue = Queue()
    with tqdm(total=rows_to_generate) as pbar:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for _ in range(rows_to_generate):
                picked_ticker = np.random.choice(TICKERS)
                futures.append(executor.submit(create_synthetic_row, picked_ticker, stock_prices_to_enhance, YEARS, result_queue))

            for future in as_completed(futures):
                pbar.update(1)
    
    while not result_queue.empty():
        row = result_queue.get()
        stock_prices_enhanced = pd.concat([stock_prices_to_enhance, pd.DataFrame([row])], axis=0)

    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.5.csv", header=True, index=False)
    print("Dataset Replica with Size 1.5 generated!\n")