import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

CWD = os.getcwd()
STOCKS = pd.read_csv(CWD + "/datasets/historical_stocks.csv", header=0)
STOCK_PRICES = pd.read_csv(CWD + "/datasets/historical_stock_prices.csv", header=0)
TICKERS = STOCKS['ticker'].unique()
INVALID_LINES = list()

def validate_line(idx):
    ticker = STOCK_PRICES.loc[idx, 'ticker']
    date = STOCK_PRICES.loc[idx, 'date']

    year, month, day = date.split('-')                      # Date Format: YYYY-MM-DD
    year, month, day = int(year), int(month), int(day)

    if ticker not in TICKERS:
        INVALID_LINES.append(idx)
        return
    
    if month > 12 or month < 1:
        INVALID_LINES.append(idx)
        return
    
    # Months with 31 Days: January, Match, May, July, August, October, December
    if (month in [1, 3, 5, 7, 8, 10, 12]) and (day < 1 or day > 31):
        INVALID_LINES.append(idx)
        return
    
    # Months with 30 Days: April, June, September, November
    if (month in [4, 6, 9, 11]) and (day < 1 or day > 30):
        INVALID_LINES.append(idx)
        return
    
    # February -> Leap Year or not-Leap Year
    if month == 2:
        if (year % 4 == 0) and (day > 29 or day < 1):
            INVALID_LINES.append(idx)
            return
        else:
            if day > 28 or day < 1:
                INVALID_LINES.append(idx)
                return
    
### #### ###
### MAIN ###
### #### ###
if __name__ == '__main__':

    print("DATASET LOADED!\n")

    print("BEGINNING - Lines Validation: Ticker and Date...")
    start = datetime.now()
    with tqdm(total=len(STOCK_PRICES)) as pbar:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            for idx in STOCK_PRICES.index:
                executor.submit(validate_line, idx)
                pbar.update(1)
    
    print("Lines Validation Time: " + str(datetime.now() - start) + "\n")

    print("Saving Filtered Data")
    stocks_reduced = STOCK_PRICES.drop(INVALID_LINES)
    stocks_reduced.to_csv(CWD + "/datasets/historical_stock_prices_reduced.csv", header=True, index=False)