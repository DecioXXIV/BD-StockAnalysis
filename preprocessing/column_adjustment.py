import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

CWD = os.getcwd()
STOCK_PRICES = pd.read_csv(CWD + "/datasets/historical_stock_prices.csv", header=0)
STOCK_INFOS = pd.read_csv(CWD + "/datasets/historical_stocks.csv", header=0)

### #### ###
### MAIN ###
### #### ###
if __name__ == '__main__':

    print("DATASET LOADED!\n")

    print("STEP 1 - Removing the Column 'adj_close'...")
    STOCK_PRICES.drop(columns=['adj_close'], inplace=True)
    print("Column 'adj_close' removed!\n")

    print("STEP 2 - Inserting the Column 'year'...")
    STOCK_PRICES['year'] = STOCK_PRICES['date'].apply(lambda x: x[0:4])     # Date = yyyy-mm-dd
    print("Column 'year' inserted!\n")
    
    print("Saving Post-Processed Data")
    STOCK_PRICES.to_csv(CWD + "/datasets/historical_stock_prices.csv", sep=';', header=True, index=False)
    STOCK_INFOS.to_csv(CWD + "/datasets/historical_stocks.csv", sep=';', header=True, index=False)