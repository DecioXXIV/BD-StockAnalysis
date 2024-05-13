import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

CWD = os.getcwd()
STOCK_PRICES = pd.read_csv(CWD + "/datasets/historical_stock_prices.csv", header=0)

def insert_year_value(idx):
    date = STOCK_PRICES.at[idx, 'date']
    year = date[0:4]
    STOCK_PRICES.at[idx, 'year'] = year

### #### ###
### MAIN ###
### #### ###
if __name__ == '__main__':

    print("DATASET LOADED!\n")

    print("STEP 1 - Removing the Column 'adj_close'...\n")
    STOCK_PRICES.drop(columns=['adj_close'], inplace=True)

    print("BEGINNING STEP 2 - Inserting the Column 'year'...")
    start = datetime.now()
    with tqdm(total=len(STOCK_PRICES)) as pbar:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            for idx in STOCK_PRICES.index:
                executor.submit(insert_year_value, idx)
                pbar.update(1)
    
    print("STEP 2 COMPLETED - Time Elapsen: " + str(datetime.now() - start) + "\n")

    print("Saving Post-Processed Data")
    STOCK_PRICES.to_csv(CWD + "/datasets/historical_stock_prices_post_processed.csv", header=True, index=False)