import os
import numpy as np
import pandas as pd

CWD = os.getcwd()
STOCK_PRICES = pd.read_csv(CWD + "/datasets/historical_stock_prices.csv", sep=';', header=0)

def reduce_dataframe(df, size):
    df_reduced = df.sample(frac=size, random_state=24)
    return df_reduced

def enhance_dataframe(df, size):
    df_to_enhance = df.copy()
    final_rows = int(len(STOCK_PRICES) * size)
    rows_to_generate = final_rows - df_to_enhance.shape[0]

    print(f"Generazione di Righe Sintetiche per il Dataset con Size '{size}x': devono essere generate {rows_to_generate} righe...")

    sample = df.sample(n=rows_to_generate, random_state=24)
    years = np.random.randint(low=1950, high=2024, size=rows_to_generate)
    months = np.random.randint(low=1, high=13, size=rows_to_generate)
    days = [np.random.randint(1, 29 if month == 2 else (31 if month in [4, 6, 9, 11] else 32)) for month in months]

    sample['date'] = [f"{year}-{month:02d}-{day:02d}" for year, month, day in zip(years, months, days)]
    sample['year'] = years

    df_enhanced = pd.concat([df_to_enhance, sample], axis=0, ignore_index=True)
    return df_enhanced

### #### ###
### MAIN ###
### #### ###
if __name__ == '__main__':

    print("DATASET LOADED!\n")

    # Size == 0.5
    stock_prices_reduced = reduce_dataframe(STOCK_PRICES, 0.5)
    stock_prices_reduced.to_csv(CWD + f"/datasets/historical_stock_prices_0.5.csv", sep=';', header=True, index=False)
    print("Generata la Replica con Size '0.5x'!\n")

    # Size == 0.75
    stock_prices_reduced = reduce_dataframe(STOCK_PRICES, 0.75)
    stock_prices_reduced.to_csv(CWD + f"/datasets/historical_stock_prices_0.75.csv", sep=';', header=True, index=False)
    print("Generata la Replica con Size '0.75x'!\n")

    # Size == 1.25
    stock_prices_enhanced = enhance_dataframe(STOCK_PRICES, 1.25)
    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.25.csv", sep=';', header=True, index=False)
    print("Generata la Replica con Size '1.25'!\n")

    # Size == 1.33
    stock_prices_enhanced = enhance_dataframe(stock_prices_enhanced, 1.33)
    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.33.csv", sep=';', header=True, index=False)
    print("Generata la Replica con Size '1.33'!\n")

    # Size == 1.5
    stock_prices_enhanced = enhance_dataframe(stock_prices_enhanced, 1.5)
    stock_prices_enhanced.to_csv(CWD + f"/datasets/historical_stock_prices_1.5.csv", sep=';', header=True, index=False)
    print("Generata la Replica con Size '1.5x'!\n")