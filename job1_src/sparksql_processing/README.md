# Job 1, Spark SQL Processing

Per eseguire in locale lo script con Spark SQL, eseguire il seguente comando su Terminale:

$SPARK_HOME/bin/spark-submit \
--master local \\
/local/path/to/sparksql.py
--stock_infos file:///local/path/to/historical_stocks.csv \
--stock_prices file:///local/path/to/historical_stock_prices.csv