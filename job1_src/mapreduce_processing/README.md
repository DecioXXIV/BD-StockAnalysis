# Job 1, MapReduce Processing

Per eseguire in locale lo script con MapReduce, eseguire il seguente comando su Terminale:

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
-mapper 'python /local/path/to/mapper.py' \
-reducer 'python /local/path/to/reducer.py' \
-output /hdfs/path/to/output_folder \
-input /hdfs/path/to/historical_stocks.csv \
-input /hdfs/path/to/historical_stock_prices.csv