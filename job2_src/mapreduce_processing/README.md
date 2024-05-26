# Job 2, MapReduce Processing

Per eseguire in locale lo script con MapReduce, eseguire i seguenti comandi su Terminale:

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
-mapper 'python /local/path/to/mapper1.py' \
-reducer 'python /local/path/to/reducer1.py' \
-output /hdfs/path/to/partial_output_folder \
-input /hdfs/path/to/historical_stocks.csv \
-input /hdfs/path/to/historical_stock_prices.csv

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
-mapper 'python /local/path/to/mapper2.py' \
-reducer 'python /local/path/to/reducer2.py' \
-output /hdfs/path/to/final_output_folder \
-input /hdfs/path/to/partial_output_file

N.B: si consiglia di recuperare da HDFS il file generato dalla prima passata di MapReduce, trasformarlo in formato CSV (con ';' come separatore).
Il file CSV va poi ricaricato su HDFS per poter essere fornito in input alla seconda passata di MapReduce, che genera il report finale.