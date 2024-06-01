# Job 2, Hive Processing

Per eseguire in locale lo script con Hive, eseguire i seguenti comandi su Terminale:

1. Creazione Tabelle di Input -> hive -f create_input_tables.hql
2. Creazione Viste -> hive -f create_views.hql
3. STEP 1 -> hive -f step_1.hql
4. STEP 2 -> hive -f step_2.hql
5. STEP 3 -> hive -f step_3.hql
6. FINAL STEP -> hive -f step_4_final.hql
7. Visualizzazione Risultati -> hive -e "SELECT * FROM results LIMIT 20;"