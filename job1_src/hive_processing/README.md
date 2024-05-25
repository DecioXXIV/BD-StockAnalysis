# Job 1, Hive Processing

Per eseguire in locale lo script con Hive, eseguire i seguenti comandi su Terminale:

1. Creazione Tabelle di Input -> hive -f create_input_tables.hql
2. STEP 1 -> hive -f first_step.hql
3. STEP 2 -> hive -f second_step.hql
4. FINAL STEP -> hive -f final_step.hql
5. Visualizzazione Risultati -> hive -e "SELECT * FROM final_results LIMIT 20;"