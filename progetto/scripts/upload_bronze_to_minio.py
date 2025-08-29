import os
from deltalake import DeltaTable, write_deltalake
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

# ================== CONFIGURAZIONE ==================
storage_options = { 
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),                
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"), 
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"), 
    "AWS_ALLOW_HTTP": "true" 
}

bronze_tables = ["qualita_aria_bronze", "anagrafica_stazioni_bronze", "anagrafica_parametri_bronze"] 

# --- CORREZIONE APPLICATA QUI ---
# Ora lo script cercherà la cartella 'delta_tables' all'interno della
# cartella da cui lo esegui (ovvero 'progetto').
local_base_path = "delta_tables" 
# ====================================================

print("Inizio l'upload delle tabelle Bronze su MinIO S3...")

for table_name in bronze_tables:
    local_table_path = os.path.join(local_base_path, table_name)
    s3_table_uri = f"s3a://external/{table_name}"
    try:
        print(f"\n--- Processando la tabella: {table_name} ---")
        
        print(f"Lettura da percorso locale: {local_table_path}")
        dt_local = DeltaTable(local_table_path)
        df = dt_local.to_pandas()
        print(f"Lette {len(df)} righe.")

        print(f"Scrittura su destinazione S3: {s3_table_uri}")
        write_deltalake(
            s3_table_uri,
            df,
            mode="overwrite",
            storage_options=storage_options,
            description=f"Tabella Bronze '{table_name}' caricata da VM."
        )
        print("Scrittura completata con successo!")

    except Exception as e:
        print(f"ERRORE durante il processamento di {table_name}: {e}")

print("\nUpload completato.")