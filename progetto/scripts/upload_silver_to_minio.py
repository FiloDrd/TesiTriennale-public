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

# 2. Definisci il percorso della tabella Silver LOCALE (da dove leggere)
#    Deve corrispondere esattamente al percorso usato nel notebook.
local_silver_path = "delta_tables/profili_stazioni_silver"

# 3. Definisci l'URI della tabella Silver REMOTA su S3 (dove scrivere)
s3_silver_uri = "s3a://external/profili_stazioni_silver"
# ====================================================

print("--- Inizio Script di Upload del Silver Layer su MinIO S3 ---")

try:
    # --- LETTURA DELLA TABELLA LOCALE ---
    print(f"\nLeggendo la tabella Delta dal percorso locale: {local_silver_path}")
    dt_local = DeltaTable(local_silver_path)
    df_to_upload = dt_local.to_pandas()
    print(f"Lettura completata. Trovate {len(df_to_upload)} righe da caricare.")

    # --- SCRITTURA SULLA DESTINAZIONE REMOTA (MINIO) ---
    print(f"\nScrivendo il DataFrame sulla destinazione S3: {s3_silver_uri}")
    write_deltalake(
        s3_silver_uri,
        df_to_upload,
        mode="overwrite", # Sovrascrive la tabella su MinIO se già esiste
        storage_options=storage_options,
        description="Tabella Silver con profili stazione, caricata da script separato."
    )
    print("\nCaricamento completato con successo!")

except FileNotFoundError:
    print(f"\nERRORE: Tabella locale non trovata in '{local_silver_path}'.")
    print("Assicurati di aver eseguito prima il notebook per generare la tabella.")
except Exception as e:
    print(f"\nSi è verificato un errore durante l'upload: {e}")

print("\n--- Script terminato ---")