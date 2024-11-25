from ingestion import list_csv_files, load_csv_files
from processing import process_data
from geodata import enrich_with_geodata
from storage import save_to_gcs
import logging

# Configuratie-instellingen
LOCAL_DATA_PATH = "./data"
GEOJSON_PATH = "./georef-netherlands-postcode-pc4.geojson"
GCS_BUCKET_NAME = "jouw-bucket"
PROCESSED_PREFIX = "processed"

def batch_pipeline(local_data_path, geojson_path, bucket_name, prefix="processed"):
    try:
        # Stap 1: Data ophalen
        print("Stap 1: Data ophalen...")
        csv_files = list_csv_files(local_data_path)
        dataframes = load_csv_files(csv_files)
        print(f"{len(dataframes)} bestanden geladen.")
        
        # Stap 2: Data verwerken
        print("Stap 2: Data verwerken...")
        aggregated_df = process_data(dataframes)
        print(f"Gegevens verwerkt: {len(aggregated_df)} rijen.")
        
        # Stap 3: Geodata verrijken
        print("Stap 3: Geografische data verrijken...")
        enriched_data = enrich_with_geodata(aggregated_df, geojson_path)
        print(f"Geografische data gekoppeld: {len(enriched_data)} rijen.")
        logging.basicConfig(level=logging.DEBUG)

        # Stap 4: Data opslaan naar GCS
        print("Stap 4: Data opslaan naar Google Cloud Storage...")
        subsets_per_year = {year: df for year, df in enriched_data.groupby('jaar')}
        save_to_gcs(bucket_name, subsets_per_year, prefix=prefix)
        print(f"Data succesvol opgeslagen in bucket '{bucket_name}'.")
    
    except Exception as e:
        logging.error("Er is een fout opgetreden in de pipeline.", exc_info=True)
        raise e

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename="pipeline.log",
        filemode="w"
    )
    logging.info("Pipeline gestart...")
    batch_pipeline(
        local_data_path=LOCAL_DATA_PATH,
        geojson_path=GEOJSON_PATH,
        bucket_name=GCS_BUCKET_NAME,
        prefix=PROCESSED_PREFIX
    )
    logging.info("Pipeline succesvol afgerond.")
