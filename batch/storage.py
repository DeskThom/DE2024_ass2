from google.cloud import storage

def save_to_gcs(bucket_name, dataframes, prefix="processed"):
    """Sla de verwerkte subsets op in Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for year, df in dataframes.items():
        file_path = f"{prefix}/{year}.csv"
        blob = bucket.blob(file_path)
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
        print(f"Bestand opgeslagen in GCS: {file_path}")
