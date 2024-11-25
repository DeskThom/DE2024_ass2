import os
import pandas as pd

def list_csv_files(data_path):
    """Lijst alle CSV-bestanden in de opgegeven map."""
    csv_files = []
    for root, dirs, files in os.walk(data_path):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    return csv_files

def load_csv_files(csv_files):
    """Laad CSV-bestanden in afzonderlijke DataFrames."""
    dataframes = {}
    for file_path in csv_files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        dataframes[file_name] = pd.read_csv(file_path)
    return dataframes
