def process_data(dataframes):
    """Verwerk en combineer dataframes."""
    import pandas as pd
    combined_data = []
    for name, df in dataframes.items():
        leverancier = name.split('_')[0]
        jaar = int(name.split('_')[-1])
        df['leverancier'] = leverancier
        df['jaar'] = jaar
        combined_data.append(df)
    final_df = pd.concat(combined_data, ignore_index=True)

    # Aggregatie
    aggregated_df = final_df.groupby(['zipcode_from', 'zipcode_to', 'leverancier', 'jaar']).agg({
        'annual_consume': 'sum',
        'num_connections': 'sum'
    }).reset_index()
    aggregated_df['gemiddeld_verbruik'] = aggregated_df['annual_consume'] / aggregated_df['num_connections']

    # Validatie van postcodes
    aggregated_df = aggregated_df[
        aggregated_df['zipcode_from'].str.match(r'^\d{4}[A-Z]{2}$', na=False)
    ]

    # Conversie naar 4-cijferige postcodes
    aggregated_df['postcode_4digit'] = aggregated_df['zipcode_from'].str[:4].astype(int)
    aggregated_df = aggregated_df[aggregated_df['postcode_4digit'] > 0]
    return aggregated_df
