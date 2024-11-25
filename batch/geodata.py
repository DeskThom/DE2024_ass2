def enrich_with_geodata(aggregated_df, geojson_path):
    import geopandas as gpd
    geojson = gpd.read_file(geojson_path)

    # Normaliseer eerst de datatypes en formaten
    aggregated_df['postcode_4digit'] = aggregated_df['postcode_4digit'].astype(int)
    geojson['pc4_code'] = geojson['pc4_code'].astype(int)

    # Skip missing postcodes
    missing_in_geojson = set(aggregated_df['postcode_4digit']) - set(geojson['pc4_code'])
    missing_in_aggregated = set(geojson['pc4_code']) - set(aggregated_df['postcode_4digit'])


    # Merge met GeoJSON
    merged = aggregated_df.merge(geojson, left_on='postcode_4digit', right_on='pc4_code', how='left')
    merged = gpd.GeoDataFrame(merged, geometry='geometry')

    print(f"Aantal rijen in merged: {len(merged)}")
    print(f"Aantal rijen met geometrieën: {merged['geometry'].notnull().sum()}")
    print(f"Voorbeeld van een paar rijen:\n{merged.head()}")

    
    return merged
