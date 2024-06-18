import pandas as pd
import geopandas as gpd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# Remove non active voters from the dataset and save the filtered dataset to a new CSV file
ncvoter7 = Path('../Data/ncvoter7.txt')

df = pd.read_csv(ncvoter7, delimiter='\t')

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_file_path = './active_voters.csv'
active_voters_df.to_csv(filtered_file_path, index=False)

"""
Read the addresses.shp file into a GeoDataFrame and convert it into WGS 84, so we can use Lat and Lon.  
Save the GeoDataFrame into a GeoPack file named adddress_4326.gpkg
"""
shapefile_path = Path('../Data/Beaufort_Count_Streets/addresses.shp')

gdf = gpd.read_file(shapefile_path)

gdf = gdf.set_crs('EPSG:4269')

gdf_wgs84 = gdf.to_crs('EPSG:4326')

geopackage_path = './addresses_4326.gpkg'
gdf_wgs84.to_file(geopackage_path, driver='GPKG')

"""
Match the address "res_street_address" from the active_voters.csv file with the FullAddres in the address_4326.gpkg file.
The addresses do not have the same format, so we need to clean the addresses before we can match them.

Use parallel processing to match the addresses.  Can use Pool and Starmap or ProcessPoolExecutor and map.
"""

def clean_address(address):
    address = str(address).upper().strip()  # Convert to uppercase and trim whitespace
    address = address.replace('.', '').replace(',', '')  # Remove punctuation
    address = address.replace(' STREET', ' ST').replace(' ROAD', ' RD')  # Standardize abbreviations
    address = address.replace(' AVENUE', ' AVE').replace(' BOULEVARD', ' BLVD')
    address = address.replace(' DRIVE', ' DR').replace(' LANE', ' LN')
    address = address.replace(' NORTH', ' N').replace(' SOUTH', ' S')
    address = address.replace(' EAST', ' E').replace(' WEST', ' W')
    return address

def clean_addresses_parallel(addresses):
    with ProcessPoolExecutor() as executor:
        cleaned_addresses = list(executor.map(clean_address, addresses))
    return cleaned_addresses

# Load the cleaned data
active_voters_df = pd.read_csv(filtered_file_path)
gdf_wgs84 = gpd.read_file(geopackage_path)

# Clean the addresses in parallel
active_voters_df['cleaned_res_street_address'] = clean_addresses_parallel(active_voters_df['res_street_address'])
gdf_wgs84['cleaned_FullAddres'] = clean_addresses_parallel(gdf_wgs84['FullAddres'])

# Perform the matching of addresses
matched_df = pd.merge(active_voters_df, gdf_wgs84, left_on='cleaned_res_street_address', right_on='cleaned_FullAddres', how='inner')

""" 
Create a GeoDataFrame with the street address and geometry for the matched address and save it
"""
matched_gdf = gpd.GeoDataFrame(matched_df, geometry='geometry')
matched_geopackage_path = './matched_addresses.gpkg'
matched_gdf.to_file(matched_geopackage_path, driver='GPKG')

print(f"Matched addresses have been successfully saved to {matched_geopackage_path}")
