import pandas as pd
import geopandas as gpd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# Get the current script's directory
script_dir = Path(__file__).resolve().parent

# Resolve the data path relative to the script directory
data_path = script_dir.parent / 'Data'

ncvoter7 = data_path / "ncvoter7.txt"

# Read the file with a fallback encoding
try:
    df = pd.read_csv(ncvoter7, delimiter='\t', encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(ncvoter7, delimiter='\t', encoding='latin1')

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_file_path = script_dir / 'active_voters.csv'
active_voters_df.to_csv(filtered_file_path, index=False)

"""
Read the addresses.shp file into a GeoDataFrame and convert it into WGS 84, so we can use Lat and Lon.  
Save the GeoDataFrame into a GeoPack file named adddress_4326.gpkg
"""
shapefile_path = data_path / "Beaufort_Count_Streets/addresses.shp"

address_gdf = gpd.read_file(shapefile_path)


# This step is not necessary, but the the csv could be a good reference
address_df = address_gdf.drop(columns=["geometry"])
address_df.to_csv("beaufort_address.csv")

address_gdf.to_csv("beaufort_address_gdf.csv")

# Transform the CRS to WGS 84
address_4326 = address_gdf.to_crs(epsg=4326)

geopackage_path = script_dir / 'addresses_4326.gpkg'

# Remove existing file if it exists to prevent conflicts
if geopackage_path.exists():
    geopackage_path.unlink()

# Save the GeoDataFrame to a GeoPackage
address_4326.to_csv("address_4326.csv")
address_4326.to_file(geopackage_path, driver='GPKG')

"""
Match the address "res_street_address" from the active_voters.csv file with the FullAddres in the address_4326.gpkg file.
The addresses do not have the same format, so we need to clean the addresses before we can match them.
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

# Load the cleaned data
active_voters_df = pd.read_csv(filtered_file_path)
gdf_wgs84 = gpd.read_file(geopackage_path)

# Clean addresses in parallel
def parallel_clean_addresses(addresses):
    with ProcessPoolExecutor() as executor:
        cleaned_addresses = list(executor.map(clean_address, addresses))
    return cleaned_addresses

# Apply parallel cleaning
active_voters_df['cleaned_res_street_address'] = parallel_clean_addresses(active_voters_df['res_street_address'])
gdf_wgs84['cleaned_FullAddres'] = parallel_clean_addresses(gdf_wgs84['FullAddres'])

# Perform the matching of addresses
matched_df = pd.merge(active_voters_df, gdf_wgs84, left_on='cleaned_res_street_address', right_on='cleaned_FullAddres', how='inner')

""" 
Create a GeoDataFrame with the street address and geometry for the matched address and save it
"""
matched_gdf = gpd.GeoDataFrame(matched_df, geometry='geometry')
matched_geopackage_path = script_dir / 'matched_addresses.gpkg'

# Remove existing file if it exists to prevent conflicts
if matched_geopackage_path.exists():
    matched_geopackage_path.unlink()

matched_gdf.to_file(matched_geopackage_path, driver='GPKG')

print(f"Matched addresses have been successfully saved to {matched_geopackage_path}")
