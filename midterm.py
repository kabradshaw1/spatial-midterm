import pandas as pd
import geopandas as gpd
import os

# Remove non active voters from the dataset and save the filtered dataset to a new CSV file
ncvoter7 = './Data/ncvoter7.txt'

df = pd.read_csv(ncvoter7, delimiter='\t')

active_voters_df = df[df['voter_status_desc'] == 'ACTIVE']

filtered_file_path = './active_voters.csv'
active_voters_df.to_csv(filtered_file_path, index=False)

"""
Read the addresses.shp file into a GeoDataFrame and convert it into WGS 84, so we can use Lat and Lon.  
Save the GeoDataFrame into a GeoPack file named adddress_4326.gpkg
"""
shapefile_path = './Data/Beaufort_Count_Streets/addresses.shp'

shapefile_path = os.path.expanduser(shapefile_path)

gdf = gpd.read_file(shapefile_path)

gdf = gdf.set_crs('EPSG:4269')

gdf_wgs84 = gdf.to_crs('EPSG:4326')

geopackage_path = './addresses_4326.gpkg'
gdf_wgs84.to_file(geopackage_path, driver='GPKG')

"""
Match the address "res_street_address" from the active_voters.csv file with the FullAddres in the address_4326.gpkg file.
The addresses do not have the same format, so we need to clean the addresses before we can match them.
"""

# Create a GeoDataFrame with a the street address and geometry for the matched address and name it matched_address.gpkg
