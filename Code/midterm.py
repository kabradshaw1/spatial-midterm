import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
from multiprocessing import Pool

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
active_voters_df.to_csv(filtered_file_path)

"""
Read the addresses.shp file into a GeoDataFrame and convert it into WGS 84, so we can use Lat and Lon.  
Save the GeoDataFrame into a GeoPack file named adddress_4326.gpkg
"""
shapefile_path = data_path / "Beaufort_Count_Streets/addresses.shp"

address_gdf = gpd.read_file(shapefile_path)


# This step is not necessary, but the the csv could be a good reference
address_df = address_gdf.drop(columns=["geometry"])
address_df.to_csv("beaufort_addresses.csv")

address_gdf.to_csv("beaufort_addresses_gdf.csv")

# Transform the CRS to WGS 84
address_4326 = address_gdf.to_crs(epsg=4326)

geopackage_path = script_dir / 'addresses_4326.gpkg'

# Remove existing file if it exists to prevent conflicts
if geopackage_path.exists():
    geopackage_path.unlink()

# Save the GeoDataFrame to a GeoPackage
address_4326.to_csv("addresses_4326.csv")
address_4326.to_file(geopackage_path, driver='GPKG')

"""
Match the address "res_street_address" from the active_voters.csv file with the FullAddres in the address_4326.gpkg file.
The addresses do not have the same format, so we need to clean the addresses before we can match them.
"""

def match_address(add_str: str, 
                  add_series: pd.Series):
    idx = None
    for i in add_series.index:
        add_gpd = add_series.loc[i]
        # make sure both addresses are in upper case
        if add_str.upper() in add_gpd.upper():
            idx = i
            break
    return idx

if __name__ == '__main__':

    active_voter = pd.read_csv("active_voters.csv")
    add_100 = active_voter["res_street_address"][0:100]

    # convert the Series into a list
    add_list0 = add_100.to_list()
    # trim the tailing space in the addresses
    add_list1 = [add.rstrip() for add in add_list0]

    # read shapefile to get address
    address_gpd = gpd.read_file("./addresses_4326.gpkg")
    full_address_series = address_gpd["FullAddres"]

    # We don't want to share the entire GeoDataFrame among multiple processes to save memory. Since we only need
    # "FullAddres" to find matched index, we only need to pass it.
    # input list
    input_list = [(add_str, full_address_series) for add_str in add_list1]

    with Pool(12) as process_pool:
        returned_index_list = process_pool.starmap(func=match_address, iterable=input_list)

    final_add_list = []
    final_pnt_list = []

    for add_idx, gpd_idx in enumerate(returned_index_list):
        if gpd_idx is not None:
            final_add_list.append(add_list1[add_idx])
            final_pnt_list.append(address_gpd.loc[gpd_idx, "geometry"])

    add_geo_pd = pd.DataFrame(data={"mail_add": final_add_list,
                                    "geometry": final_pnt_list})
    add_geo_gpd = gpd.GeoDataFrame(data=add_geo_pd, geometry="geometry")
    add_geo_gpd.plot()
    plt.show()
""" 
Create a GeoDataFrame with the street address and geometry for the matched address and save it
"""

