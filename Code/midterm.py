import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
from multiprocessing import Pool

def main():
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

    # Read the addresses.shp file into a GeoDataFrame and convert it into WGS 84
    shapefile_path = data_path / "Beaufort_Count_Streets/addresses.shp"
    address_gdf = gpd.read_file(shapefile_path)

    # Save a CSV of the non-geometry data
    address_df = address_gdf.drop(columns=["geometry"])
    address_df.to_csv("beaufort_addresses.csv", index=False)

    # Transform the CRS to WGS 84
    address_4326 = address_gdf.to_crs(epsg=4326)

    geopackage_path = script_dir / 'addresses_4326.gpkg'

    # Remove existing file if it exists to prevent conflicts
    if geopackage_path.exists():
        try:
            geopackage_path.unlink()
        except PermissionError:
            print(f"PermissionError: Unable to delete {geopackage_path}. Ensure you have the correct permissions.")
            return

    # Save the GeoDataFrame to a GeoPackage
    try:
        address_4326.to_file(geopackage_path, driver='GPKG')
    except Exception as e:
        print(f"Error saving GeoPackage: {e}")
        return

def match_address(add_str: str, add_series: pd.Series):
    for i in add_series.index:
        add_gpd = add_series.loc[i]
        if add_str.upper() in add_gpd.upper():
            return i
    return None

if __name__ == '__main__':
    main()

    active_voter = pd.read_csv("active_voters.csv")
    add_100 = active_voter["res_street_address"][0:100]

    # Convert the Series into a list and trim the trailing spaces
    add_list1 = [add.rstrip() for add in add_100.to_list()]

    # Read the addresses GeoPackage to get address
    address_gpd = gpd.read_file("./addresses_4326.gpkg")
    full_address_series = address_gpd["FullAddres"]

    # Prepare input list for multiprocessing
    input_list = [(add_str, full_address_series) for add_str in add_list1]

    with Pool(12) as process_pool:
        returned_index_list = process_pool.starmap(func=match_address, iterable=input_list)

    final_add_list = []
    final_pnt_list = []

    for add_idx, gpd_idx in enumerate(returned_index_list):
        if gpd_idx is not None:
            final_add_list.append(add_list1[add_idx])
            final_pnt_list.append(address_gpd.loc[gpd_idx, "geometry"])

    add_geo_pd = pd.DataFrame(data={"mail_add": final_add_list, "geometry": final_pnt_list})
    add_geo_gpd = gpd.GeoDataFrame(data=add_geo_pd, geometry="geometry")
    add_geo_gpd.plot()
    plt.show()
