import geopandas as gpd
from pathlib import Path

# Get the current script's directory
script_dir = Path(__file__).resolve().parent

# Path to the matched_addresses GeoPackage file
matched_geopackage_path = script_dir / 'matched_addresses.gpkg'

# Read the GeoPackage file into a GeoDataFrame
matched_gdf = gpd.read_file(matched_geopackage_path)

# Display the first few rows of the GeoDataFrame
print(matched_gdf.head())

# Optionally, display the column names to understand the structure of the GeoDataFrame
print(matched_gdf.columns)

# For further inspection, you can also plot the geometries
matched_gdf.plot()
