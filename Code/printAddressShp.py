import geopandas as gpd
from pathlib import Path

# Get the current script's directory
script_dir = Path(__file__).resolve().parent

# Resolve the data path relative to the script directory
data_path = script_dir.parent / 'Data'

shapefile_path = data_path / "Beaufort_Count_Streets/addresses.shp"

# Read the shapefile
gdf = gpd.read_file(shapefile_path)

# Print out the column names
print("Column names in the shapefile:")
print(gdf.columns)
