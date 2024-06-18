import geopandas as gpd

shapefile_path = 'C:/Users/kylebradshaw/repos/midterm/Data/Beaufort_Count_Streets/addresses.shp'

# Read the shapefile into a GeoDataFrame
gdf = gpd.read_file(shapefile_path)

# Print the contents of the GeoDataFrame
print(gdf)