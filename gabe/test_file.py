import geopandas as gpd

#countyGDF = gpd.read_file("./cdzipcounty.shp")
countyGDF = gpd.read_file("./ACS_2016_5YR_TRACT_13_GEORGIA.gdb")

countyGDF.head()

print("Hello world")