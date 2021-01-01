import pandas as pd
import geopandas as gpd
import os

ROOT_DIR = '/Users/oro/work/ga-psc'
#ROOT_DIR = '/Users/gabrielvoorhis-allen/CSProjects/Bluebonnet/ga-psc'
TRACT_TO_ZCTA_DATA = 'data/210101_tract_to_zip_GA.csv'
ACS_GDB = 'data/ACS_2016_5YR_TRACT_13_GEORGIA.gdb'

def build_national_tract_to_zip_df():
    # In Georgia, can assume ZCTAs are equivalent to ZIPs, so even though the database
    # is a mapping from Tracts to ZCTAs, it functions as a mapping to ZIPs
    national_tract_to_zip_path = os.path.join(ROOT_DIR, TRACT_TO_ZCTA_DATA)
    national_tract_to_zip_df = pd.read_csv(national_tract_to_zip_path, dtype='str')
    national_tract_to_zip_df = national_tract_to_zip_df[["TRACT", "ZIP"]]
    #print(national_tract_to_zip_df.head())
    return national_tract_to_zip_df

def build_georgia_tracts_df():
    georgia_tracts_path = os.path.join(ROOT_DIR, ACS_GDB)
    georgia_df = gpd.read_file(georgia_tracts_path, layer="X00_COUNTS")
    georgia_tracts_df = georgia_df["GEOID"]
    georgia_tracts_df = [geoID[-11:] for geoID in georgia_tracts_df]
    georgia_tracts_df = gpd.GeoDataFrame(georgia_tracts_df)
    georgia_tracts_df.columns = ["TRACT"]
    #print(georgia_tracts_df.head())
    return georgia_tracts_df


def mapTractsToZipCodes():
    national_tract_to_zip_df = build_national_tract_to_zip_df()
    georgia_tracts_df = build_georgia_tracts_df()
    georgia_tracts_to_zip_df = national_tract_to_zip_df.merge(georgia_tracts_df, how='inner')
    georgia_tracts_to_zip_df.set_index("TRACT", inplace=True)
    #print(georgia_tracts_to_zip_df.head())
    georgia_tracts_to_zip_df.to_csv("tract_to_zip_out.csv")
    return


if __name__ == '__main__':
    mapTractsToZipCodes()