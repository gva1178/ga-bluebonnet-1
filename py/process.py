import fiona
import geopandas as gpd
import os


ROOT_DIR = '/Users/oro/work/ga-psc'
CSV_DIR = '/Users/oro/work/ga-psc/data/csv'
#ROOT_DIR = '/Users/gabrielvoorhis-allen/CSProjects/Bluebonnet/ga-psc'
ACS_GDB = 'data/ACS_2016_5YR_TRACT_13_GEORGIA.gdb'
ZIP_SHAPE = 'data/shp/gazip.shp'
GEOMETRY_LAYER = 'ACS_2016_5YR_TRACT_13_GEORGIA'
ZCTA_GEO = 'data/cb_2016_us_zcta510_500k'
FIPS = 'data/txt/FIPS.txt'
STUFF = 'all the data - stuff.csv'
RACE = 'all the data - Race.csv'
ANCESTRY = 'all the data - Ancestry.csv'
COUNTIES = 'csv/GA counties summary.csv'
FIPS = 'txt/fips.txt'
TRACT_TO_ZIP = 'csv/tract_to_zip_out3.csv'
DATA_DIR = '/Users/oro/work/ga-psc/data'

def get_meta(acs_file, acs_layers):
    acs_meta = acs_layers.pop(acs_layers.index('TRACT_METADATA_2016'))
    return gpd.read_file(acs_file, driver='FileGDB', layer=acs_meta)


def get_counts(acs_file, acs_layers):
    acs_counts = acs_layers.pop(acs_layers.index('X00_COUNTS'))
    return gpd.read_file(acs_file, driver='FileGDB', layer=acs_counts)


def process_meta():
    acs_file = os.path.join(ROOT_DIR, ACS_GDB)
    acs_layers = fiona.listlayers(acs_file)
    print("Layers: " + (str(acs_layers)))
    df_meta = get_meta(acs_file, acs_layers)
    df_counts = get_counts(acs_file, acs_layers)
    return df_meta, df_counts


def list_layers(exclude_meta=False):
    acs_file = os.path.join(ROOT_DIR, ACS_GDB)
    acs_layers = fiona.listlayers(acs_file)

    if exclude_meta:
        acs_layers.pop(acs_layers.index('TRACT_METADATA_2016'))
        acs_layers.pop(acs_layers.index('X00_COUNTS'))

    return acs_layers


def process_acs(exclude_meta=False, acs_layers=None):
    acs_file = os.path.join(ROOT_DIR, ACS_GDB)

    if acs_layers is None:
        acs_layers = fiona.listlayers(acs_file)

        if exclude_meta:
            acs_layers.pop(acs_layers.index('TRACT_METADATA_2016'))
            acs_layers.pop(acs_layers.index('X00_COUNTS'))

    for acs_layer in acs_layers:
        yield gpd.read_file(acs_file, driver='FileGDB', layer=acs_layer)


def get_acs_geo():
    acs_file = os.path.join(ROOT_DIR, ACS_GDB)
    return gpd.read_file(acs_file, driver='FileGDB', layer=GEOMETRY_LAYER)


def get_zcta_geo():
    zcta_file = os.path.join(ROOT_DIR, ZCTA_GEO)
    return gpd.read_file(zcta_file, driver='FileGDB', )



# placeholder to analyze the zip code shapefile
def process_zip():
    zip_file = os.path.join(ROOT_DIR, ZIP_SHAPE)
    return gpd.read_file(zip_file)




def join_csvs():
    county_file = os.path.join(DATA_DIR, COUNTIES)
    county_df = gpd.read_file(county_file)
    county_to_fips = load_fips()
    county_df['COUNTY_CODE'] = county_df['County'].apply(lambda x: county_to_fips[x])
    race_file = os.path.join(CSV_DIR, RACE)
    ancestry_file = os.path.join(CSV_DIR, ANCESTRY)
    stuff_file = os.path.join(CSV_DIR, STUFF)

    merged = join_acses([race_file, ancestry_file, stuff_file], county_to_fips)
    dbl_merged = county_df.merge(merged, on='COUNTY_CODE', how='outer')
    dbl_merged.to_csv('stuff_all.csv')


def join_acses(l, county_to_fips):
    acs = l[0]
    acs = gpd.read_file(acs)
    acs['COUNTY_CODE'] = acs['GEOID'].apply(lambda x: x[7:][:5])
    for layer in l[1:]:
        new_layer = gpd.read_file(layer)
        if 'stuff' not in layer:
            new_layer['COUNTY_CODE'] = new_layer['GEOID'].apply(lambda x: x[7:][:5])
        else:
            new_layer['COUNTY_CODE'] = new_layer['cdczipnov3_Intersect1.County_1'].apply(lambda x: county_to_fips[x])
        acs = acs.merge(new_layer, on='COUNTY_CODE', how='outer')
    return acs


def load_fips():
    county_to_fips = {}
    ctf = os.path.join(DATA_DIR, FIPS)
    with open(ctf) as ctf_file:
        for line in ctf_file:
            code, county, state = line.split('\t')
            county_to_fips[county] = code
    return county_to_fips


def join_csv_with_county(csv_file, county_df, county_to_fips):
    acs = gpd.read_file(csv_file)
    if 'stuff' not in csv_file:
        acs['COUNTY_CODE'] = acs['GEOID'].apply(lambda x: x[7:][:5])
    else:
        acs['COUNTY_CODE'] = acs['cdczipnov3_Intersect1.County_1'].apply(lambda x: county_to_fips[x])
    merged = county_df.merge(acs, on='COUNTY_CODE', how='outer')
    return merged


if __name__ == '__main__':
    a = get_zcta_geo()
    print(a.head())
