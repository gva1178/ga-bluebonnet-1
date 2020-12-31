import fiona
import geopandas as gpd
import os


ROOT_DIR = '/Users/oro/work/ga-psc'
# ROOT_DIR = '/Users/gabrielvoorhis-allen/CSProjects/Bluebonnet/ga-psc'
ACS_GDB = 'data/ACS_2016_5YR_TRACT_13_GEORGIA.gdb'
ZIP_SHAPE = 'data/shp/gazip.shp'
GEOMETRY_LAYER = 'ACS_2016_5YR_TRACT_13_GEORGIA'
ZCTA_GEO = 'data/cb_2016_us_zcta510_500k'


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


if __name__ == '__main__':
    a = get_zcta_geo()
    print(a.head())
