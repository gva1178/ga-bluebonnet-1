import fiona
import geopandas as gpd
import os
import pprint
import process
import utils


def analyze_tracts():
    acs_meta, acs_counts = process.process_meta()
    acs_layers = process.list_layers(exclude_meta=True)
    race_layers = acs_layers[1:4]

    for gdf in process.process_acs(exclude_meta=True, acs_layers=race_layers):
        top_n_tracts(acs_meta, acs_counts, gdf, 10)


def top_n_tracts(acs_meta, acs_counts, gdf, n):
    # just as a reference so we're sure what the short names actually mean
    full_names = utils.get_full_name_from_cols(acs_meta, gdf.columns)






if __name__ == '__main__':
    analyze_tracts()