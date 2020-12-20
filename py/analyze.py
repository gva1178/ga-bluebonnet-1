import fiona
import geopandas as gpd
import os
import pprint
import process
import matplotlib.pyplot as plt

from collections import defaultdict
import utils
import pandas


def select_cols(gdf, removed_cols, names_to_cols, all=False):
    if all:
        return gdf.columns

    cols_to_names = {}
    for name, col in names_to_cols.items():
        if name.startswith('RACE'):
            cols_to_names[col] = name
        else:
            removed_cols.append(col)
    return cols_to_names


def analyze_tracts():
    acs_meta, acs_counts = process.process_meta()
    acs_layers = process.list_layers(exclude_meta=True)
    race_layers = acs_layers[1:4]
    acs_geo = process.get_acs_geo()

    for gdf in process.process_acs(exclude_meta=True, acs_layers=race_layers):
        top_n_tracts(acs_meta, acs_counts, gdf, acs_geo, 10)


def find_count_cols(cols):
    total_count = 'Total: Total population'
    ccols, nccols = ([] for i in range(2))
    for col, name in cols.items():
        if total_count in name:
            ccols.append(col)
        else:
            nccols.append(col)

    if len(ccols) == 1:
        ccols = ccols[0]

    return ccols, nccols


def top_n_tracts(acs_meta, acs_counts, gdf, acs_geo, n):
    # place holder right now until we figure out how to integrate the margin of errors
    names_to_cols = utils.get_full_name_map_from_cols(acs_meta, gdf.columns)
    cols, removed_cols = utils.remove_margin_of_error(names_to_cols)
    # first, specify the columns we care about
    cols = select_cols(gdf, removed_cols, cols)  # {col_name : full_name}
    # then, find the total count columns
    count_col, cols = find_count_cols(cols)
    # then, remove all columns that we don't care about
    gdf.drop(columns=removed_cols, inplace=True)

    cols = [(col, f'{col}_PERCENT') for col in cols]

    # for each column, compare to the total count value to create a percentage
    for col, col_p in cols:
        gdf.eval(f'{col_p} = {col} / {count_col}', inplace=True)

    gdf['GEOID'] = gdf['GEOID'].apply(lambda x: x[7:])
    merged = gdf.merge(acs_geo, on='GEOID', how='outer')
    cols_to_name = utils.get_col_map_to_names(acs_meta, [col[0] for col in cols])
    new_cols = merged.columns.values
    new_cols[new_cols == 'geometry_y'] = 'geometry'
    merged.columns = new_cols

    merged = gpd.GeoDataFrame(merged)

    for col, col_p in cols:
        color_min = 0
        color_max = merged[col_p].max()

        fig, ax = plt.subplots(1, figsize=(30, 10))
        ax.axis('off')
        ax.set_title(cols_to_name[col], fontdict={'fontsize': '25', 'fontweight': '3'})
        ax.annotate('Source: ACS DATA', xy=(0.6, .05),
                    xycoords='figure fraction', fontsize=12, color='#555555')
        sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=color_min, vmax=color_max))
        sm.set_array([])
        fig.colorbar(sm)
        merged.plot(column=col_p, cmap='Blues', linewidth=0.8, ax=ax, edgecolor='0.8')
        print()
        plt.close(fig)


if __name__ == '__main__':
    analyze_tracts()