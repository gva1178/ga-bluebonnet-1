import fiona
import geopandas as gpd
import pandas as pd
import os
import pprint
import process
import matplotlib.pyplot as plt
import censusToZip

from collections import defaultdict
import utils
import pandas


def select_cols(gdf, removed_cols, names_to_cols, all=False):
    if all:
        return gdf.columns

    cols_to_names = {}
    for name, col in names_to_cols.items():
        #print("NAME: " + str(name))
        #print("COL: " + str(col))
        if name.startswith('RACE'):
            cols_to_names[col] = name
        else:
            removed_cols.append(col)
    return cols_to_names


def analyze_tracts():
    acs_meta, acs_counts = process.process_meta()
    print("ACS_META:")
    print("COLUMNS: ")
    print(acs_meta.columns)
    print(acs_meta.to_numpy())
    #print(acs_meta[1:4])
    #print(acs_counts.head())
    acs_layers = process.list_layers(exclude_meta=True)
    #race_layers = acs_layers[1:4]
    race_layers = acs_layers[1:2]
    print("Race layers: " + str(race_layers))
    acs_geo = process.get_acs_geo()
    #print(acs_geo.shape)

    for gdf in process.process_acs(exclude_meta=True, acs_layers=race_layers):
        print("COLUMNS: ")
        print(gdf.columns)
        '''
        print("ROWS: ")
        print(gdf.index)
        '''
        geoCol = gdf['GEOID']
        geoCol = [geoID[-6:] for geoID in geoCol]
        #print("YAAA")
        #print(geoCol)
        gdf['GEOID'] = [geoID[-6:] for geoID in gdf['GEOID']]
        #this works
        #print(gdf.to_numpy())
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

def readZipCodeMapping():
    tract_to_zipCode_df = pd.read_csv('tract_to_zip_out3.csv', dtype=str)
    return tract_to_zipCode_df

def top_n_tracts(acs_meta, acs_counts, gdf, acs_geo, n):
    # place holder right now until we figure out how to integrate the margin of errors
    names_to_cols = utils.get_full_name_map_from_cols(acs_meta, gdf.columns)
    cols, removed_cols = utils.remove_margin_of_error(names_to_cols)
    print("Cols OGG:")
    print(cols)
    print("Removed Cols OGG:")
    print(removed_cols)
    # first, specify the columns we care about
    #this seems to be removing all the columns right now
    cols = select_cols(gdf, removed_cols, cols)  # {col_name : full_name}
    print("Cols After:")
    print(cols)
    print("Removed Cols After:")
    print(removed_cols)

    # then, find the total count columns
    count_col, cols = find_count_cols(cols)
    # then, remove all columns that we don't care about
    print("GDF COLUMNS (OLD):")
    print(cols)
    print("GDF OLD")
    print(gdf.to_numpy())
    gdf.drop(columns=removed_cols, inplace=True)

    cols = [(col, f'{col}_PERCENT') for col in cols]

    # for each column, compare to the total count value to create a percentage
    for col, col_p in cols:
        gdf.eval(f'{col_p} = {col} / {count_col}', inplace=True)

    '''
    print("PRE 1")
    print(gdf['GEOID'])
    # Don't need the below line b/c already truncated
    #gdf['GEOID'] = gdf['GEOID'].apply(lambda x: x[7:])
    print("PRE 2")
    print(gdf['GEOID'])
    '''
    zipCodeDF = readZipCodeMapping()
    zipCodeDF.columns = ["GEOID", "ZIP_CODE"]
    zipCodeDF = gpd.GeoDataFrame(zipCodeDF)
    print(zipCodeDF.head())
    print("GDF COLUMNS:")
    print(gdf.columns)
    #make sure we don't lose any rows on the original
    merged = gdf.merge(zipCodeDF, on='GEOID', how='outer')
    #print(merged.columns)
    #merged = merged.merge(acs_geo, on='GEOID', how='outer')
    print("MERGED:")
    print(merged.columns)
    cols_to_name = utils.get_col_map_to_names(acs_meta, [col[0] for col in cols])
    new_cols = merged.columns.values
    new_cols[new_cols == 'geometry_y'] = 'geometry'
    merged.columns = new_cols
    print("NEW MERGED:")
    print(merged.to_numpy())

    merged = gpd.GeoDataFrame(merged)

    print(merged.columns)

    mergedCollapsedOnZip = merged.groupby(by=["ZIP_CODE"], as_index=False).sum()

    print(mergedCollapsedOnZip.columns)

    #IMPT -- we actually want the select cols() function not to drop columns that don't 
    #begin with race -- because columns like "Black ... alone or in combination" are 
    #the ones we actually want

    '''
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
        #plt.show()
        plt.close(fig)
    '''

if __name__ == '__main__':
    analyze_tracts()