import fiona
import geopandas as gpd
import pandas as pd
import os
import pprint
import process
import matplotlib.pyplot as plt
import censusToZip
import sys
from sklearn import preprocessing

from functools import partial

from collections import defaultdict
import utils
import pandas


def convertColumnsToFullName(acs_meta, layerDF):
    fullNameMap = utils.get_col_map_to_names(acs_meta, layerDF)
    shortColNames = layerDF.columns[1:-1] # exclude GEOID and Geometry
    columnFullNames = (["GEOID"] + [fullNameMap[shortCol] for shortCol in shortColNames] + ["geometry"])
    layerDF.columns = columnFullNames
    return layerDF


def removeUnselectedCols(layerDF, selectedColumns):
    removedColumns = []
    preservedColumns = (["GEOID"])
    preservedColumns += selectedColumns
    removedColumns = [col for col in layerDF.columns if col not in preservedColumns]
    return removedColumns


def dropUnwantedColumns(layerDF, selectedColumns):
    removed_cols = []
    removed_cols += utils.get_margin_of_error_list(layerDF.columns)
    removed_cols += removeUnselectedCols(layerDF, selectedColumns)
    layerDF = layerDF.drop(columns=removed_cols)
    return layerDF


def getPopulationByCensusTract(acs_meta, acs_counts):
    acs_counts_full_cols = utils.get_full_name_list_from_cols(acs_meta, acs_counts.columns)
    acs_counts.columns = (["GEOID"] + acs_counts_full_cols + ["geometry"])
    countsColToKeep = ["UNWEIGHTED SAMPLE COUNT OF THE POPULATION: Total: Total population -- (Estimate)"]
    removed_cols = ([col for col in acs_counts_full_cols if col not in countsColToKeep] + ["geometry"])
    acs_counts_trunc = acs_counts.drop(columns=removed_cols)
    acs_counts_trunc.columns = ["GEOID", "Total population in census tract"]
    return acs_counts_trunc


def addCensusTractPopulationColumn(acs_meta, acs_counts, layerDF):
    populationByTractDF = getPopulationByCensusTract(acs_meta, acs_counts)
    layerDF = layerDF.merge(populationByTractDF, on="GEOID", how='inner')
    return layerDF


def addZipCodeColumn(layerDF, zip_df):
    layerDF["GEOID"] = [tractNum[-6:] for tractNum in layerDF["GEOID"]]
    layerDF = layerDF.merge(zip_df, on="GEOID", how="inner")
    return layerDF


def collapseRowsOnZipCodeColumn(layerDF):
    layerDF = layerDF.set_index('GEOID')
    layerDF = layerDF.groupby(["ZIPCODE"], axis=0).sum()
    return layerDF


def convertValuesToPercentages(acs_meta, acs_counts, layerDF):
    #acs_counts_trunc = getPopulationByCensusTract(acs_meta, acs_counts)
    #layerDF = layerDF.merge(acs_counts_trunc, on="GEOID", how="inner")
    totalPopColumn = layerDF.pop("RACE: Total: Total population -- (Estimate)")
    layerDF = layerDF.divide(totalPopColumn, axis='index')
    return layerDF


def findTop10Zips(columnNum, layerDF):
    columnToSort = [layerDF.columns[columnNum]]
    layerDF = layerDF.sort_values(by=columnToSort, axis="index", ascending=False)
    #layerDF.columns = ["White", "Black", "Native American", "Asian", "Native Hawaiian", "Other"]
    #print(layerDF.head())
    top10Zips = layerDF.index.values[:10]
    percentages = layerDF[columnToSort].values[:10]
    percentagesFlattened = [percent[0] for percent in percentages]
    top10Zips = list(zip(top10Zips, percentagesFlattened))
    return top10Zips


def processResponse(response, cleanPromptOptions, layer, layerDF):
    retryBool = True
    for index, value in enumerate(cleanPromptOptions):
        if response == value:
            retryBool = False
            top10Zips = findTop10Zips(index, layerDF)
            printString = ("The top 10 zips for \"" + layer + "\"=\"" + response + "\" are:\n")
            for zipCode in top10Zips:
                printString += ("\t" + str(zipCode[0]) + " (percent = " + str(zipCode[1]) + ")\n")
            print(printString)
        elif response == "quit":
            retryBool = False 

    return retryBool


def sortZipCodesByDesiredColumn(layer, cleanLayerOptions, layerDF):
    promptOptions = generatePromptOptions(layer, cleanLayerOptions)
    cleanPromptOptions = [option.lower().strip() for option in promptOptions]
    prompt = ("Please select the column for which you'd like to return the top 10 zip codes:\n")
    for option in promptOptions:
        prompt += ("\t" + option + "\n")
    response = ""
    while(response != "quit"):
        response = input(prompt).lower().strip()
        retryBool = processResponse(response, cleanPromptOptions, layer, layerDF)
        if (retryBool): print("Input error. Please try again.\n")

    return layerDF


def parse_enviro_justice_cols(enviro_justice_layers, enviro_justice_cols):
    for layer in enviro_justice_layers:
        print("Layer: " + str(layer))
        if(layer in enviro_justice_cols and len(enviro_justice_cols[layer]) > 1):
            for column in enviro_justice_cols[layer]:
                print(column)
        elif(layer in enviro_justice_cols):
            print(enviro_justice_cols[layer])

        print(len(enviro_justice_cols["Food Stamps"]))
    return


def write_txtFile_with_column_options(layer, layer_df):
    f = open("namesOfColumnsForSelectedRows.txt", "a")
    f.write("\n\nLAYER: " + str(layer))
    for column in layer_df.columns:
        f.write("\n" + column)


NON_SUMMABLE = {'geometry', 'ZIPCODE', 'GEOID', None}


def cols_to_percentiles(df, inplace=True):
    for col in df.columns:
        if col not in NON_SUMMABLE:
            len = df[col].size-1
            if inplace:
                df[col] = df[col].rank(method='max').apply(lambda x: 100.0*(x-1)/len)
            else:
                df[f'{col}_PERCENTILE'] = df[col].rank(method='max').apply(lambda x: 100.0 * (x - 1) / len)


def load_zip():
    zipCodeDF = pd.read_csv('tract_to_zip_out3.csv', dtype=str)
    zipCodeDF.columns = ["GEOID", "ZIPCODE"]
    return zipCodeDF

def build_ad_targets_from_columns(selectedLayers, selectedColumnsMap, column_weights, acs_meta, acs_counts):
    zip_df = load_zip()

    # startingDF; to be built over time by merging relevant columns into it
    scores_df = gpd.GeoDataFrame(addZipCodeColumn(acs_counts, zip_df)["GEOID"])
    scores_df = addZipCodeColumn(scores_df, zip_df)
    scores_df = collapseRowsOnZipCodeColumn(scores_df)


    print('created base')
    for index, layer_df in enumerate(process.process_acs(acs_layers=selectedLayers)):
        layer = selectedLayers[index]
        print('creating ', layer)
        selectedColumns = selectedColumnsMap[layer]
        layer_df = convertColumnsToFullName(acs_meta, layer_df)
        #write_txtFile_with_column_options(layer, layer_df) #helpful for building selectedColumnsMap (i.e. choosing which columns to prioritize)
        layer_df = dropUnwantedColumns(layer_df, selectedColumns)
        layer_df = addZipCodeColumn(layer_df,zip_df)
        layer_df = collapseRowsOnZipCodeColumn(layer_df)
        #layer_df = addCensusTractPopulationColumn(acs_meta, acs_counts, layer_df) # The population numbers in the counts layer don't seem to make sense (too low)
        # can use "total" column for the given layer to get percentages
        # layer_df = convertValuesToPercentages(acs_meta, acs_counts, layer_df)
        cols_to_percentiles(layer_df)
        print('merge', layer)
        scores_df = scores_df.merge(layer_df, on='ZIPCODE', how='outer')

    return scores_df


# row is a series, which can be thought of as a list of tuples of (column, val)
def generate_ad_score_for_row(weights, row):
    total = 0
    for index, col in enumerate(row.iteritems()):
        if col[0] != 'ZIPCODE':
            total += weights[index] * row[1]
    return total


def score_and_scale(scores, weights):
    scores['ad_score'] = scores.apply(partial(generate_ad_score_for_row, weights), axis=1)
    ad_scores = scores['ad_score'].values.reshape(-1, 1)
    min_max_scaler = preprocessing.MinMaxScaler()
    ad_score_scaled = min_max_scaler.fit_transform(ad_scores)
    scores['ad_score_scaled'] = ad_score_scaled
    return scores



def build_ad_targets(export=False):
    acs_meta, acs_counts = process.process_meta()
    enviro_justice_layers = ["X02_RACE", "X17_POVERTY", "X22_FOOD_STAMPS", "X27_HEALTH_INSURANCE"]

    # The columns in this map are placeholders until we get more clarity on the exact columns we want to include
    # we're mapping from layer names to tuples of columnn names, so if we're only going to use one column from a layer, still make it a tuple with None as the second item -- e.g. ("selectedColumn", None)
    enviro_justice_cols = {"X02_RACE" : ("GEOID", "RACE: Total: Total population -- (Estimate)", "RACE: White alone: Total population -- (Estimate)"), 
                            "X17_POVERTY" : ("GEOID", "POVERTY STATUS IN THE PAST 12 MONTHS BY SEX BY AGE: Total: Population for whom poverty status is determined -- (Estimate)", None), 
                            "X22_FOOD_STAMPS" : ("GEOID", "RECEIPT OF FOOD STAMPS/SNAP IN THE PAST 12 MONTHS BY POVERTY STATUS IN THE PAST 12 MONTHS FOR HOUSEHOLDS: Total: Households -- (Estimate)", "Healthcare"), 
                            "X27_HEALTH_INSURANCE" : ("GEOID", "PRIVATE HEALTH INSURANCE BY RATIO OF INCOME TO POVERTY LEVEL IN THE PAST 12 MONTHS BY AGE: Total: Civilian noninstitutionalized population for whom poverty status is determined -- (Estimate)", None)}
    enviro_justice_weights = [1, 1, 1, 1, 1]
    base_percentiles = build_ad_targets_from_columns(enviro_justice_layers, enviro_justice_cols, enviro_justice_weights, acs_meta, acs_counts)
    scores = score_and_scale(base_percentiles,enviro_justice_weights)

    if export:
        scores.to_csv('ad_scores.csv')

    return scores

if __name__ == '__main__':
    build_ad_targets(export=True)