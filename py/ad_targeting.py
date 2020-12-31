import fiona
import geopandas as gpd
import pandas as pd
import os
import pprint
import process
import matplotlib.pyplot as plt
import censusToZip
import sys

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


def addZipCodeColumn(layerDF):
    zipCodeDF = pd.read_csv('tract_to_zip_out3.csv', dtype=str)
    zipCodeDF.columns = ["GEOID", "ZIPCODE"]
    layerDF["GEOID"] = [tractNum[-6:] for tractNum in layerDF["GEOID"]]
    layerDF = layerDF.merge(zipCodeDF, on="GEOID", how="inner")
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


def build_ad_targets_from_columns(selectedLayers, selectedColumnsMap, acs_meta, acs_counts):
    
    # startingDF; to be built over time by merging
    target_scores_DF = gpd.GeoDataFrame(addZipCodeColumn(acs_counts["GEOID"])) #RN this is the full GEOID, not just tractNum

    for index, layer_df in enumerate(process.process_acs(acs_layers=selectedLayers)):
        layer = selectedLayers[index]
        print(layer)
        selectedColumns = selectedColumnsMap[layer]
        layer_df = convertColumnsToFullName(acs_meta, layer_df)
        #write_txtFile_with_column_options(layer, layer_df) #helpful for building selectedColumnsMap (i.e. choosing which columns to prioritize)
        layer_df = dropUnwantedColumns(layer_df, selectedColumns)
        layer_df = addZipCodeColumn(layer_df)
        layer_df = collapseRowsOnZipCodeColumn(layer_df)
        #layer_df = addCensusTractPopulationColumn(acs_meta, acs_counts, layer_df) # The population numbers in the counts layer don't seem to make sense (too low)
        # can use "total" column for the given layer to get percentages
        layer_df = convertValuesToPercentages(acs_meta, acs_counts, layer_df)

        for column in selectedColumns:
            # make sure to make sure it's not None, since we're padding one-element tuples with None
            if column != None:
                # convert column entries to percentiles
                # make new dataframe, columnDF, with only zip codes and the current column
                #target_scores_DF.merge(columnDF, on="ZIPCODE")

        #layer_df = sortZipCodesByDesiredColumn(layer, cleanLayerOptions, layer_df)
        # ^ Use this to sort target_scores_DF by overall scores

    return target_scores_DF
    

def build_ad_targets():
    acs_meta, acs_counts = process.process_meta()
    enviro_justice_layers = ["X02_RACE", "X17_POVERTY", "X22_FOOD_STAMPS", "X27_HEALTH_INSURANCE"]
    # The columns in this map are placeholders until we get more clarity on the exact columns we want to include
    # we're mapping from layer names to tuples of columnn names, so if we're only going to use one column from a layer, still make it a tuple with None as the second item -- e.g. ("selectedColumn", None)
    enviro_justice_cols = {"X02_RACE" : ("RACE: Total: Total population -- (Estimate)", "RACE: White alone: Total population -- (Estimate)"), 
                            "X17_POVERTY" : ("POVERTY STATUS IN THE PAST 12 MONTHS BY SEX BY AGE: Total: Population for whom poverty status is determined -- (Estimate)", None), 
                            "X22_FOOD_STAMPS" : ("RECEIPT OF FOOD STAMPS/SNAP IN THE PAST 12 MONTHS BY POVERTY STATUS IN THE PAST 12 MONTHS FOR HOUSEHOLDS: Total: Households -- (Estimate)", "Healthcare"), 
                            "X27_HEALTH_INSURANCE" : ("PRIVATE HEALTH INSURANCE BY RATIO OF INCOME TO POVERTY LEVEL IN THE PAST 12 MONTHS BY AGE: Total: Civilian noninstitutionalized population for whom poverty status is determined -- (Estimate)", None)}

    target_scores_DF = build_ad_targets_from_columns(enviro_justice_layers, enviro_justice_cols, acs_meta, acs_counts)
    #target_scores_DF.to_csv("enviro_justice_scores.csv")
    return 


if __name__ == '__main__':
    build_ad_targets()