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


def constructLayerPrompt(layerOptions):
    layerPrompt = ("Press ENTER to search top zip codes by race. To sort zip codes "
        "by other socioeconomic factors, type one of the following options:\n")
    for layer in layerOptions:
        layerPrompt += ("\t" + layer + "\n")
    return layerPrompt 


def selectLayer(layerOptions, cleanLayerOptions):
    layerPrompt = constructLayerPrompt(layerOptions)
    while(True):
        rawResponse = input(layerPrompt)
        cleanResponse = rawResponse.lower().replace(" ", "")
        if(cleanResponse == ""): cleanResponse = "race"
        if(cleanResponse in cleanLayerOptions):
            print("You chose " + cleanResponse + "!")
            return cleanResponse
        print("Input error. Please try again.\n")


def loadLayerData(layerName, layerOptions, acs_meta, acs_counts):
    if (layerName == layerOptions[0]): #Race
        acs_layer = ["X02_RACE"]
    elif (layerName == layerOptions[1]): #Hispanic/Latino
        acs_layer = ["X03_HISPANIC_OR_LATINO_ORIGIN"]
    elif (layerName == layerOptions[2]): #Healthcare
        acs_layer = ["X27_HEALTH_INSURANCE"]
    elif (layerName == layerOptions[3]): #Food stamps
        acs_layer = ["X22_FOOD_STAMPS"]
    else: # Error -- need to account for new layer
        print("Error: Layer " + layerName + "not accounted for in find_top_n.py/loadLayerData")
        acs_layer = None

    return process.process_acs(acs_layers=acs_layer)


def convertColumnsToFullName(acs_meta, layerDF):
    fullNameMap = utils.get_col_map_to_names(acs_meta, layerDF)
    shortColNames = layerDF.columns[1:-1] # exclude GEOID and Geometry
    columnFullNames = (["GEOID"] + [fullNameMap[shortCol] for shortCol in shortColNames] + ["geometry"])
    layerDF.columns = columnFullNames
    return layerDF


def removeExtraneousRaceCols(layerDF):
    removedColumns = []
    preservedColumns = (
        ["GEOID",
        "WHITE ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: White alone or in combination with one or more other races -- (Estimate)",
        "BLACK OR AFRICAN AMERICAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: Black or African American alone or in combination with one or more other races -- (Estimate)",
        "AMERICAN INDIAN AND ALASKA NATIVE ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: People who are American Indian or Alaska Native alone or in combination with one or more other races -- (Estimate)",
        "ASIAN ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: Asian alone or in combination with one or more other races -- (Estimate)",
        "NATIVE HAWAIIAN AND OTHER PACIFIC ISLANDER ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: Native Hawaiian and Other Pacific Islander alone or in combination with one or more other races -- (Estimate)",
        "SOME OTHER RACE ALONE OR IN COMBINATION WITH ONE OR MORE OTHER RACES: Total: Some other race alone or in combination with one or more other races -- (Estimate)",
        "geometry"])
    removedColumns = [col for col in layerDF.columns if col not in preservedColumns]
    return removedColumns


def dropUnwantedColumns(layer, cleanLayerOptions, layerDF):
    # TODO: may also want to use this method to drop other unwanted/extraneous columns
    removed_cols = []
    removed_cols += utils.get_margin_of_error_list(layerDF.columns)
    if(layer == cleanLayerOptions[0]): #Race
        removed_cols += removeExtraneousRaceCols(layerDF)
    else:
        # TODO: write methods to remove unwanted columns for other layers
        pass
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


def convertValuesToPercentages(acs_meta, acs_counts, layerDF):
    acs_counts_trunc = getPopulationByCensusTract(acs_meta, acs_counts)
    layerDF = layerDF.merge(acs_counts_trunc, on="GEOID", how="inner")
    print(layerDF.columns)
    #now convert raw values to percentages
    return layerDF


def addZipCodeColumn(layerDF):
    zipCodeDF = pd.read_csv('tract_to_zip_out3.csv', dtype=str)
    zipCodeDF.columns = ["GEOID", "ZIPCODE"]
    layerDF["GEOID"] = [tractNum[-6:] for tractNum in layerDF["GEOID"]]
    layerDF = layerDF.merge(zipCodeDF, on="GEOID", how="inner")
    return layerDF


def find_top_n():
    layerOptions = ["Race", "Hispanic or Latino", "Healthcare", "Food Stamps"]
    cleanLayerOptions = [layer.lower().replace(" ", "") for layer in layerOptions]
    layer = selectLayer(layerOptions, cleanLayerOptions)
    acs_meta, acs_counts = process.process_meta()
    layer_df_generator = loadLayerData(layer, cleanLayerOptions, acs_meta, acs_counts)
    layerDF = next(layer_df_generator)
    layerDF = convertColumnsToFullName(acs_meta, layerDF)
    layerDF = dropUnwantedColumns(layer, cleanLayerOptions, layerDF)
    print(layerDF.to_numpy())
    layerDF = convertValuesToPercentages(acs_meta, acs_counts, layerDF)
    layerDF = addZipCodeColumn(layerDF)
    print(layerDF.to_numpy())

    
    #separate method for differnt prompts -- e.g. choose race, choose healthcare status...

if __name__ == '__main__':
    find_top_n()