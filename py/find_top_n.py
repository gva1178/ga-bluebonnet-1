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
            print("You chose \"" + cleanResponse + ".\" Loading options: \n")
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
        "RACE: Total: Total population -- (Estimate)",
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


def generatePromptOptions(layerName, layerOptions):
    promptOptions = []
    if (layerName == layerOptions[0]): #Race
        promptOptions += ["White", "Black", "Native American", "Asian", "Native Hawaiian", "Other"]
    elif (layerName == layerOptions[1]): #Hispanic/Latino
        print(layerName + "Not implemented yet")
        pass
    elif (layerName == layerOptions[2]): #Healthcare
        print(layerName + "Not implemented yet")
        pass
    elif (layerName == layerOptions[3]): #Food stamps
        print(layerName + "Not implemented yet")
        pass
    else: # Error -- need to account for new layer
        print("Error: Layer " + layerName + "not accounted for in find_top_n.py/generatePromptOptions")

    return promptOptions


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


def find_top_n():
    layerOptions = ["Race", "Hispanic or Latino", "Healthcare", "Food Stamps"]
    cleanLayerOptions = [layer.lower().replace(" ", "") for layer in layerOptions]
    layer = selectLayer(layerOptions, cleanLayerOptions)
    acs_meta, acs_counts = process.process_meta()
    layer_df_generator = loadLayerData(layer, cleanLayerOptions, acs_meta, acs_counts)
    layerDF = next(layer_df_generator)
    layerDF = convertColumnsToFullName(acs_meta, layerDF)
    layerDF = dropUnwantedColumns(layer, cleanLayerOptions, layerDF)
    #layerDF = addCensusTractPopulationColumn(acs_meta, acs_counts, layerDF) # The population numbers in the counts layer don't seem to make sense (too low)
    layerDF = addZipCodeColumn(layerDF)
    layerDF = collapseRowsOnZipCodeColumn(layerDF)
    layerDF = convertValuesToPercentages(acs_meta, acs_counts, layerDF)
    layerDF = sortZipCodesByDesiredColumn(layer, cleanLayerOptions, layerDF)
    
    #separate method for different prompts -- e.g. choose race, choose healthcare status...

if __name__ == '__main__':
    find_top_n()