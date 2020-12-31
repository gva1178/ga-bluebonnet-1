import fiona
import geopandas as gpd
import pandas as pd
import os
import pprint
import process
import matplotlib.pyplot as plt
import sys


def percentiles_from_vals(df, cols):
    for col in cols:
        df[f'{col}_PERCENTILE'] = df[col].apply(lambda x: x*100)
    return df

