
import pandas as pd
import os
ROOT_DIR = '/Users/oro/work/ga-psc'
# ROOT_DIR = '/Users/gabrielvoorhis-allen/CSProjects/Bluebonnet/ga-psc'
#TRACT_TO_ZCTA_DATA = 'data/zcta-to-census-tract-201222.txt'
TRACT_TO_ZCTA_DATA = '210101_tract_to_zip_GA.csv'
ZCTA_TO_ZIP_DATA = 'data/GA_zcta_to_zc_map.csv'

tract_to_zcta_mapping_dict = {}
zcta_to_zc_mapping_dict = {}
# why don't changes to this df seem to be persistent across functions?
tract_to_zipcode_df = pd.DataFrame()



def get_tract_to_zipcode_df():
    '''
    # tract_to_zipcode_df is globally defined -- why doesn't this work?
    if tract_to_zipcode_df.empty:
        init() 
    '''
    if tract_to_zcta_mapping_dict == {} or zcta_to_zc_mapping_dict == {}:
        pass
        #init()
    # init calls 'write_tract_to_zipcode_df,' so it'll be defined now
    return tract_to_zipcode_df

def write_tract_to_zipcode_df(tract_to_zcta, zcta_to_zip):
    tract_to_zipcode_df = tract_to_zcta.join(zcta_to_zip.set_index('ZCTA'), on='ZCTA', how='inner')
    tract_to_zipcode_df = tract_to_zipcode_df[['TRACT', 'ZIP_CODE']]
    '''
    print("FINAL DF: ")
    print(tract_to_zipcode_df.head())
    if tract_to_zipcode_df.empty:
        print("TRUE!!!   1\n\n")
    else:
        print("FALSE!!!  1\n\n")
    '''
    tract_to_zipcode_df.reset_index(drop=True, inplace=True)
    #print(tract_to_zipcode_df.head())
    tract_to_zipcode_df.to_csv('tract_to_zip_out3.csv', index=False)
    csvFile = pd.read_csv('tract_to_zip_out3.csv', dtype='str')
    '''
    print("FILE!!")
    print(csvFile.head())
    print("CHECKING AT END: ")
    print(tract_to_zipcode_df.head())
    '''
    return tract_to_zipcode_df

def write_tract_to_zipcode_CSV():
    '''
    # tract_to_zipcode_df is globally defined -- why doesn't this work?
    if tract_to_zipcode_df.empty:
        init() 
    '''
    if tract_to_zcta_mapping_dict == {} or zcta_to_zc_mapping_dict == {}:
        pass
        #init()
    
    csv_out = tract_to_zipcode_df.to_csv('tract_to_zip_out.csv')
    return



def get_zip_from_tract(TractNum):
    # these dictionaries are kind of vestigial -- now just store mapping in oracle df
    if tract_to_zcta_mapping_dict == {} or zcta_to_zc_mapping_dict == {}:
        init()
    
    zip_code = tract_to_zipcode_df[str(TractNum)]
    #print("CENSUS TRACT: " + str(TractNum))
    #print("ZIP CODE: " + zip_code)
    return zip_code


def build_tract_to_zcta_dict():
    tract_to_zcta_path = os.path.join(ROOT_DIR, TRACT_TO_ZCTA_DATA)

    #converts ZCTA5 and TRACT columns to string format so leading 0s are not omitted
    list_string_cols = ['ZCTA5', 'TRACT']
    dict_dtypes = {x : 'str' for x in list_string_cols}

    tract_to_zcta_mapping_df = pd.read_csv(tract_to_zcta_path, dtype=dict_dtypes)
    tract_to_zcta_mapping_df = tract_to_zcta_mapping_df[['TRACT', 'ZCTA5']]
    tract_to_zcta_mapping_df.columns=['TRACT', 'ZCTA']
    zipcode_mapping_dict_df = tract_to_zcta_mapping_df.to_dict('split')
    zipcode_mapping_dict = zipcode_mapping_dict_df['data']
    zipcode_mapping_dict = {pair[0]: pair[1] for pair in zipcode_mapping_dict}
    return tract_to_zcta_mapping_df

#Think that this is an unnecessary step for GA (in the input data file
#because ZCTA -> ZC is an identity relationship)
def build_zcta_to_zc_dict():
    zcta_to_zip_path = os.path.join(ROOT_DIR, ZCTA_TO_ZIP_DATA)

    #converts ZCTA5 and TRACT columns to string format so leading 0s are not omitted
    #don't think GA has any leading zeros, so unnecessary
    list_string_cols = ['ZCTA', 'ZIP_CODE']
    dict_dtypes = {x : 'str' for x in list_string_cols}

    zcta_to_zc_mapping_df = pd.read_csv(zcta_to_zip_path, dtype=dict_dtypes)
    #
    zcta_to_zc_mapping_df = zcta_to_zc_mapping_df[['ZCTA', 'ZIP_CODE']]
    zipcode_mapping_dict_df = zcta_to_zc_mapping_df.to_dict('split')
    zipcode_mapping_dict = zipcode_mapping_dict_df['data']
    zipcode_mapping_dict = {pair[0]: pair[1] for pair in zipcode_mapping_dict}
    return zcta_to_zc_mapping_df


def init():
    tract_to_zcta_mapping_df = build_tract_to_zcta_dict()
    zcta_to_zc_mapping_df = build_zcta_to_zc_dict()
    tract_to_zipcode_df = write_tract_to_zipcode_df(tract_to_zcta_mapping_df, zcta_to_zc_mapping_df)
    '''
    if tract_to_zipcode_df.empty:
        print("TRUE!!!   2\n\n")
    else:
        print("FALSE!!!  2\n\n")
    print(tract_to_zipcode_df.head())
    '''
    return tract_to_zipcode_df

# build database
def census_tracts_to_zipcode():
    init()
    return tract_to_zipcode_df


# join big dataframe with dataframe that's a mapping from census tracts to zip cats (intersection)
# then we'll have zip code in every layer

# after that, build new dataframe that's collapsed on the zip code row

if __name__ == '__main__':
    census_tracts_to_zipcode()