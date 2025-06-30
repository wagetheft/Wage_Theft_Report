
import re
import pandas as pd
from constants.zipcodes import one_hundred_largest

fast_infer = True

def generate_generic_zipcode_for_city(tempA, n_char = 1):
    #adding a generic zipcode for each city -- used later to filter records:
    position = len(tempA)
    # n_char -- replace just last zip code char seems to work best
    suffix = "X" * n_char
    tempB = tempA[:position-n_char] + suffix
    return tempB


def InferZipcode(df, cityDict):
    if not df.empty:
        df = InferZipcodeFromCityName(df, cityDict)  # zipcodes by city name
        df = InferZipcodeFromCompanyName(df, cityDict) #zipcodes by company name
        df = InferZipcodeFromAddress(df, cityDict) #zipcodes by address that is full length
        df = InferZipcodeFromJurisdictionName(df, cityDict) #zipcodes by jurisdiction name -- many false positive -- last ditch if no city name
        def FillBlankZipcodes (df):
            zipcode_is_empty = (
                ( (df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == 0 ) )  
            )
            df.loc[zipcode_is_empty, "zip_cd"] = "99999"
            return df
        df = FillBlankZipcodes(df)
    return df


def InferZipcodeFromCityName(df, cityDict):

    if 'cty_nm' in df.columns: #if no city column then skip

        #check if column is in df
        if 'zip_cd' not in df.columns:
            df['zip_cd'] = '99999'
        df.zip_cd = df.zip_cd.astype(str)

        df['cty_nm'] = df['cty_nm'].astype(str).str.lower().str.strip()

        for city in cityDict:
            #upper_region = city.upper()
            #PATTERN_CITY = '|'.join(upper_region)

            zipcode_is_empty = (
                ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
            )

            test = cityDict[city][0]
            if fast_infer and test not in one_hundred_largest:
                continue
            pattern = r'\b{}\b'.format(re.escape(test.lower()))
            #more robust "SanFrancisco", "San-Francisco", "San Francisco" all match.
            #pattern = r'\b' + re.escape(city).replace(r'\.', r'\.?').replace(r'\ ', r'[\s\-]*') + r'\b'
            foundZipbyCity = (
                ((df['cty_nm'].str.contains(pattern, na=False, 
                    case=False, flags=re.IGNORECASE, regex=True)))  # flags=re.IGNORECASE
                #((df['cty_nm'].astype(str).str.contains(PATTERN_CITY, na=False, 
                #    case=False, flags=re.IGNORECASE)))  # flags=re.IGNORECASE
            )

            tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
            tempB = generate_generic_zipcode_for_city(tempA)
            df.loc[zipcode_is_empty * foundZipbyCity, "zip_cd"] = tempB
    
    return df


def InferZipcodeFromAddress(df, cityDict):

    if 'street_addr' in df.columns: 

        df['street_addr'] = df['street_addr'].astype(str).str.lower().str.strip()

        for city in cityDict:

            zipcode_is_empty = (
                ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
            )

            test = cityDict[city][0]
            if fast_infer and test not in one_hundred_largest:
                continue
            pattern = r'\b{}\b'.format(re.escape(test.lower()))
            foundCity = (
                ((df['street_addr'].str.contains(pattern, na=False, 
                    case=False, flags=re.IGNORECASE, regex=True)))
                #((df['cty_nm'].astype(str).str.contains(PATTERN_CITY, na=False, 
                #    case=False, flags=re.IGNORECASE)))
            )

            tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
            tempB = generate_generic_zipcode_for_city(tempA)
            df.loc[zipcode_is_empty * foundCity, "zip_cd"] = tempB
    
    return df


def InferZipcodeFromCompanyName(df, cityDict):
    # fill nan zip code by assumed zip by city name in trade name; ex. "Cupertino Elec."
    if 'cty_nm' not in df.columns:
        df['cty_nm'] = ''
    if 'trade_nm' not in df.columns:
        df['trade_nm'] = ""
    if 'legal_nm' not in df.columns:
        df['legal_nm'] = ""
    if 'zip_cd' not in df.columns:
        df['zip_cd'] = '99999'

    df['trade_nm'] = df['trade_nm'].astype(str).str.lower().str.strip()
    df['legal_nm'] = df['legal_nm'].astype(str).str.lower().str.strip()

    for city in cityDict:
        #PATTERN_CITY = '|'.join(cityDict[city][0])

        citynameisempty = ((pd.isna(df['cty_nm'])) | (df['cty_nm'] == '') )

        zipcode_is_empty = (
            ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
        )
        
        test = cityDict[city][0]
        if fast_infer and test not in one_hundred_largest:
                continue
        pattern = r'\b{}\b'.format(re.escape(test.lower()))

        foundZipbyCompany2 = (
            ((df['trade_nm'].str.contains(pattern, na=False, case=False, flags=re.IGNORECASE))) |
            ((df['legal_nm'].str.contains(
                test, na=False, case=False, flags=re.IGNORECASE, regex=True)))
            #((df['trade_nm'].astype(str).str.contains(PATTERN_CITY, na=False, case=False, flags=re.IGNORECASE))) |
            #((df['legal_nm'].astype(str).str.contains(
            #    PATTERN_CITY, na=False, case=False, flags=re.IGNORECASE)))
        )

        tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
        tempB = generate_generic_zipcode_for_city(tempA)
        df.loc[(zipcode_is_empty & citynameisempty) * foundZipbyCompany2,'zip_cd'] = tempB

    return df


def InferZipcodeFromJurisdictionName(df, cityDict):

    if 'cty_nm' not in df.columns:
        df['cty_nm'] = ''
    if 'juris_or_proj_nm' not in df.columns:
        df['juris_or_proj_nm'] = ""
    if 'Jurisdiction_region_or_General_Contractor' not in df.columns:
        df['Jurisdiction_region_or_General_Contractor'] = ""
    if 'zip_cd' not in df.columns:
        df['zip_cd'] = '99999'

    df.juris_or_proj_nm = df.juris_or_proj_nm.astype(str).str.lower().str.strip()
    df.Jurisdiction_region_or_General_Contractor = df.Jurisdiction_region_or_General_Contractor.astype(
        str).str.lower().str.strip()

    for city in cityDict:

        #PATTERN_CITY = '|'.join(cityDict[city][0])

        citynameisempty = ((pd.isna(df['cty_nm'])) | (df['cty_nm'] == '') )

        zipcode_is_empty = (
            ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
        )

        test = cityDict[city][0]
        if fast_infer and test not in one_hundred_largest:
                continue
        pattern = r'\b{}\b'.format(re.escape(test.lower()))

        foundZipbyCompany2 = (
            (df['juris_or_proj_nm'].str.contains(test, na=False, flags=re.IGNORECASE, regex=True)) |
            (df['Jurisdiction_region_or_General_Contractor'].str.contains(
                    pattern, na=False, flags=re.IGNORECASE, regex=True))
            #(df['juris_or_proj_nm'].str.contains(PATTERN_CITY, na=False, flags=re.IGNORECASE, regex=True)) |
            #(df['Jurisdiction_region_or_General_Contractor'].str.contains(
            #        PATTERN_CITY, na=False, flags=re.IGNORECASE, regex=True))
        )

        tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
        tempB = generate_generic_zipcode_for_city(tempA)
        df.loc[(citynameisempty & zipcode_is_empty) * foundZipbyCompany2, 'zip_cd'] = tempB

    return df


def Filter_for_Zipcode(df, TARGET_ZIPCODES = '00000', infer_zip = True, target_state = 'California'):
    if target_state == "California":
        df = df.loc[df['st_cd']== "CA"] #hacketty time at 1:30 pm and somewhere to go Jan 11, 2024 by F Peterson
    else:
        if TARGET_ZIPCODES[1] != '00000': # faster run for "All_Zipcode" condition
            TARGET_ZIPCODES_HERE = TARGET_ZIPCODES #make local copy
            if not infer_zip:
                for zipcode in TARGET_ZIPCODES_HERE:
                    if 'X' in zipcode: TARGET_ZIPCODES_HERE.remove(zipcode) #remove the dummy codes
            # Filter on region by zip code
            df = df.loc[df['zip_cd'].isin(TARGET_ZIPCODES_HERE)] #<-- DLSE bug here is not finding zipcodes
    return df


