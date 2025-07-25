
import pandas as pd


import platform
if platform.system() == 'Windows' or platform.system() =='Darwin':
    from util_industry_pattern import Filter_for_Target_Industry
    from util_zipcode import Filter_for_Zipcode

else:
    from api.util_industry_pattern import Filter_for_Target_Industry
    from api.util_zipcode import Filter_for_Zipcode


def filter_function(df, TARGET_ZIPCODES, TARGET_INDUSTRY,
    infer_zip, infer_by_naics, YEAR_START, YEAR_END, target_state):

    #check_in0 = str(len(df))
    df = FilterForDate(df, YEAR_START, YEAR_END)
    #check_in1 = str(len(df))
    df = Filter_for_Zipcode(df, TARGET_ZIPCODES, infer_zip, target_state)
    #check_in2 = str(len(df))
    df = Filter_for_Target_Industry(df, TARGET_INDUSTRY, infer_by_naics) #<--- BUGGY HERE 1/12/2023 w/ 2x records -- hacky fix w/ dup removal
    #check_in3 = str(len(df))

    return df


def FilterForDate(df, YEAR_START, YEAR_END):
    
    df = df[
        ((pd.to_datetime(df['findings_start_date'], errors = 'coerce', dayfirst=True) >= YEAR_START) & 
        (pd.to_datetime(df['findings_start_date'], errors = 'coerce', dayfirst=True) <= YEAR_END)) |
        
        (pd.isnull(df['findings_start_date']) & (pd.to_datetime(df['findings_end_date'], errors = 'coerce', dayfirst=True) >= YEAR_START) & 
        (pd.to_datetime(df['findings_end_date'], errors = 'coerce', dayfirst=True) <= YEAR_END)) | 
        
        (pd.isnull(df['findings_start_date']) & pd.isnull(df['findings_end_date']))
    ]

    return df












