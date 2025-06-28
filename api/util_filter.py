
import pandas as pd
import time

from debug_utils import append_log

from util_industry_pattern import Filter_for_Target_Industry
from util_zipcode import Filter_for_Zipcode


def filter_function(df, TARGET_ZIPCODES, TARGET_INDUSTRY, open_cases_only,
    infer_zip, infer_by_naics, TARGET_ORGANIZATIONS, YEAR_START, YEAR_END, target_state, bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv):

    function_name = "filter_function"

    df = FilterForDate(df, YEAR_START, YEAR_END)

    # zip codes filter *********************************
    time_1 = time.time()
    df = Filter_for_Zipcode(df, TARGET_ZIPCODES, infer_zip, target_state)
    time_2 = time.time()
    log_number+=1
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    #df.to_csv(os.path.join(abs_path, (file_name+'test_2_out_file_report').replace(' ', '_') + '.csv'))

    time_1 = time.time()
    df = Filter_for_Target_Industry(df, TARGET_INDUSTRY, infer_by_naics) #<--- BUGGY HERE 1/12/2023 w/ 2x records -- hacky fix w/ dup removal
    
    #df.to_csv(os.path.join(abs_path, (file_name+'test_3_out_file_report').replace(' ', '_') + '.csv'))

    #moved to seperate function
    #if (TARGET_ORGANIZATIONS[1] != ""): 
    #    df = Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS)  #<--- BUGGY HERE 1/12/2023 2x records -- hacky fix w/ dup removal
    #time_2 = time.time()
    #log_number = "Filter_for_Target_Industry and Organization"
    #append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    #df.to_csv(os.path.join(abs_path, (file_name+'test_4_out_file_report').replace(' ', '_') + '.csv'))

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










