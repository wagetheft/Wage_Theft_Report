
import pandas as pd
import numpy as np
import time

from api.debug_utils import append_log

from api.wagetheft_clean_value_utils import is_string_series

from api.util_zipcode import InferZipcode
from api.wagetheft_calc_utils import wages_owed
from api.util_industry_pattern import Infer_Industry
from api.util_signatory_pattern import infer_prevailing_wage_cases
    


def inference_function(df, cityDict, TARGET_INDUSTRY, 
        prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals, 
        bug_log, LOGBUG, log_number):

    function_name = "inference_function"

    # zip codes infer *********************************
    time_1 = time.time()
    #if infer_zip == 1: 
    InferZipcode(df, cityDict) #long runtime
    time_2 = time.time()
    log_number = "InferZipcode"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    time_1 = time.time()
    df = Infer_Industry(df, TARGET_INDUSTRY)
    time_2 = time.time()
    log_number = "Infer_Industry"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")
    # unused df = Filter_for_Target_Industry(df,TARGET_INDUSTRY) ##debug 12/23/2020 <-- run here for faster time but without global summary
    
    time_1 = time.time()
    df = InferAgencyFromCaseIDAndLabel(df, 'juris_or_proj_nm')
    time_2 = time.time()
    log_number = "InferAgency"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    # PREVAILING WAGE
    time_1 = time.time()
    df = infer_prevailing_wage_cases(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals)
    if is_string_series(df['Prevailing']):
        df['Prevailing'] = pd.to_numeric(df['Prevailing'], errors='coerce')
    time_2 = time.time()
    log_number = "infer_prevailing_wage_cases"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    time_1 = time.time()
    df = wages_owed(df)
    time_2 = time.time()
    log_number = "calc wages_owed"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    #coulf be buggy 1/18/2024 so removed
    #time_1 = time.time()
    #df = fill_case_status_for_missing_enddate(df)
    #time_2 = time.time()
    #log_number+=1
    #append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    return df


def InferAgencyFromCaseIDAndLabel(df, LABEL_COLUMN):

    if LABEL_COLUMN in df.columns:
        # find case IDs with a hyphen
        foundAgencybyCaseID_1 = pd.isna(df[LABEL_COLUMN])
        # df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna(foundAgencybyCaseID_1.replace( (True,False), (df['case_id_1'].astype(str).apply(lambda st: st[:st.find("-")]), df[LABEL_COLUMN]) ) ) #https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas
        # https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas
        '''
        df.loc[foundAgencybyCaseID_1, LABEL_COLUMN] = df['case_id_1'].astype(
            str).apply(lambda st: st[:st.find("-")])
        '''
        df.loc[foundAgencybyCaseID_1, LABEL_COLUMN] = df['case_id_1'].astype(str).str.split('-').str[0] #the ai agent told me to revise 6/27/2025

        # cind case ID when no hyphen
        foundAgencybyCaseID_2 = pd.isna(df[LABEL_COLUMN])
        #df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna(foundAgencybyCaseID_2.replace( (True,False), (df['case_id_1'].astype(str).str[:3], df[LABEL_COLUMN]) ) )
        df.loc[foundAgencybyCaseID_2, LABEL_COLUMN] = df['case_id_1'].astype(
            str).str[:3]

        # hardcode for DLSE nomemclature with a note * for assumed
        DLSE_terms = ['01', '04', '05', '06', '07', '08', '09', '10', '11',
                      '12', '13', '14', '15', '16', '17', '18', '23', '32', '35', 'WC']
        pattern_DLSE = '|'.join(DLSE_terms)
        found_DLSE = (df[LABEL_COLUMN].str.contains(pattern_DLSE))
        #df[LABEL_COLUMN] = found_DLSE.replace( (True,False), ("DLSE", df[LABEL_COLUMN] ) )
        df.loc[found_DLSE, LABEL_COLUMN] = "DLSE"

    return df


