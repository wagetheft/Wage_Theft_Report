
# 3/3/2019 by F. Peterson first file--170 lines
# 3/5/2020 by F. Peterson forking into SCC OLSE and SCC Wage Theft Coalition ver

import pandas as pd
import numpy as np
import os
import platform
import warnings
import time
import datetime

#moved down one directory for working dir diff in VM
if platform.system() == 'Windows' or platform.system() =='Darwin':
    #for desktop testing--"moved down one directory"
    from constants.zipcodes import stateDict
    from constants.zipcodes import countyDict
    from constants.zipcodes import cityDict
    from constants.industries import industriesDict
    from constants.prevailingWageTerms import prevailingWageTermsList
    from constants.prevailingWageTerms import prevailingWageLaborCodeList
    from constants.prevailingWageTerms import prevailingWagePoliticalList
    from constants.signatories import signatories
    from wagetheft_read_df import read_df
    from wagetheft_shape_df import shape_df
    from wagetheft_calc_utils import wages_owed
    from wagetheft_clean_value_utils import is_string_series
    from wagetheft_gen_report import compile_theft_report
    from wagetheft_inference_util import (
        infer_prevailing_wage_cases,
        InferAgencyFromCaseIDAndLabel,
    )

    from debug_utils import (
        debug_fileSetup_def,
        debug_fileClose_def,
        append_log,
    )
    from util_zipcode import (
        InferZipcode,
        generate_generic_zipcode_for_city,
    )
    from util_trade_pattern import lookupTrade
    from util_industry_pattern import Infer_Industry
    from util_signatory_pattern import (
        prevailing_wage_blacklist,
        filter_function_organization,
    )
else:
    from api.constants.zipcodes import stateDict
    from api.constants.zipcodes import countyDict
    from api.constants.zipcodes import cityDict
    from api.constants.industries import industriesDict
    from api.constants.prevailingWageTerms import prevailingWageTermsList
    from api.constants.prevailingWageTerms import prevailingWageLaborCodeList
    from api.constants.prevailingWageTerms import prevailingWagePoliticalList
    from api.constants.signatories import signatories
    from api.wagetheft_read_df import read_df
    from api.wagetheft_shape_df import shape_df
    from api.wagetheft_calc_utils import wages_owed
    from api.wagetheft_clean_value_utils import is_string_series
    from api.wagetheft_gen_report import compile_theft_report
    from api.wagetheft_inference_util import (
        infer_prevailing_wage_cases,
        InferAgencyFromCaseIDAndLabel,
    )

    from api.debug_utils import (
        debug_fileSetup_def,
        append_log,
    )
    from api.util_zipcode import (
        InferZipcode,
        generate_generic_zipcode_for_city,
    )
    from api.util_trade_pattern import lookupTrade
    from api.util_industry_pattern import Infer_Industry
    from api.util_signatory_pattern import (
        prevailing_wage_blacklist,
        filter_function_organization,
    )

warnings.filterwarnings("ignore", 'This pattern has match groups')

# Local/Debug main() ******************************************************************
def main():
    # settings****************************************************
    PARAM_1_TARGET_STATE = "" #"California"
    PARAM_1_TARGET_COUNTY = "" #"Santa_Clara_County"
    PARAM_1_TARGET_ZIPCODE = "San_Jose_Zipcode" #"San_Jose_Zipcode"
    PARAM_2_TARGET_INDUSTRY = "Construction" #'All NAICS' #"Janitorial" #for test use 'All NAICS'
    PARAM_3_TARGET_ORGANIZATION = "" #"Cobabe Brothers Incorporated|COBABE BROTHERS PLUMBING|COBABE BROTHERS|COBABE"
    
    PARAM_YEAR_START = "" #"2000/01/01" # default is 'today' - years=4 #or "2016/05/01"
    PARAM_YEAR_END = "" #default is 'today'
    
    OPEN_CASES = 0 # 1 for open cases only (or nearly paid off), 0 for all cases
    
    USE_ASSUMPTIONS = 1  # 1 to fill violation and ee gaps with assumed values
    INFER_NAICS = 1  # 1 to infer code by industry NAICS sector
    INFER_ZIP = 1  # 1 to infer zip code
    
    federal_data = 1 # 1 to include federal data
    state_judgements = 1
    state_cases = 1
    
    # report output block settings****************************************************
    TABLES = 1  # 1 for tables and 0 for just text description
    SUMMARY = 1  # 1 for summaries and 0 for none
    SUMMARY_SIG = 1 # 1 for summaries only of regions with significant wage theft (more than $10,000), 0 for all
    TOP_VIOLATORS = 1  # 1 for tables of top violators and 0 for none
    prevailing_wage_report = 0 # 1 to label prevailing wage violation records and list companies with prevailing wage violations, 0 not to
    signatories_report = 0 # 1 to include signatories (typically, this report is only for union compliance officers) 0 to exclude signatories

    # NOTES
    # geocode data https://www.geocod.io
    # https://webapps.dol.gov/wow/
    # https://www.goodjobsfirst.org/violation-tracker

    # API call***************************************************************************
    report_test = generateWageReport(
        PARAM_1_TARGET_STATE, PARAM_1_TARGET_COUNTY, PARAM_1_TARGET_ZIPCODE, 
        PARAM_2_TARGET_INDUSTRY, 
        PARAM_3_TARGET_ORGANIZATION,
        federal_data, state_judgements, state_cases, 
        INFER_ZIP, 
        prevailing_wage_report, signatories_report,
        OPEN_CASES, TABLES, SUMMARY, SUMMARY_SIG, 
        TOP_VIOLATORS, USE_ASSUMPTIONS, INFER_NAICS, 
        PARAM_YEAR_START, PARAM_YEAR_END
        )
    
    temp_stuff = report_test


# API function entry*******************************************************************
def generateWageReport(
        target_state, target_county, target_city, 
        target_industry, 
        target_organization,
        includeFedData, includeStateJudgements, includeStateCases, 
        infer_zip, 
        prevailing_wage_report, signatories_report,
        open_cases_only, include_tables, include_summaries, only_sig_summaries, 
        include_top_viol_tables, use_assumptions, infer_by_naics, 
        YEAR_START_TEXT, YEAR_END_TEXT
        ):

    # ********************************************************************************
    # APPLICATION SETTINGS************************************************************
    # ********************************************************************************

    warnings.filterwarnings("ignore", category=UserWarning)

    # Defaults start
    if target_industry == "": target_industry = "All NAICS"
    if (target_state == target_county == target_city == ""): target_state = "California"
    if (target_state != "") and ((target_county != "") or (target_city != "")): target_state = "" #temp fix
    if (target_county != "") and (target_city != ""): target_county = "" #temp fix
    
    if YEAR_START_TEXT == "": YEAR_START = pd.to_datetime('today') - pd.DateOffset(years=4)
    else: YEAR_START = pd.to_datetime(YEAR_START_TEXT)
    
    if YEAR_END_TEXT == "": YEAR_END = pd.to_datetime('today')
    else: YEAR_END = pd.to_datetime(YEAR_END_TEXT)
    # Defaults end
    
    # Settings External - start
    option_dict = {
        'TARGET_ZIPCODES':search_Dict_tree(target_state, target_county, target_city, stateDict, countyDict, cityDict), 
        'TARGET_INDUSTRY':industriesDict[target_industry],
        'TARGET_ORGANIZATIONS':[['organizations'], [target_organization]],

        'target_jurisdition':"", # will be set later'
        
        'SIGNATORY_INDUSTRY':signatories,

        'infer_zip':infer_zip, 
        'infer_by_naics':infer_by_naics, 
        'use_assumptions':use_assumptions,
        
        #DATA
        'YEAR_START':YEAR_START, 
        'YEAR_END':YEAR_END,
        'target_state':target_state,
        'open_cases_only':open_cases_only, 
        
        #REPORTS
        'signatories_report':signatories_report, 
        'Nonsignatory_Ratio_Block':False,
        'include_methods':True,
    }
    # region definition*******************************************************************
    # <--revise to include other jurisdiction types such as County
    if option_dict['TARGET_ZIPCODES'][0].find("County"): JURISDICTON_NAME = " "
    else: JURISDICTON_NAME = "City of "
    option_dict['target_jurisdition'] = JURISDICTON_NAME + option_dict['TARGET_ZIPCODES'][0].replace("_"," ")
    # endregion definition****************************************************************

    prep_dict = {
        # Settings External - cont.
        'includeFedData':includeFedData,
        'includeStateJudgements':includeStateJudgements,
        'includeStateCases':includeStateCases,
        # Settings External - end

        #EXTERN LIBRARIES
        'prevailing_wage_terms':prevailingWageTermsList, #from prevailingWageTerms.py,
        'prevailing_wage_labor_code':prevailingWageLaborCodeList, #from prevailingWageTerms.py,
        'prevailing_wage_politicals':prevailingWagePoliticalList, #from prevailingWageTerms.py,
        'cityDict':cityDict,
        #EXTERN  LIBRARIES - end

        # Settings Internal - start
        'url_backup_file':'url_backup',
        'url_backup_path':'url_backup/',
        'url_abs_path': "", # will be set later

        'short_run':True, # True for short run, False for full run

        'DF_OG': pd.DataFrame(),
    }
    script_dir0 = os.path.dirname(os.path.dirname(__file__))
    prep_dict['url_abs_path'] = os.path.join(script_dir0, prep_dict['url_backup_path'])

    #unlikely search criteria that triggers a longer full search
    if target_state != "" and target_state.lower() != ("California").lower():
        prep_dict['short_run'] = False
    if target_industry.lower() != ("Construction").lower():
        prep_dict['short_run'] = False
    if (
        YEAR_START != (pd.to_datetime('today') - pd.DateOffset(years=4)) 
        and YEAR_START < pd.to_datetime('2010-05-01')
        ):
        prep_dict['short_run'] = False
    
    # SET DEBUG SETTINGS AND LOG FILE NAME/PATH *********************************************************
    debug = {
        'file_name':option_dict['TARGET_ZIPCODES'][0] + "_" + target_industry,

        'debug_log_path':"", # will be set later
        'abs_path':"", # will be set later -- same as debug_log_path', legacy, refactor out
        'bug_log': "", # will be set later
        'bug_log_csv':"", # will be set later
        'bugFile':"", # will be set later

        'log_number':1, #typically 1
        'LOGBUG':True, #typically False

        'TEST_CASES':1000000000, # read all records -- infinit large number

        'RunFast':False, # True skip slow formating; False run normal
        'FLAG_DUPLICATE':0, # 1 FLAG_DUPLICATE duplicate, #0 drop duplicates --typically 0
        'New_Data_On_Run_Test':False, #typically False

        'TEST_':0, #typically 0
        # 0 for normal run w/ all records
        # 1 for custom test dataset (url0 = "https://stanford.edu/~granite/DLSE_no_returns_Linux_TEST.csv" <-- open and edit this file with test data)
        # 2 for small dataset (first 100 of each file)
    }
    if debug['TEST_'] != 0 and debug['TEST_'] != 1:
        debug['TEST_CASES'] = 1000 #2 #short set--use first 1000 for debugging

    # LOGS
    current_week = get_current_week_string()
    rel_path = 'report_output_/' + current_week + '/'
    script_dir = os.path.dirname(os.path.dirname(__file__))
    debug['debug_log_path'] = os.path.join(script_dir, rel_path)
    debug['abs_path'] = debug['debug_log_path']
    os.chdir(script_dir) #Change the current working directory per https://stackoverflow.com/questions/12201928/open-gives-filenotfounderror-ioerror-errno-2-no-such-file-or-directory
    if not os.path.exists(script_dir): os.makedirs(script_dir)
    if not os.path.exists(debug['debug_log_path']): os.makedirs(debug['debug_log_path'])
    debug['bug_log'] = os.path.join(debug['debug_log_path'], ('log_'+'bug_').replace(' ', '_') + '.txt')
    debug['bug_log_csv'] = os.path.join(debug['debug_log_path'], ('log_'+'bug_').replace(' ', '_') + '.csv')
    debug_fileSetup_def(debug['bugFile'], debug['LOGBUG'])
    
    f_dict = {
        'temp_file_name':name_gen(debug['debug_log_path'], debug['file_name'], '_theft_summary_', target_organization, '.html'),
        'temp_file_name_HTML_to_PDF':name_gen(debug['debug_log_path'], debug['file_name'], '_temp_theft_summary_', target_organization, '.html'),
        'temp_file_name_PDF':name_gen(debug['debug_log_path'], debug['file_name'], '_theft_summary_', target_organization, '.pdf'),
        'temp_file_name_csv':name_gen(debug['debug_log_path'], debug['file_name'], '_theft_summary_', target_organization, '.csv'),
        'sig_file_name_csv':name_gen(debug['debug_log_path'], debug['file_name'], '_signatory_wage_theft_', target_organization, '.csv'),
        'prev_file_name_csv':name_gen(debug['debug_log_path'], debug['file_name'], '_prevailing_wage_theft_', target_organization, '.csv'),
    }


    # print definition****************************************************************
    print_dict = {
        'include_tables':include_tables,
        'include_summaries':include_summaries,
        'only_sig_summaries':only_sig_summaries,
        'include_top_viol_tables':include_top_viol_tables,

        'prevailing_wage_report':prevailing_wage_report,
        'Nonsignatory_Ratio_Block':option_dict['Nonsignatory_Ratio_Block'],
        'include_methods':option_dict['include_methods'],

        'includeFedData':prep_dict['includeFedData'],
        'includeStateCases':prep_dict['includeStateCases'],
        'includeStateJudgements':prep_dict['includeStateJudgements'],

        'target_organization':target_organization,
        'target_jurisdition':option_dict['target_jurisdition'],
        'TARGET_INDUSTRY':option_dict['TARGET_INDUSTRY'],
        'YEAR_START':option_dict['YEAR_START'],
        'YEAR_END':option_dict['YEAR_END'],

        'open_cases_only':option_dict['open_cases_only'],

        'sig_file_name_csv':f_dict['sig_file_name_csv'],
        'prev_file_name_csv':f_dict['prev_file_name_csv'],
        'temp_file_name_HTML_to_PDF':f_dict['temp_file_name_HTML_to_PDF'],
        
        'DF_OG':prep_dict['DF_OG'],

        'TEST_':debug['TEST_'],
    }

    # ********************************************************************************
    # START APPLICATION***************************************************************
    # ********************************************************************************

    # Read data***********************************************************************
    time_0 = time_1 = time.time()

    # df.to_csv(debug['bug_log_csv']) #debug outfile -- use to debug

    out_target, prep_dict['DF_OG'] = read_df(
        industriesDict, 
        prep_dict, 
        debug,
        )
    
    #TARGET LIST
    out_target = shape_df(
        out_target, 
        option_dict, 
        debug['FLAG_DUPLICATE'],
        debug['bug_log_csv'],
        )

    out_target, out_target_organization = extract_values_for_report(
        out_target, 
        option_dict['TARGET_ORGANIZATIONS'], 
        option_dict['signatories_report'],
        f_dict['temp_file_name_csv'])
    
    out_prevailing_target, out_signatory_target = prevailing_wage_blacklist(out_target)

    target_dict = {
        'case_disposition_series':out_target_organization['Case Status'].copy(),
        'out_prevailing_target':out_prevailing_target,
        'out_signatory_target':out_signatory_target,
    }

    #SUM COUNTS
    sum_dict = {
        'total_ee_violtd': out_target_organization['ee_violtd_cnt'].sum(),
        'total_bw_atp': out_target_organization['bw_amt'].sum(),
        'total_case_violtn': out_target_organization['violtn_cnt'].sum(),
    }

    #PRINT
    compile_theft_report(
        out_target,
        out_target_organization,
        target_dict,
        print_dict,
        sum_dict,
        debug,
        f_dict['temp_file_name'],
        option_dict['signatories_report'],
    )

    #CLOSE LOG FILE***************************************************************
    time_2 = time.time()
    append_log(debug['bug_log'], f"Time to finish report " + "%.5f" % (time_2 - time_0) + "\n", debug['LOGBUG'])
    append_log(debug['bug_log'], f_dict['temp_file_name'], debug['LOGBUG'])
    debug_fileClose_def(debug['bugFile'], debug['LOGBUG']) #close the log file
    # updated 8/10/2022 by f. peterson to .format() per https://stackoverflow.com/questions/18053500/typeerror-not-all-arguments-converted-during-string-formatting-python
    
    #RETURN REPORT
    #return f_dict['temp_file_name_PDF']  # the temp json returned from API -- PDF crashes
    return f_dict['temp_file_name']  # the temp json returned from API


#MODULE FUNCTIONS***************************************************************
def search_Dict_tree(target_state, target_county, target_city, stateDict, countyDict, cityDict):

    #check to verify that only lowest level has data
    #initialize lists
    STATE_LIST = []
    COUNTY_LIST = []
    CITY_LIST = []
    ZIPCODE_LIST = []

    target_region = target_state #temp hack to make a regional list
    if target_region == "": STATE_LIST = []
    else: 
        STATE_LIST = [target_region, target_region][1:] #dummy of nationDict[target_region]
        ZIPCODE_LIST = [[target_region, target_region][0]]
        target_state = ""
        target_county = ""
        target_city = ""
    if target_state == "": COUNTY_LIST = []
    else: 
        COUNTY_LIST = stateDict[target_state][1:]
        ZIPCODE_LIST = [stateDict[target_state][0]]
        target_county = ""
        target_city = ""
    if target_county == "": CITY_LIST = []
    else: 
        CITY_LIST = countyDict[target_county][1:]
        ZIPCODE_LIST = [countyDict[target_county][0]]
        target_city = ""
    if target_city == "": "DO_NOTHING" #base case passes through last for loop
    else: 
        CITY_LIST = [target_city]
        ZIPCODE_LIST = [cityDict[target_city]][0]
        #target_precinct = ""
        
    for states in STATE_LIST if STATE_LIST else range(1):
        if STATE_LIST: 
            COUNTY_LIST.extend(stateDict[states][1:]) # [1:] to skip region name in cell one
    
    for counties in COUNTY_LIST if COUNTY_LIST else range(1):
        if COUNTY_LIST: 
            CITY_LIST.extend(countyDict[counties][1:]) # [1:] to skip region name in cell one
        
    for city in CITY_LIST if CITY_LIST else range(1):
        if CITY_LIST: 
            ZIPCODE_LIST.extend(cityDict[city][1:]) #[1:] to skip region name in cell one
            
            tempA = cityDict[city][len(cityDict[city]) - 1] #take the last zipcode for region and modify w/ ending 'xx'

            ZIPCODE_LIST.append(generate_generic_zipcode_for_city(tempA) )

                ##add precinct level loop which replaces zipcode level and adds mass confusion

    ZIPCODE_LIST = list( dict.fromkeys(ZIPCODE_LIST) ) #remove duplicates created in default region generation

    return ZIPCODE_LIST


def inference_function(df, cityDict, TARGET_INDUSTRY, 
        prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals, 
        bug_log, LOGBUG, log_number):

    function_name = "inference_function"

    # zip codes infer *********************************
    time_1 = time.time()
    #if infer_zip == 1: 
    InferZipcode(df, cityDict)
    time_2 = time.time()
    log_number = "InferZipcode"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    time_1 = time.time()
    df = Infer_Industry(df, TARGET_INDUSTRY)
    time_2 = time.time()
    log_number = "Infer_Industry"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)
    # unused df = Filter_for_Target_Industry(df,TARGET_INDUSTRY) ##debug 12/23/2020 <-- run here for faster time but without global summary
    
    time_1 = time.time()
    df = InferAgencyFromCaseIDAndLabel(df, 'juris_or_proj_nm')
    time_2 = time.time()
    log_number = "InferAgency"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    # PREVAILING WAGE
    time_1 = time.time()
    df = infer_prevailing_wage_cases(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals)
    if is_string_series(df['Prevailing']):
        df['Prevailing'] = pd.to_numeric(df['Prevailing'], errors='coerce')
    time_2 = time.time()
    log_number = "infer_prevailing_wage_cases"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    time_1 = time.time()
    df = wages_owed(df)
    time_2 = time.time()
    log_number = "calc wages_owed"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    #coulf be buggy 1/18/2024 so removed
    #time_1 = time.time()
    #df = fill_case_status_for_missing_enddate(df)
    #time_2 = time.time()
    #log_number+=1
    #append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    return df


def extract_values_for_report(
            out_target, 
            TARGET_ORGANIZATIONS, 
            signatories_report,
            temp_file_name_csv
            ):
    
    out_target_organization = filter_function_organization(out_target, TARGET_ORGANIZATIONS)

    # filter
    if signatories_report == 0:
        if 'Signatory' not in out_target.columns:
            out_target['Signatory'] = 0
        if 'legal_nm' not in out_target.columns:
            out_target['legal_nm'] = ""
        if 'trade_nm' not in out_target.columns:
            out_target['trade_nm'] = ""

        # unused out_target = out_target.loc[out_target['Signatory']!=1] #filter
        out_target['legal_nm'] = np.where(
            out_target['Signatory'] == 1, "masked", out_target['legal_nm'])
        out_target['trade_nm'] = np.where(
            out_target['Signatory'] == 1, "masked", out_target['trade_nm'])
        out_target['street_addr'] = np.where(
            out_target['Signatory'] == 1, "masked", out_target['street_addr'])
        out_target['case_id_1'] = np.where(
            out_target['Signatory'] == 1, "masked", out_target['case_id_1'])
        if 'DIR_Case_Name' in out_target.columns:
            out_target['DIR_Case_Name'] = np.where(
                out_target['Signatory'] == 1, "masked", out_target['DIR_Case_Name'])
            
        out_target_organization['legal_nm'] = np.where(
            out_target_organization['Signatory'] == 1, "masked", out_target_organization['legal_nm'])
        out_target_organization['trade_nm'] = np.where(
            out_target_organization['Signatory'] == 1, "masked", out_target_organization['trade_nm'])
        out_target_organization['street_addr'] = np.where(
            out_target_organization['Signatory'] == 1, "masked", out_target_organization['street_addr'])
        out_target_organization['case_id_1'] = np.where(
            out_target_organization['Signatory'] == 1, "masked", out_target_organization['case_id_1'])
        if 'DIR_Case_Name' in out_target_organization.columns:
            out_target_organization['DIR_Case_Name'] = np.where(
                out_target_organization['Signatory'] == 1, "masked", out_target_organization['DIR_Case_Name'])
    
    # create csv output file**********************************
    
    # added to prevent bug that outputs 2x
    out_target_organization = out_target_organization.drop_duplicates(keep='last')
    out_target = out_target.drop_duplicates(keep='last')

    out_target_organization.to_csv(temp_file_name_csv, encoding="utf-8-sig")

    return out_target, out_target_organization


#HELPER FUNCTIONS*************************************************
def name_gen(path, name, report_type, organization, type):
        return os.path.join(
            path, 
            (name + report_type + organization).replace(' ', '_') + type
    )


def get_current_week_string():
    current_date = datetime.datetime.now()
    year, week_num, _ = current_date.isocalendar()  # Get year and week number using isocalendar
    return f"week{week_num}"

if __name__ == '__main__':

    main()