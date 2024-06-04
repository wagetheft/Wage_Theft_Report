# wagetheft_report_2020_v2
# Python 3.9.7
# Last updated
# 3/3/2019 by F. Peterson first file--170 lines
# 5/1/2019 by F. Peterson numerous updates
# 3/5/2020 by F. Peterson locked code before forking into OLSE and SCC WTC versions (donation of ~1,500 lines--@$73,500--from WTC to OLSE)
# 10/26/2020 by F. Peterson (finally debugged 4am on 10/30/2020)
# 11/1/2020 by F. Peterson still working on this a week later
# 11/9/2020 by F. Peterson fixed infer zip code and added proximity match to allow filtering unpaid theft
# 11/15/2020 by F. Peterson removed duplicates
# 11/16/2020 by F. Peterson adjusted care home keywords
# 11/21/2020 by F. Peterson add name of highest theft business to city summary
# 11/22/2020 by F. Peterson add hospital/care signatory employer list
# 12/16/2020 by F. Peterson add revised industry keyword exclusion function, and revise NAICS library
# 12/24/2020 by F. Peterson revised industry NAICS keyword library w/ regexp and update industry label function to include state NAICS labels
# 1/8/2021 by F. Peterson revised city and zipcode summary per Michael Tayag edits to text (2,711 lines)
# 1/12/2021 by F. Peterson added signatory by industry label (does not work?) --> works 1/13/2021
# 1/13/2021 by F. Peterson added industry summary to regional summary (2,867 lines)
# 1/14/2021 by F. Peterson added a summary of ratio of union to non-union company wage theft (2,880 lines-->@$140k in work with $5 per line and ~10 lines per 8hr day)
# 1/22/2021 by F. Peterson careful output check with 100 x 1,000 dataset
# 1/24/2021 by F. Peterson debug details in signatory library access to industry labels (2,969 lines)
# 10/25/2021 by F. Peterson added summary block 1/0 triggers, 4 LoC and added formatting to prev wage table
# 2/3/2022 by F. Peterson added organization filter -- add State filter, added new WHD file and debugged, fix date bug, industry label adjusted (3,195 lines)
# 2/14/2022 by F. Peterson add WHD prevailing wage inference
# 3/3/2022 by F. Peterson added exclusion terms to construction industry
# 3/4/2022 by F. Peterson fixed a bug with trade/industry header mixup and a bug with double output
# 3/7/2022 by F. Peterson added county of San Diego
# 6/28/2022 by F. Peterson started adding API (does not run)
# 6/29/2022 by I. Kolli added parameters to API code
# 7/2/2022 by F. Peterson added several more parameters to API code and create output folder if missing: tested and works + added url pulls for WHD and DOL violations
# 7/14/2022 by F. Peterson update DOL WHD url to updated url 7/13/2022
# 8/10/2022 by F. Peterson resolve several bugs-- incomplete but runs without crash
# 8/13/2022 see Git log for update record
# 10/26/2022 by F. Peterson -- debug hang on State and Fed reports -- caused by zipcode bug (old school but I like the log here)

# Note: add an edit distance comparison
# to fix replace() https://stackoverflow.com/questions/64843109/compiler-issue-assertionerror-on-replace-given-t-or-f-condition-with-string/64856554#64856554

# DUPLICATE CHECKS:
# no duplicate by case numbers
# no duplicate by legal or trade names
# no duplicates by bw_amt
# no duplicates by ee_pmt_recv (duplicated zero false positives but nbd)

import re
import pandas as pd
import numpy as np
import math
from datetime import datetime
import os
import platform
from string import ascii_letters
import warnings
import time
import requests
import io


#moved down one directory
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
else:
    from api.constants.zipcodes import stateDict
    from api.constants.zipcodes import countyDict
    from api.constants.zipcodes import cityDict
    from api.constants.industries import industriesDict
    from api.constants.prevailingWageTerms import prevailingWageTermsList
    from api.constants.prevailingWageTerms import prevailingWageLaborCodeList
    from api.constants.prevailingWageTerms import prevailingWagePoliticalList
    from api.constants.signatories import signatories

warnings.filterwarnings("ignore", 'This pattern has match groups')


def main():
    # settings****************************************************
    PARAM_1_TARGET_STATE = "" #"California"
    PARAM_1_TARGET_COUNTY = "" #"Santa_Clara_County"
    PARAM_1_TARGET_ZIPCODE = "" #"San_Jose_Zipcode"
    PARAM_2_TARGET_INDUSTRY = "" #"Janitorial" #"Construction" #for test use 'All NAICS'
    PARAM_3_TARGET_ORGANIZATION = "Made Up Name Test" #"Cobabe Brothers Incorporated|COBABE BROTHERS PLUMBING|COBABE BROTHERS|COBABE"
    
    PARAM_YEAR_START = "2000/01/01" # default is 'today' - years=4 #or "2016/05/01"
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
    SUMMARY_SIG = 0 # 1 for summaries only of regions with significant wage theft (more than $10,000), 0 for all
    TOP_VIOLATORS = 1  # 1 for tables of top violators and 0 for none
    prevailing_wage_report = 0 # 1 to label prevailing wage violation records and list companies with prevailing wage violations, 0 not to
    signatories_report = 0 # 1 to include signatories (typically, this report is only for union compliance officers) 0 to exclude signatories

    #!!!manually add to report***********************************************************
    # (1)generate report from http://wagetheftincitieslikemine.site/
    # (2)geocode data https://www.geocod.io
    # (3)generate Tableau bubble plot with location data https://public.tableau.com/profile/forest.peterson#!

    # checks
    # https://webapps.dol.gov/wow/
    # https://www.goodjobsfirst.org/violation-tracker

    # API call***************************************************************************
    generateWageReport(PARAM_1_TARGET_STATE, PARAM_1_TARGET_COUNTY, PARAM_1_TARGET_ZIPCODE, PARAM_2_TARGET_INDUSTRY, \
                       PARAM_3_TARGET_ORGANIZATION,
                       federal_data, state_judgements, state_cases, INFER_ZIP, prevailing_wage_report, signatories_report,
                       OPEN_CASES, TABLES, SUMMARY, SUMMARY_SIG, 
                       TOP_VIOLATORS, USE_ASSUMPTIONS, INFER_NAICS,PARAM_YEAR_START, PARAM_YEAR_END)


# Functions*************************************************


def generateWageReport(target_state, target_county, target_city, target_industry, 
                        target_organization,
                        includeFedData, includeStateJudgements, includeStateCases, infer_zip, prevailing_wage_report, signatories_report,
                        open_cases_only, include_tables, include_summaries, only_sig_summaries, 
                        include_top_viol_tables, use_assumptions, infer_by_naics, YEAR_START_TEXT, YEAR_END_TEXT):

    warnings.filterwarnings("ignore", category=UserWarning)
    start_time = time.time()

    #temp fix
    #include_top_viol_tables = 0 #5/29/2024 temp fix bug

    # Defaults start
    use_assumptions = 1
    include_methods = True
    if target_industry == "": target_industry = "All NAICS"
    if (target_state == "") and (target_county == "") and (target_city == ""): target_state = "California"
    
    if YEAR_START_TEXT == "":
        YEAR_START = pd.to_datetime('today') - pd.DateOffset(years=4)
    else:
        YEAR_START = pd.to_datetime(YEAR_START_TEXT)
    
    if YEAR_END_TEXT == "":
        YEAR_END = pd.to_datetime('today')
    else: 
        YEAR_END = pd.to_datetime(YEAR_END_TEXT)
    # Defaults end
    
    # Settings External - start
    TARGET_ZIPCODES = search_Dict_tree(target_state, target_county, target_city, stateDict, countyDict, cityDict)
    TARGET_INDUSTRY = industriesDict[target_industry]
    TARGET_ORGANIZATIONS = [['organizations'], [target_organization]]  # use uppercase
    # Settings External - end

    # Settings Internal - start
    TEST_ = 0 # see Read_Violation_Data() -- 
    # 0 for normal run w/ all records
    # 1 for custom test dataset (url0 = "https://stanford.edu/~granite/DLSE_no_returns_Linux_TEST.csv" <-- open and edit this file with test data)
    # 2 for small dataset (first 100 of each file)
    RunFast = False  # True skip slow formating; False run normal
    New_Data_On_Run_Test = False #to generate a new labeled dataset on run
    LOGBUG = False #True to log, False to not
    FLAG_DUPLICATE = 0  # 1 FLAG_DUPLICATE duplicate, #0 drop duplicates
    # Settings Internal - end

    # Settings Internal that will Move to UI Options - start
    Nonsignatory_Ratio_Block = False #<-- always true
    # Settings Internal that will Move to UI Options - end

    #LIBRARIES - start
    prevailing_wage_terms = prevailingWageTermsList #from prevailingWageTerms.py
    prevailing_wage_labor_code = prevailingWageLaborCodeList #from prevailingWageTerms.py
    prevailing_wage_politicals = prevailingWagePoliticalList #from prevailingWageTerms.py

    SIGNATORY_INDUSTRY = signatories #from signatories.py
    #LIBRARIES - end
    
    # TEST_ PARAMETERS
    if TEST_ == 0 or TEST_ == 1:
        TEST_CASES = 1000000000  # read all records
    else:  # TEST_ == 2 #short set--use first 1000 for debugging
        TEST_CASES = 100

    # SET OUTPUT FILE NAME AND PATH: ALL FILE NAMES AND PATHS DEFINED HERE **********************************
    # report main output file -- change to PDF option
    # relative path
    rel_path = 'report_output_/'
    # <-- dir the script is in (import os) plus up one
    script_dir = os.path.dirname(os.path.dirname(__file__))
    abs_path = os.path.join(script_dir, rel_path)
    os.chdir(script_dir) #Change the current working directory per https://stackoverflow.com/questions/12201928/open-gives-filenotfounderror-ioerror-errno-2-no-such-file-or-directory
    
    if not os.path.exists(script_dir):  # create folder if necessary
        os.makedirs(script_dir)

    if not os.path.exists(abs_path):  # create folder if necessary
        os.makedirs(abs_path)

    file_name = TARGET_ZIPCODES[0] + "_" + target_industry

    file_type = '.html'
    out_file_report = '_theft_summary_'
    temp_file_name = os.path.join(abs_path, (file_name+out_file_report+target_organization).replace(
        ' ', '_') + file_type)  # <-- absolute dir and file name

    file_type = '.csv'
    temp_file_name_csv = os.path.join(abs_path, (file_name+out_file_report).replace(
        ' ', '_') + file_type)  # <-- absolute dir and file name

    out_file_report = '_signatory_wage_theft_'
    sig_file_name_csv = os.path.join(abs_path, (file_name+out_file_report).replace(
        ' ', '_') + file_type)  # <-- absolute dir and file name

    out_file_report = '_prevailing_wage_theft_'
    prev_file_name_csv = os.path.join(abs_path, (file_name+out_file_report).replace(
        ' ', '_') + file_type)  # <-- absolute dir and file name

    file_name = 'log_'
    out_file_report = '_bug_'
    file_type = '.txt'
    # <-- absolute dir and file name
    bug_log = os.path.join(
        abs_path, (file_name+out_file_report).replace(' ', '_') + file_type)
    file_type = '.csv'
    # <-- absolute dir and file name
    bug_log_csv = os.path.join(
        abs_path, (file_name+out_file_report).replace(' ', '_') + file_type)

    bugFile = ""
    log_number = 1
    if LOGBUG: 
        bugFile = open(bug_log, 'w')
        debug_fileSetup_def(bugFile)
        bugFile.close()

    # region definition*******************************************************************
    time_1 = time.time()
    # <--revise to include other jurisdiction types such as County
    JURISDICTON_NAME = " "
    if TARGET_ZIPCODES[0].find("County"): "DO_NOTHING"
    else: JURISDICTON_NAME = "City of "
    
    # target jurisdiction: Report Title block and file name "<h1>DRAFT REPORT: Wage Theft in the jurisdiction of... "
    target_jurisdition = JURISDICTON_NAME + TARGET_ZIPCODES[0].replace("_"," ")
    target_industry = TARGET_INDUSTRY[0][0]
    time_2 = time.time()
    log_number = 2
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n") 
    # endregion definition*******************************************************************

    # Read data***************************************************************************
    time_0 = time.time()
    time_1 = time.time()

    DF_OG = pd.DataFrame()

    url_backup_file = 'url_backup'
    url_backup_path = url_backup_file + '/'
    script_dir0 = os.path.dirname(os.path.dirname(__file__))
    abs_path0 = os.path.join(script_dir0, url_backup_path)

    includeTestData = 0 #this is always 0
    if (TEST_ == 1): 
        includeTestData = 1
        includeFedData = 0
        includeStateJudgements = 0
        includeStateCases = 0
        #includeLocalData = False -- unused
        #includeOfficeData = False -- unused
    
    url_list = [
        ["temp", includeTestData,'TEST'], 
        ["temp", includeFedData,'DOL_WHD'], 
        ["temp", includeStateCases,'DIR_DLSE'],
        ["temp", includeStateCases,'DLSE_WageClaim'],
        ["temp", includeStateJudgements,'DLSE_J-23'],
        ["temp", includeStateJudgements,'DLSE_J-5413']
        #includeLocalData = False -- unused
        #includeOfficeData = False -- unused
        ]

    # df.to_csv(bug_log_csv) #debug outfile

    OLD_DATA = False
    if New_Data_On_Run_Test: 
        OLD_DATA = True #reset for testing
    PATH_EXISTS = os.path.exists(abs_path0) #url_backup folder
    if PATH_EXISTS: #check file age
        dir = os.listdir(abs_path0)
        if len(dir) < 2: #check if missing a file
            OLD_DATA = True
        else: #check if old
            import glob
            csv_files = glob.glob(os.path.join(abs_path0, "*.csv"))
            age_limit = (1 * 30 * 24 * 60 * 60) #seconds = 1 month x 30 days/mo x 24 hr/day x 60 min/hr x 60s/min
            for f in csv_files:
                born_on = os.path.getmtime(f)
                age = (time.time() - born_on)
                if age > age_limit: #if older than one month
                    OLD_DATA = True
                    os.remove(f) #remove and make new

    if PATH_EXISTS and not OLD_DATA: #url_backup folder cleaned files exist and newer than one month
        import glob
        csv_files = glob.glob(os.path.join(abs_path0, "*.csv"))
        
        for f in csv_files:
            #df_backup = pd.read_csv(f, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
            df_backup = pd.read_csv(f, encoding = 'utf8', low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
            DF_OG = pd.concat([df_backup, DF_OG], ignore_index=True)  
        

    else: #read new files from url source
        
        TEMP_TARGET_INDUSTRY = industriesDict['All NAICS']

        #Test file
        url0 = "https://stanford.edu/~granite/DLSE_no_returns_Linux_TEST.csv" #<-- open and edit this file with test data
        
        #find updated url -- #dev by Henry 8/21/2023
        ret = requests.post('https://enforcedata.dol.gov/views/data_summary.php', data={'agency':'whd'})
        m = re.search(r'(https://enfxfr.dol.gov/\.\./data_catalog/WHD/whd_whisard_[0-9]{8}\.csv\.zip)', str(ret.content))   
        #url1 = "https://enfxfr.dol.gov/data_catalog/WHD/whd_whisard_20230710.csv.zip" #update link from here https://enforcedata.dol.gov/views/data_catalogs.php
        url1 = m.group(0)

        #pre-2019 DLSE data
        # url2 = "https://www.researchgate.net/profile/Forest-Peterson/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019/data/61de6b974e4aff4a643603ae/HQ20009-HQ-2nd-Production-8132019.csv"
        # url2 = "https://drive.google.com/file/d/1TRaixcwTg08bEyPSchyHntkkktG2cuc-/view?usp=sharing"
        url2 = "https://stanford.edu/~granite/HQ20009-HQ2ndProduction8.13.2019_no_returns_Linux_CA.csv" #10/2/2022 added _Linux

        #NEED TO REVISE SO THIS IS A FRESH PULL LIKE WHD PULL
        TEST1 = "https://stanford.edu/~granite/WageClaimDataExport_State_Construction_Jan_8_2024.csv"
        TEST2 = "https://stanford.edu/~granite/ExportData_Judge_state_NAISC_23_Jan_8_2024.csv"
        TEST3 = "https://stanford.edu/~granite/ExportData_Judge_state_NAISC_5413_Jan_8_2024.csv"

        url_list = [
            [url0, includeTestData,'TEST'], 
            [url1, includeFedData,'DOL_WHD'], 
            [url2, includeStateCases,'DIR_DLSE'],
            [TEST1, includeStateCases,'DLSE_WageClaim'],
            [TEST2, includeStateJudgements,'DLSE_J-23'],
            [TEST3, includeStateJudgements,'DLSE_J-5413']
            #includeLocalData = False -- unused
            #includeOfficeData = False -- unused
            ]
        
        trigger = False #true is encoding="ISO-8859-1" else false is encoding='utf8'
        count = 1
        for n in url_list:
            if ((n[2] == 'DOL_WHD') or (n[2] == 'DIR_DLSE') ): 
                trigger = True #toggle between (true) encoding="ISO-8859-1" and (false) encoding='utf8'
            url = n[0]
            out_file_report = n[2]
            df_url = pd.DataFrame()
            df_url = Read_Violation_Data(TEST_CASES, url, out_file_report, trigger, bug_log_csv, abs_path, file_name) #save raw copy to csv_read_backup
            df_url = df_url.replace('\s', ' ', regex=True)  # remove line returns
            df_url = clean_function(RunFast, df_url, FLAG_DUPLICATE, bug_log, LOGBUG, log_number, bug_log_csv)
            df_url = inference_function(df_url, cityDict, TEMP_TARGET_INDUSTRY, 
                prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals, 
                bug_log, LOGBUG, log_number)
            save_backup_to_folder(df_url, url_backup_file+str(count), url_backup_path) #save copy to url_backup -- cleaned file
            count += 1

            DF_OG = pd.concat([df_url, DF_OG], ignore_index=True)

            trigger = False

    time_2 = time.time()
    append_log(bug_log, LOGBUG, f"Time to read csv(s) " + "%.5f" % (time_2 - time_1) + "\n")
    
    out_target = DF_OG.copy()  # new df -- hold df_csv as a backup and only df from this point on
    
    for n in url_list: #filter dataset for this run -- example, remove fed and state
        if n[1] == 0: # if data set should be removed
            out_target = out_target[(out_target.juris_or_proj_nm != n[2])] #save every data set except what is to be removed
        #out_target.to_csv(os.path.join(abs_path, (file_name+f'test_{n[2]}_out_file_report').replace(' ', '_') + '.csv'))

    out_target = DropDuplicateRecords(out_target, FLAG_DUPLICATE, bug_log_csv) #look for duplicates across data sets

    out_target = filter_function(out_target, TARGET_ZIPCODES, TARGET_INDUSTRY, open_cases_only, 
        infer_zip, infer_by_naics, TARGET_ORGANIZATIONS, YEAR_START, YEAR_END, target_state, 
        bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv)
    
    if signatories_report:
        time_1 = time.time()
        out_target = infer_signatory_cases(out_target, SIGNATORY_INDUSTRY)
        time_2 = time.time()
        log_number = "signatories_report"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # note--estimate back wage, penalty, and interest, based on violation
    time_1 = time.time()
    if use_assumptions: 
        out_target = compute_and_add_violation_count_assumptions(out_target)
        out_target = infer_backwages(out_target)  # B backwage, M monetary penalty
        out_target = infer_wage_penalty(out_target)  # B backwage, M monetary penalty
        out_target = wages_owed(out_target)
        out_target = calculate_interest_owed(out_target)
        out_target = backwages_owed(out_target)
    
    #out_target.to_csv(bug_log_csv)

    if open_cases_only == 1: 
        out_target = RemoveCompletedCases(out_target)
    
    time_2 = time.time()
    log_number = "optional assumptions, open cases"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    #*****EXTRACT VALUES FOR REPORT
    time_1 = time.time()
    case_disposition_series = out_target['Case Status'].copy()
    
    total_ee_violtd = out_target['ee_violtd_cnt'].sum()
    total_bw_atp = out_target['bw_amt'].sum()
    total_case_violtn = out_target['violtn_cnt'].sum()

    time_2 = time.time()
    log_number = "3 copy column and sum columns"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # filter
    out_prevailing_target = pd.DataFrame()
    out_signatory_target = pd.DataFrame()
    if 'Prevailing' in out_target.columns or "Signatory" in out_target.columns:
        time_1 = time.time()
        unique_legalname_sig = GroupByX(out_target, 'legal_nm')
        time_2 = time.time()
        log_number = "4 optional GroupByX for prevail and sig"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

        #unique_legalname_sig  = unique_legalname_sig[~unique_legalname_sig.index.duplicated()]
        time_1 = time.time()

        if 'Prevailing' in out_target.columns:
            out_prevailing_target = unique_legalname_sig.loc[unique_legalname_sig['Prevailing'] == 1]

        if "Signatory" in out_target.columns:
            out_signatory_target = unique_legalname_sig.loc[unique_legalname_sig["Signatory"] == 1]
        
        time_2 = time.time()
        log_number = "5 optional prevailing and signatory process"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")
    
    if signatories_report == 0 and 'Signatory' and 'legal_nm' and 'trade_nm' in out_target.columns:
            time_1 = time.time()
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
            time_2 = time.time()
            log_number = "optional signatory report"
            append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")
    
    # create csv output file**********************************
    
    time_1 = time.time()
    # added to prevent bug that outputs 2x
    out_target = out_target.drop_duplicates(keep='last')
    out_target.to_csv(temp_file_name_csv, encoding="utf-8-sig")
    time_2 = time.time()
    log_number = "6 drop_duplicates + print backup"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")
    
    # summary -- these are not used
    time_1 = time.time()
    all_unique_legalname = GroupByMultpleCases(out_target, 'legal_nm')
    all_unique_legalname = all_unique_legalname.sort_values(
        by=['records'], ascending=False) #this is never used
    
    all_agency_df = GroupByMultpleAgency(out_target)
    all_agency_df = all_agency_df.sort_values(by=['records'], ascending=False)  #this is never used
    
    time_2 = time.time()
    log_number = "7 never used group by legal name + all agency thing"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")
    
    # group repeat offenders************************************
    time_1 = time.time()
    out_counts = out_target.copy()  # hold for case counts

    unique_legalname = GroupByX(out_target, 'legal_nm')

    unique_address = GroupByMultpleCases(out_target, 'street_addr')
    unique_legalname2 = GroupByMultpleCases(out_target, 'legal_nm')
    unique_tradename = GroupByMultpleCases(out_target, 'trade_nm')
    unique_agency = GroupByMultpleCases(out_target, 'juris_or_proj_nm')
    unique_owner = GroupByMultpleCases(
        out_target, 'Jurisdiction_region_or_General_Contractor')
    agency_df = GroupByMultpleAgency(out_target)

    out_target = unique_legalname.copy()
    time_2 = time.time()
    log_number = "8 group by eight criteria"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # sort and format************************************

    # sort for table
    time_1 = time.time()
    out_sort_ee_violtd = out_target.sort_values(
        by=['ee_violtd_cnt'], ascending=False)
    out_sort_bw_amt = out_target.sort_values(by=['bw_amt'], ascending=False)
    out_sort_repeat_violtd = out_target.sort_values(
        by=['records'], ascending=False)

    unique_address = unique_address.sort_values(
        by=['records'], ascending=False)
    unique_legalname = unique_legalname.sort_values(
        by=['records'], ascending=False)
    unique_legalname2 = unique_legalname2.sort_values(
        by=['records'], ascending=False)
    unique_tradename = unique_tradename.sort_values(
        by=['records'], ascending=False)
    unique_agency = unique_agency.sort_values(by=['records'], ascending=False)
    unique_owner = unique_owner.sort_values(by=['records'], ascending=False)
    agency_df = agency_df.sort_values(by=['records'], ascending=False)
    time_2 = time.time()
    log_number = "9 sort a series of different"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # Format for summary
    time_1 = time.time()
    
    DF_OG = Filter_for_Zipcode(DF_OG, "", "", "California") #hack to just california records
    DF_OG_ALL = DF_OG.copy()
    DF_OG_ALL = DropDuplicateRecords(DF_OG_ALL, FLAG_DUPLICATE, bug_log_csv)

    DF_OG_VLN = DF_OG.copy()
    DF_OG_VLN = DropDuplicateRecords(DF_OG_VLN, FLAG_DUPLICATE, bug_log_csv)
    DF_OG_VLN = Clean_Summary_Values(DF_OG_VLN)
    time_2 = time.time()
    log_number = "10 drop duplicates and clean summary"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # report headers***************************************************
    # note that some headers have been renamed at the top of this program
    time_1 = time.time()
    header_two_way_table = ["violtn_cnt", "ee_violtd_cnt", "bw_amt", "records", "ee_pmt_recv"]
    header = ["legal_nm", "trade_nm", "cty_nm"] + header_two_way_table
    header_two_way = header_two_way_table + \
        ["zip_cd", 'legal_nm', "juris_or_proj_nm", 'case_id_1',
            'violation', 'violation_code', 'backwage_owed']

    header += ["naics_desc."]

    prevailing_header = header + ["juris_or_proj_nm", "Note"]

    if signatories_report == 1:
        header += ["Signatory"]
        prevailing_header += ["Signatory"]

    dup_header = header + ["street_addr"]
    dup_agency_header = header_two_way_table + ["juris_or_proj_nm"]
    dup_owner_header = header_two_way_table + \
        ["Jurisdiction_region_or_General_Contractor"]

    multi_agency_header = header + ["agencies", "agency_names", "street_addr"]
    time_2 = time.time()
    log_number = "11 make header lists"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # textfile output***************************************
    # HTML opening
    time_1 = time.time()

    # report main file--`w' create/zero text file for writing: the stream is positioned at the beginning of the file.
    textFile = open(temp_file_name, 'w')
    textFile.write("<!DOCTYPE html> \n")
    textFile.write("<html><body> \n")

    Title_Block(TEST_, DF_OG_VLN, DF_OG_ALL, target_jurisdition, TARGET_INDUSTRY,
                prevailing_wage_report, includeFedData, includeStateCases, includeStateJudgements, target_organization,
                open_cases_only, textFile)

    if Nonsignatory_Ratio_Block == True:
        #Signatory_to_Nonsignatory_Block(DF_OG, DF_OG, textFile)
        do_nothing = "<p>Purposeful Omission of Nonsignatory Ratio Block</p>"

    #if math.isclose(DF_OG['bw_amt'].sum(), out_counts['bw_amt'].sum(), rel_tol=0.03, abs_tol=0.0):
    #    do_nothing = "<p>Purposful Omission of Industry Summary Block</p>"
    #else:
    Industry_Summary_Block(out_counts, out_counts, total_ee_violtd, total_bw_atp,
        total_case_violtn, unique_legalname, agency_df, open_cases_only, textFile)
    Proportion_Summary_Block(out_counts, total_ee_violtd, total_bw_atp,
        total_case_violtn, unique_legalname, agency_df, YEAR_START, YEAR_END, open_cases_only, 
        target_jurisdition, TARGET_INDUSTRY, case_disposition_series, textFile, bug_log_csv)

    textFile.write("<HR> </HR>")  # horizontal line

    time_2 = time.time()
    log_number = 12
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # HTML closing
    time_1 = time.time()
    textFile.write("<P style='page-break-before: always'>")
    textFile.write("</html></body>")
    textFile.close()
    time_2 = time.time()
    log_number = "13 HTML Closing"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    # TABLES
    time_1 = time.time()
    
    if (include_tables == 1) and (len(unique_legalname.index) != 0):
        print_table_html_by_industry_and_city(temp_file_name, unique_legalname, header_two_way_table)
        print_table_html_by_industry_and_zipcode(temp_file_name, unique_legalname, header_two_way_table)

    if (include_summaries == 1) and (len(unique_legalname.index) != 0): 
        print_table_html_Text_Summary(include_summaries, temp_file_name, unique_legalname, header_two_way, header_two_way_table,
            total_ee_violtd, total_case_violtn, only_sig_summaries, TARGET_INDUSTRY)

    if (include_top_viol_tables == 1)  and (len(unique_address.index) != 0):
        print_top_viol_tables_html(out_target, unique_address, unique_legalname2, 
            unique_tradename, unique_agency, unique_owner, agency_df, out_sort_ee_violtd, 
            out_sort_bw_amt, out_sort_repeat_violtd, temp_file_name, signatories_report,
            out_signatory_target, sig_file_name_csv, prevailing_header, header, multi_agency_header, 
            dup_agency_header, dup_header, dup_owner_header, prevailing_wage_report, out_prevailing_target, 
            prev_file_name_csv, TEST_)
        
    if (len(unique_legalname.index) == 0) and (len(unique_address.index) == 0):
        textFile = open(temp_file_name, 'a')
        textFile.write("<p> There were no records found to report.</p> \n")
    
    time_2 = time.time()

    if include_methods:
        textFile = open(temp_file_name, 'a')
        textFile.write("<html><body> \n")
        textFile.write("<HR> </HR>")  # horizontal line
        textFile.write("<h1> Notes and methods summary</h1>")  # horizontal line

        Footer_Block(TEST_, textFile)

        Notes_Block(textFile)

        Methods_Block(textFile)

        Sources_Block(textFile)

        textFile.write("</html></body>")

    # updated 8/10/2022 by f. peterson to .format() per https://stackoverflow.com/questions/18053500/typeerror-not-all-arguments-converted-during-string-formatting-python
    log_number = 14
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} " + "%.5f" % (time_2 - time_1) + "\n")

    append_log(bug_log, LOGBUG, f"Time to finish report " + "%.5f" % (time_2 - time_0) + "\n")
    append_log(bug_log, LOGBUG, "<h1>DONE</h1> \n" + "</html></body> \n") #CLOSE
    # updated 8/10/2022 by f. peterson to .format() per https://stackoverflow.com/questions/18053500/typeerror-not-all-arguments-converted-during-string-formatting-python
    
    return temp_file_name  # the temp json returned from API



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


def get_key_from_value(d, val): #https://note.nkmk.me/en/python-dict-get-key-from-value/
    keys = [k for k, v in d.items() if v == val]
    if keys:
        return keys[0]
    return None


def generate_generic_zipcode_for_city(tempA, n_char = 1):
    #adding a generic zipcode for each city -- used later to filter records:
    position = len(tempA)
    # n_char -- replace just last zip code char seems to work best
    suffix = "X" * n_char
    tempB = tempA[:position-n_char] + suffix
    return tempB


def clean_function(RunFast, df, FLAG_DUPLICATE, bug_log, LOGBUG, log_number, bug_log_csv):

    function_name = "clean_function"

    if not RunFast:
        
        time_1 = time.time()
        df = Cleanup_Number_Columns(df)
        time_2 = time.time()
        log_number = "Cleanup number columns"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")
        
        time_1 = time.time()
        df = Cleanup_Text_Columns(df, bug_log, LOGBUG, bug_log_csv)
        time_2 = time.time()
        log_number = "Cleanup text columns"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

        #time_1 = time.time()
        #df = CleanUpAgency(df, ) #<-- use for case file codes
        #time_2 = time.time()
        #log_number+=1
        #append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

        time_1 = time.time()
        df = Define_Column_Types(df)
        time_2 = time.time()
        log_number = "Define_Column_Types"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

        # remove duplicate cases using case_id and violation as a unique key
        time_1 = time.time()
        df = DropDuplicateRecords(df, FLAG_DUPLICATE, bug_log_csv)
        df = FlagDuplicateBackwage(df, FLAG_DUPLICATE)
        time_2 = time.time()
        log_number = "DropDuplicateRecords"
        append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")
        
    return df


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

    if (TARGET_ORGANIZATIONS[1] != ""): 
        df = Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS)  #<--- BUGGY HERE 1/12/2023 2x records -- hacky fix w/ dup removal
    time_2 = time.time()
    log_number = "Filter_for_Target_Industry and Organization"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    #df.to_csv(os.path.join(abs_path, (file_name+'test_4_out_file_report').replace(' ', '_') + '.csv'))

    return df



def InferZipcode(df, cityDict):
    if not df.empty:
        df = InferZipcodeFromCityName(df, cityDict)  # zipcodes by city name
        df = InferZipcodeFromCompanyName(df, cityDict) #zipcodes by company name
        df = InferZipcodeFromAddress(df, cityDict) #zipcodes by address that is full length
        df = InferZipcodeFromJurisdictionName(df, cityDict) #zipcodes by jurisdiction name -- many false positive -- last ditch if no city name
        df = FillBlankZipcodes(df)
    return df


def infer_signatory_cases(df, SIGNATORY_INDUSTRY):
    if "Signatory" not in df.columns:
        df["Signatory"] = 0
    if 'signatory_industry' not in df.columns:
        df['signatory_industry'] = ""
    df = InferSignatoriesFromNameAndFlag(df, SIGNATORY_INDUSTRY)
    df = InferSignatoryIndustryAndLabel(df, SIGNATORY_INDUSTRY)
    # unused df_temp_address = InferSignatoriesFromAddressAndFlag(df, signatory_address_list)
    # unused df = InferSignatoriesFromNameAndAddressFlag(df, signatory_list, signatory_address_list)
    # unused df = df.append(df_temp_address)
    return df


def infer_prevailing_wage_cases(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals):
    df = InferPrevailingWageAndColumnFlag(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals)
    return df


def print_table_html_by_industry_and_city(temp_file_name, unique_legalname, header_two_way_table):

    # report main file--'a' Append, the file is created if it does not exist: stream is positioned at the end of the file.
    textFile = open(temp_file_name, 'a')
    textFile.write("<h2>Wage theft by industry and city region</h2> \n")
    textFile.close()

    df_all_industry = unique_legalname.groupby(['industry', pd.Grouper(key='cty_nm')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
        "bw_amt": 'sum',
        "violtn_cnt": 'sum',
        "ee_violtd_cnt": 'sum',
        "ee_pmt_recv": 'sum',
        "records": 'sum',
    }).reset_index().sort_values(['industry', 'cty_nm'], ascending=[True, True])

    df_all_industry = df_all_industry.set_index(['industry', 'cty_nm'])
    df_all_industry = df_all_industry.sort_index()

    df_all_industry = df_all_industry.reindex(columns=header_two_way_table)
    for industry, new_df in df_all_industry.groupby(level=0):

        new_df = pd.concat([
            new_df,
            new_df.sum().to_frame().T.assign(
                industry='', cty_nm='COUNTYWIDE').set_index(['industry', 'cty_nm'])
        ], sort=True).sort_index()

        new_df["bw_amt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
        new_df["violtn_cnt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
        new_df["ee_violtd_cnt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)
        new_df["ee_pmt_recv"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
        new_df["records"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["records"]), axis=1)

        write_to_html_file(new_df, header_two_way_table,
                            "", file_path(temp_file_name))


def print_table_html_by_industry_and_zipcode(temp_file_name, unique_legalname, header_two_way_table):

    textFile = open(temp_file_name, 'a')  # append to main report file
    textFile.write(
        "<h2>Wage theft by zip code region and industry</h2> \n")
    textFile.close()

    df_all_industry_3 = unique_legalname.groupby(["zip_cd", pd.Grouper(key='industry')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
        "bw_amt": 'sum',
        "violtn_cnt": 'sum',
        "ee_violtd_cnt": 'sum',
        "ee_pmt_recv": 'sum',
        "records": 'sum',
    }).reset_index().sort_values(['zip_cd', 'industry'], ascending=[True, True])

    df_all_industry_3 = df_all_industry_3.set_index(['zip_cd', 'industry'])
    df_all_industry_3 = df_all_industry_3.sort_index()

    df_all_industry_3 = df_all_industry_3.reindex(columns=header_two_way_table)
    for zip_cd, new_df_3 in df_all_industry_3.groupby(level=0):

        new_df_3 = pd.concat([
            new_df_3,
            new_df_3.sum().to_frame().T.assign(
                zip_cd='', industry='ZIPCODEWIDE').set_index(['zip_cd', 'industry'])
        ], sort=True).sort_index()

        new_df_3["bw_amt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
        new_df_3["ee_pmt_recv"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
        new_df_3["records"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["records"]), axis=1)
        new_df_3["violtn_cnt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
        new_df_3["ee_violtd_cnt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)

        write_to_html_file(new_df_3, header_two_way_table,
                            "", file_path(temp_file_name))


def print_table_html_Text_Summary(include_summaries, temp_file_name, unique_legalname, header_two_way, header_two_way_table,
    total_ee_violtd, total_case_violtn, only_sig_summaries, TARGET_INDUSTRY):
    if include_summaries == 1:
        
        if 'backwage_owed' not in unique_legalname.columns: unique_legalname['backwage_owed'] = 0 #hack bug fix 10/29/2022

        df_all_industry_n = unique_legalname.groupby(["cty_nm", pd.Grouper(key="zip_cd"), pd.Grouper(key='industry'),  pd.Grouper(key='legal_nm')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
            "bw_amt": 'sum',
            "violtn_cnt": 'sum',
            "ee_violtd_cnt": 'sum',
            "ee_pmt_recv": 'sum',
            'backwage_owed': 'sum',
            "records": 'sum',
        }).reset_index().sort_values(['cty_nm', "zip_cd", 'industry', 'legal_nm'], ascending=[True, True, True, True])

        df_all_industry_n = df_all_industry_n.set_index(
            ['cty_nm', "zip_cd", 'industry', 'legal_nm'])
        df_all_industry_n = df_all_industry_n.sort_index()

        df_all_industry_n = df_all_industry_n.reindex(columns=header_two_way)
    
        RunHeaderOnce = True
        for cty_nm, new_df_n, in df_all_industry_n.groupby(level=0):

            # new_df_2 = new_df_n.reset_index(level=1, drop=True).copy() #make a copy without zipcode level 0
            new_df_2 = new_df_n.droplevel("zip_cd").copy()

            new_df_legal_nm = new_df_2.drop(
                columns=['legal_nm'])  # delete empty column
            # pull out legal_nm column from level
            new_df_legal_nm = new_df_legal_nm.reset_index()
            city_unique_legalname = GroupByX(new_df_legal_nm, 'legal_nm')
            city_total_bw_atp = new_df_2['bw_amt'].sum()
            # debug 10/30/2020 this is an approximation based on records which is actually an overtstated mix of case and violations counts
            city_cases = new_df_2['records'].sum()

            new_df_drop1 = new_df_n.droplevel("zip_cd").copy()
            new_df_drop1 = new_df_drop1.droplevel('legal_nm')
            city_agency_df = GroupByMultpleAgency(new_df_drop1)

            #PRINT SECTION HEADER
            if RunHeaderOnce and (only_sig_summaries == 0 or city_cases > 10 or city_total_bw_atp > 10000):
                RunHeaderOnce = False
                textFile = open(temp_file_name, 'a')  # append to report main file
                textFile.write("<h2>Wage theft by city and industry</h2> \n")
                textFile.close()

            #PRINT SUMMARY BLOCK
            if only_sig_summaries == 0 or (city_cases > 10 or city_total_bw_atp > 10000):
                City_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn,
                                city_unique_legalname, city_agency_df, cty_nm, only_sig_summaries, file_path(temp_file_name))
                City_Summary_Block_4_Zipcode_and_Industry(
                    new_df_n, df_all_industry_n, TARGET_INDUSTRY, only_sig_summaries, file_path(temp_file_name))

                #Industry_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn, city_unique_legalname, city_agency_df, cty_nm, SUMMARY_SIG, file_path(temp_file_name))
                #Industry_Summary_Block_4_Zipcode_and_City(new_df_n, df_all_industry_n, TARGET_INDUSTRY, SUMMARY_SIG, file_path(temp_file_name) )

            new_df_2 = new_df_2.groupby(["cty_nm", 'industry']).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
                "bw_amt": 'sum',
                "violtn_cnt": 'sum',
                "ee_violtd_cnt": 'sum',
                "ee_pmt_recv": 'sum',
                "records": 'sum',
            }).reset_index().sort_values(['cty_nm', 'industry'], ascending=[True, True])

            new_df_2 = new_df_2.set_index(['cty_nm', 'industry'])
            new_df_2 = new_df_2.sort_index()

            #commented out to test 1/29/2023
            
            new_df_2 = pd.concat([
                new_df_2.sum().to_frame().T.assign(
                    cty_nm='', industry='CITYWIDE').set_index(['cty_nm', 'industry'])
                , new_df_2
            ], sort=True).sort_index()
            

            # new_df_2 = new_df_2.loc[:,~new_df_2.columns.duplicated()] #https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns

            new_df_2["bw_amt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
            new_df_2["ee_pmt_recv"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
            new_df_2["records"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["records"]), axis=1)
            new_df_2["violtn_cnt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
            new_df_2["ee_violtd_cnt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)

            #PRINT DATA TABLE
            if only_sig_summaries == 0 or (city_cases > 10 or city_total_bw_atp > 10000):
                write_to_html_file(new_df_2, header_two_way_table,
                                "", file_path(temp_file_name))


def print_top_viol_tables_html(df, unique_address, unique_legalname2, 
    unique_tradename, unique_agency, unique_owner, agency_df, out_sort_ee_violtd, 
    out_sort_bw_amt, out_sort_repeat_violtd, temp_file_name, signatories_report,
    out_signatory_target, sig_file_name_csv, prevailing_header, header, multi_agency_header, dup_agency_header, dup_header, 
    dup_owner_header, prevailing_wage_report, out_prevailing_target, prev_file_name_csv, TEST):

    if not df.empty and (len(unique_address) != 0):
        import matplotlib

        
        TEST = True
        if TEST == False:
            # format
            #unique_address = Clean_Repeat_Violator_HTML_Row(unique_address, 'street_addr') #removed 4/18/2024 -- F.Peterson, I have no idea what this did: looks like added 'no records' to some rows
            unique_address = FormatNumbersHTMLRow(unique_address)

            #unique_legalname2 = Clean_Repeat_Violator_HTML_Row(unique_legalname2, 'legal_nm')
            unique_legalname2 = FormatNumbersHTMLRow(unique_legalname2)

            #unique_tradename = Clean_Repeat_Violator_HTML_Row(unique_tradename, 'trade_nm')
            unique_tradename = FormatNumbersHTMLRow(unique_tradename)

            #unique_agency = Clean_Repeat_Violator_HTML_Row(unique_agency, 'juris_or_proj_nm')
            unique_agency = FormatNumbersHTMLRow(unique_agency)

            #unique_owner = Clean_Repeat_Violator_HTML_Row(unique_owner, 'Jurisdiction_region_or_General_Contractor')
            unique_owner = FormatNumbersHTMLRow(unique_owner)

            agency_df = FormatNumbersHTMLRow(agency_df)

            out_sort_ee_violtd = FormatNumbersHTMLRow(out_sort_ee_violtd)
            out_sort_bw_amt = FormatNumbersHTMLRow(out_sort_bw_amt)
            out_sort_repeat_violtd = FormatNumbersHTMLRow(out_sort_repeat_violtd)

        df.plot()  # table setup

        # tables top 10 violators

        #write_to_html_file(out_sort_bw_amt, header, "TEST: Top violators by amount of backwages stolen (by legal name)", file_path(temp_file_name), 6)
        #write_to_html_file(out_sort_ee_violtd, header, "TEST: Top violators by number of employees violated (by legal name)", file_path(temp_file_name), 6)
        #write_to_html_file(out_sort_repeat_violtd, header, "TEST: Top violators by number of repeat violations (by legal name)", file_path(temp_file_name), 6)

        result = '''
            <html>
            <head>
            <style>

                h2 {
                    text-align: center;
                    font-family: Helvetica, Arial, sans-serif;
                }
                table { 
                    margin-left: auto;
                    margin-right: auto;
                }
                table, th, td {
                    border: 1px solid black;
                    border-collapse: collapse;
                }
                th, td {
                    padding: 5px;
                    text-align: center;
                    font-family: Helvetica, Arial, sans-serif;
                    font-size: 90%;
                }
                table tbody tr:hover {
                    background-color: #dddddd;
                }
                .wide {
                    width: 90%; 
                }

            </style>
            </head>
            <body>
            '''
        
        #with open(temp_file_name, 'a', encoding='utf-8') as f:  # append to report main file
        if TEST == False:
            if not out_sort_bw_amt.empty:
                # by backwages
                result += "<h2>Top violators by amount of backwages stolen (by legal name)</h2> \n"
                result += out_sort_bw_amt.head(6).to_html(columns=header, index=False)

            if not out_sort_ee_violtd.empty:
                # by employees
                result += "<h2>Top violators by number of employees violated (by legal name)</h2> \n"
                result += out_sort_ee_violtd.head(6).to_html(
                    columns=header, index=False)

            if not out_sort_repeat_violtd.empty:
                # by repeated
                result += "<h2>Top violators by number of repeat violations (by legal name)</h2> \n"
                result += out_sort_repeat_violtd.head(6).to_html(
                    columns=header, index=False)

            # by repeat violators******************
            row_head = 24
            if not unique_address.empty:
                # by unique_address
                result += "<h2>Group by address and sort by records</h2> \n"
                result += unique_address.head(row_head).to_html(
                    columns=dup_header, index=False)
            else:
                result += "<p> There are no groups by address to report</p> \n"

            if not unique_legalname2.empty:
                # by 'legal_name'
                result += "<h2>Group by legal name and sort by records</h2> \n"
                result += unique_legalname2.head(row_head).to_html(
                    columns=dup_header, index=False)
            else:
                result += "<p> There are no groups by legal name to report</p> \n"

            if not unique_tradename.empty and not (unique_tradename['trade_nm'].isna().all() | (unique_tradename['trade_nm']=="").all()) and TEST != 3:
                # by unique_trade_nm
                result += "<h2>Group by trade name and sort by records</h2> \n"
                result += unique_tradename.head(row_head).to_html(
                    columns=dup_header, index=False)
            else:
                result += "<p> There are no groups by trade name to report</p> \n"

            if not agency_df.empty:
                # report for cases from multiple agencies
                result += "<h2>Group by company and sort by number of agencies involved</h2> \n"
                result += agency_df.head(row_head).to_html(
                    columns=multi_agency_header, index=False)
            else:
                result += "<p> There are no groups by companies with violations by multiple agencies to report</p> \n"

            if not unique_agency.empty:
                # report agency counts
                # by unique_agency
                result += "<h2>Cases by agency or owner</h2> \n"
                result += unique_agency.head(row_head).to_html(
                    columns=dup_agency_header, index=False)
            else:
                result += "<p> There are no case groups by agency or owner to report</p> \n"

            if not unique_owner.empty:
                # by 'unique_owner'
                result += "<h2>Cases by jurisdiction (includes private jurisdictions)</h2> \n"
                result += unique_owner.head(row_head).to_html(
                    columns=dup_owner_header, index=False)
            else:
                result += "<p> There are no case groups by jurisdiction to report</p> \n"

            # signatory
            if signatories_report == 1 and not out_signatory_target.empty:

                out_sort_signatory = pd.DataFrame()
                out_sort_signatory = out_signatory_target.sort_values(
                    'legal_nm', ascending=True)

                out_sort_signatory['violtn_cnt'] = out_sort_signatory.apply(
                    lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)
                out_sort_signatory['ee_pmt_recv'] = out_sort_signatory.apply(
                    lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)

                f.write("<P style='page-break-before: always'>")
                out_sort_signatory.to_csv(sig_file_name_csv)

                f.write("<h2>All signatory wage violators</h2> \n")

                if not len(out_sort_signatory.index) == 0:
                    f.write("<p>Signatory wage theft cases: ")
                    f.write(str.format('{0:,.0f}', len(
                        out_sort_signatory.index)))
                    f.write("</p> \n")

                if not out_sort_signatory['bw_amt'].sum() == 0:
                    f.write("<p>Total signatory wage theft: $")
                    f.write(str.format(
                        '{0:,.0f}', out_sort_signatory['bw_amt'].sum()))
                    f.write("</p> \n")
                '''
                if not out_sort_signatory['ee_violtd_cnt'].sum()==0:
                    f.write("<p>Signatory wage employees violated: ")
                    out_sort_signatory['ee_violtd_cnt'] = pd.to_numeric(out_sort_signatory['ee_violtd_cnt'] )
                    f.write(str.format('{0:,.0f}',out_sort_signatory['ee_violtd_cnt'].sum() ) )
                    f.write("</p> \n")

                if not out_sort_signatory['violtn_cnt'].sum()==0:
                    f.write("<p>Signatory wage violations: ")
                    out_sort_signatory['violtn_cnt'] = pd.to_numeric(out_sort_signatory['violtn_cnt'] )
                    f.write(str.format('{0:,.0f}',out_sort_signatory['violtn_cnt'].sum() ) )
                    f.write("</p> \n")
                '''

                f.write("\n")

                out_sort_signatory.to_html(
                    f, max_rows=3000, columns=prevailing_header, index=False)

                f.write("\n")


            # prevailing wage
            if prevailing_wage_report == 1 and not out_prevailing_target.empty:
                out_sort_prevailing_wage = pd.DataFrame()
                #out_sort_prevailing_wage = out_prevailing_target.sort_values('records', ascending=False)
                out_sort_prevailing_wage = out_prevailing_target.sort_values(
                    'legal_nm', ascending=True)

                out_sort_prevailing_wage['violtn_cnt'] = out_sort_prevailing_wage.apply(
                    lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)
                out_sort_prevailing_wage['ee_pmt_recv'] = out_sort_prevailing_wage.apply(
                    lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)

                f.write("<P style='page-break-before: always'>")
                out_sort_prevailing_wage.to_csv(prev_file_name_csv)

                f.write("<h2>All prevailing wage violators</h2> \n")

                f.write("<p>Prevailing wage theft cases: ")
                f.write(str.format('{0:,.0f}', len(
                    out_sort_prevailing_wage.index)))
                #f.write(str.format('{0:,.0f}',len(out_sort_prevailing_wage['records'].sum() ) ) )
                f.write("</p> \n")

                f.write("<p>Total prevailing wage theft: $")
                f.write(str.format(
                    '{0:,.0f}', out_sort_prevailing_wage['bw_amt'].sum()))
                f.write("</p> \n")

                f.write("<p>Total prevailing wage theft: $")
                f.write(str.format(
                    '{0:,.0f}', out_sort_prevailing_wage['bw_amt'].sum()))
                f.write("</p> \n")

                # buggy 6/14/2021
                # f.write("<p>Prevailing wage employees violated: ")
                # out_sort_prevailing_wage['ee_violtd_cnt'] = pd.to_numeric(out_sort_prevailing_wage['ee_violtd_cnt'] )
                # f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['ee_violtd_cnt'].sum() ) )
                # f.write("</p> \n")

                # f.write("<p>Prevailing wage violations: ")
                # out_sort_prevailing_wage['violtn_cnt'] = pd.to_numeric(out_sort_prevailing_wage['violtn_cnt'] )
                # f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['violtn_cnt'].sum() ) )
                # f.write("</p> \n")

                f.write("\n")
                # 12/25/2021 added "float_format=lambda x: '%10.2f' % x" per https://stackoverflow.com/questions/14899818/format-output-data-in-pandas-to-html
                out_sort_prevailing_wage.to_html(
                    f, max_rows=3000, columns=prevailing_header, index=False, float_format=lambda x: '%10.2f' % x)

                f.write("\n")
            
        result += '''
            </body>
            </html>
            '''

        with open(temp_file_name, mode='a', encoding='utf-8') as f:  # append to report main file
                f.write(result)
    else:
        result += '''
            </body>
            </html>
            '''
        with open(temp_file_name, mode='a', encoding='utf-8') as f:  # append to report main file
                f.write(result)


def Clean_Repeat_Violator_HTML_Row(df, COLUMN_NAME): #idk what this if for anymore -- F.PEterson 4/19/2024
    # https://stackoverflow.com/questions/18172851/deleting-dataframe-row-in-pandas-based-on-column-value
    df = df[df.records > 1]
    
    if COLUMN_NAME in df.columns:
        if (df[COLUMN_NAME].isna().all() | (df[COLUMN_NAME]=="").all()):
            df[COLUMN_NAME] = "no records"
    else:
        df = df.assign(COLUMN_NAME = "no records")
    
    #https://stackoverflow.com/questions/24284342/insert-a-row-to-pandas-dataframe
    for i in range(10000):
        df = pd.concat([pd.DataFrame([[1,2]], columns=df.columns), df], ignore_index=True)
    
    return df


def GroupByX(df, COLUMN_NAME):

    if COLUMN_NAME in df.columns and not (df[COLUMN_NAME].isna().all() | (df[COLUMN_NAME]=="").all() ) : 

        df = df[df[COLUMN_NAME].notnull()]
        df = df[df[COLUMN_NAME] != 'nan']
        df = df[df[COLUMN_NAME] != 'NAN']
        df = df[df[COLUMN_NAME] != '']
        df = df[df[COLUMN_NAME] != 0]
        df = df[df[COLUMN_NAME] != False]

        df[COLUMN_NAME] = df[COLUMN_NAME].str.upper()

        df['records'] = 1
        df['records'] = df.groupby(COLUMN_NAME)[COLUMN_NAME].transform(
            'count')  # count duplicates

        df['bw_amt'] = df.groupby(COLUMN_NAME)["bw_amt"].transform('sum')
        #df['backwage_owed'] = df.groupby(COLUMN_NAME)['backwage_owed'].transform('sum')
        df['violtn_cnt'] = df.groupby(
            COLUMN_NAME)["violtn_cnt"].transform('sum')
        df['ee_violtd_cnt'] = df.groupby(
            COLUMN_NAME)["ee_violtd_cnt"].transform('sum')
        df['ee_pmt_recv'] = df.groupby(
            COLUMN_NAME)["ee_pmt_recv"].transform('sum')

        df = df.drop_duplicates(subset=[COLUMN_NAME], keep='first')

        #df[COLUMN_NAME] = df[COLUMN_NAME].str.title()
    return df


def GroupByMultpleCases(df, COLUMN_NAME):

    if COLUMN_NAME in df.columns and not (df[COLUMN_NAME].isna().all() | (df[COLUMN_NAME]=="").all() ): 

        df = df[df[COLUMN_NAME].notnull()]
        df = df[df[COLUMN_NAME] != 'nan']
        df = df[df[COLUMN_NAME] != 'NAN']
        df = df[df[COLUMN_NAME] != '']
        df = df[df[COLUMN_NAME] != 0]
        df = df[df[COLUMN_NAME] != False]

        df[COLUMN_NAME] = df[COLUMN_NAME].str.upper()

        df['records'] = 1
        df['records'] = df.groupby(COLUMN_NAME)[COLUMN_NAME].transform(
            'count')  # count duplicates
        if 'bw_amt' not in df.columns:
            df['bw_amt'] = 0
        else:
            df['bw_amt'] = df.groupby(COLUMN_NAME)["bw_amt"].transform('sum')
        if 'backwage_owed' not in df.columns:
            df['backwage_owed'] = 0
        else:
            df['backwage_owed'] = df.groupby(
                COLUMN_NAME)['backwage_owed'].transform('sum')
        if 'violtn_cnt' not in df.columns:
            df['violtn_cnt'] = 0
        else:
            df['violtn_cnt'] = df.groupby(
                COLUMN_NAME)["violtn_cnt"].transform('sum')
        if 'ee_violtd_cnt' not in df.columns:
            df['ee_violtd_cnt'] = 0
        else:
            df['ee_violtd_cnt'] = df.groupby(
                COLUMN_NAME)["ee_violtd_cnt"].transform('sum')
        if 'ee_pmt_recv' not in df.columns:
            df['ee_pmt_recv'] = 0
        else:
            df['ee_pmt_recv'] = df.groupby(
                COLUMN_NAME)["ee_pmt_recv"].transform('sum')

        df = df.drop_duplicates(subset=[COLUMN_NAME], keep='first')
        df = df[df.records > 1]

        #df[COLUMN_NAME] = df[COLUMN_NAME].str.title()
    return df


def GroupByMultpleAgency(df):

    df['legal_nm'] = df['legal_nm'].astype(str).str.upper()
    df['juris_or_proj_nm'] = df['juris_or_proj_nm'].astype(str).str.upper()

    df = df[df['legal_nm'].notnull()]
    df = df[df['legal_nm'] != 'nan']
    df = df[df['legal_nm'] != 'NAN']
    df = df[df['legal_nm'] != '']

    df = df[df['juris_or_proj_nm'].notnull()]
    df = df[df['juris_or_proj_nm'] != 'nan']
    df = df[df['juris_or_proj_nm'] != 'NAN']
    df = df[df['juris_or_proj_nm'] != '']

    df['records'] = 1
    df['records'] = df.groupby(['legal_nm', 'juris_or_proj_nm'])[
        'legal_nm'].transform('count')
    df['bw_amt'] = df.groupby(['legal_nm', 'juris_or_proj_nm'])[
        "bw_amt"].transform('sum')
    df['violtn_cnt'] = df.groupby(['legal_nm', 'juris_or_proj_nm'])[
        "violtn_cnt"].transform('sum')
    df['ee_violtd_cnt'] = df.groupby(['legal_nm', 'juris_or_proj_nm'])[
        "ee_violtd_cnt"].transform('sum')
    df['ee_pmt_recv'] = df.groupby(['legal_nm', 'juris_or_proj_nm'])[
        "ee_pmt_recv"].transform('sum')

    df = df.drop_duplicates(
        subset=['legal_nm', 'juris_or_proj_nm'], keep='first') #POSSIBLE BUGGEY 1/12/2023

    df['agencies'] = 0
    df['agencies'] = df.groupby('legal_nm')['legal_nm'].transform(
        'count')  # count duplicates across agencies
    df['records'] = df.groupby('legal_nm')['records'].transform('sum')
    df['bw_amt'] = df.groupby('legal_nm')["bw_amt"].transform('sum')
    df['violtn_cnt'] = df.groupby('legal_nm')["violtn_cnt"].transform('sum')
    df['ee_violtd_cnt'] = df.groupby(
        'legal_nm')["ee_violtd_cnt"].transform('sum')
    df['ee_pmt_recv'] = df.groupby('legal_nm')["ee_pmt_recv"].transform('sum')

    df['agency_names'] = np.nan
    if not df.empty:
        df['agency_names'] = df.groupby('legal_nm')['juris_or_proj_nm'].transform(
            ", ".join)  # count duplicates across agencies

    df = df.drop_duplicates(subset=['legal_nm'], keep='first')
    df = df[df.agencies > 1]

    #df['legal_nm'] = df['legal_nm'].str.title()
    df['juris_or_proj_nm'] = df['juris_or_proj_nm'].str.title() #convert string to title case

    return df


def fill_case_status_for_missing_enddate(df):
    if 'findings_start_date' not in df.columns:
        df['findings_start_date'] = ""
    if 'findings_end_date' not in df.columns:
        df['findings_end_date'] = ""
    #if 'Case Status' not in df.columns:
    #    df['Case Status'] = ""

    enddate_missing = (
        ((df['findings_end_date'] == '0' ) | (df['findings_end_date'] == 0 ) | pd.isna(df['findings_end_date']) | 
        (df['findings_end_date'] == False) | (df['findings_end_date'] == "" ) | (df['findings_end_date'].isna() ) )  
    )

    casestatus_missing = (
        ((df['Case Status'] == '0' ) | (df['Case Status'] == 0 ) | pd.isna(df['Case Status']) | 
        (df['Case Status'] == False) | (df['Case Status'] == "" ) | (df['Case Status'].isna() ))  
    )
    
    df.loc[enddate_missing & casestatus_missing, "Case Status"] = "APPEARS OPEN: NO END + NO STATUS"

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


def Filter_for_Target_Industry(df, TARGET_INDUSTRY, infer_by_naics):
    
    if 'trade2' not in df.columns:
        df['trade2'] = ''
    appended_data = pd.DataFrame()
    # https://stackoverflow.com/questions/28669482/appending-pandas-dataframes-generated-in-a-for-loop
    for x in range(len(TARGET_INDUSTRY) ):
        temp_term = TARGET_INDUSTRY[x][0]       
        #df_temp = df.loc[df['industry'].str.upper() == temp_term.upper()]
        df_temp = df[df['industry'].str.upper() == temp_term.upper()]
        if not infer_by_naics: #exclude the use of inferenced values
            df_temp = df_temp.loc[df_temp['naic_cd'].notnull()]
        #appended_data = pd.concat([appended_data, df_temp])
        appended_data = pd.concat([appended_data, df_temp])

    appended_data = appended_data.drop_duplicates()

    return appended_data


def Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS):
    organization_list = ''.join(TARGET_ORGANIZATIONS[1]).split('|')

    df_temp_0 = df.loc[df['legal_nm'].astype(str).str.contains(
        '|'.join(organization_list), case=False, na=False) ] #na=False https://stackoverflow.com/questions/66536221/getting-cannot-mask-with-non-boolean-array-containing-na-nan-values-but-the
    df_temp_1 = df.loc[df['trade_nm'].astype(str).str.contains(
        '|'.join(organization_list), case=False, na=False) ]

    df_temp = pd.concat([df_temp_0, df_temp_1], ignore_index=True)
    df_temp = df_temp.drop_duplicates()

    return df_temp


def Filter_for_Zipcode(df, TARGET_ZIPCODES, infer_zip, target_state):
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


def RemoveCompletedCases(df):

    #INFERENCE
    "APPEARS OPEN: NO END + NO STATUS"

    #DLSE categories
    '''
    'Closed - Paid in Full (PIF)/Satisfied'
    'Full Satisfaction'
    'Open'
    'Open - Bankruptcy Stay'
    'Open - Payment Plan'
    'Partial Satisfaction'
    'Pending/Open'
    'Preliminary'
    'Vacated'
    '''

    #Closure Disposition
    '''
    Dismissed   Claimant Withdrew
    Paid in Full   Post ODA
    Paid in Full   Pre-Hearing
    Settled   At Conference
    Settled   At hearing
    Settled   Pre-Conference
    Settled   Pre-hearing
    Plaintiff rec d $0 ODA
    '''

    #Closure Disposition - Other Reason
    '''
    Duplicate
    '''

    if "Note" in df: #remove these cases from the active data
        df = df.loc[
            (df["Note"] != 'Closed - Paid in Full (PIF)/Satisfied') &
            (df["Note"] != 'Full Satisfaction') &
            (df["Note"] != 'Vacated') &

            (df["Case Status"] != 'Closed-Claimant Judgment') &
            (df["Case Status"] != 'Closed - Satisfied') &
            #(df["Case Status"] != 'Open - Partial Payment/Satisfaction') &

            (df["Case Status"] != 'Dismissed   Claimant Withdrew') &
            (df["Case Status"] != 'Paid in Full   Post ODA') &
            (df["Case Status"] != 'Paid in Full   Pre-Hearing') &
            (df["Case Status"] != 'Settled   At Conference') &
            (df["Case Status"] != 'Settled   At hearing') &
            (df["Case Status"] != 'Settled   Pre-Conference') &
            (df["Case Status"] != 'Settled   Pre-hearing') &
            (df["Case Status"] != 'Settled   Post Hearing (Post Judgment, Prior to JEU referral)') &
            (df["Case Status"] != 'Settled   Post Hearing (Prior to ODA)') &
            (df["Case Status"] != 'Settled   Post Hearing (Post ODA, Prior to Judgment)') &
            (df["Case Status"] != 'Settled   Post Appeal') &
            (df["Case Status"] != 'Plaintiff rec d ODA') &
            (df["Case Status"] != 'Paid in Full   Post Judgment Entry') &
            (df["Case Status"] != 'Duplicate') &
            (df["Case Status"] != 'duplicate') &
            (df["Case Status"] != 'Duplicate Case') &
            (df["Case Status"] != 'Duplicate Claim') &
            (df["Case Status"] != 'ODA - Dft') &
            (df["Case Status"] != 'Outside settlement between parties') &
            (df["Case Status"] != 'Parties settled outside the office') &
            (df["Case Status"] != 'Payment remitted to Labor Commissioner because employer cannot locate employee') &
            (df["Case Status"] != 'Pd prior to PHC') & 
            (df["Case Status"] != 'nothing owed still employed') &
            
            (df["Closure Disposition"] != 'Dismissed   Claimant Withdrew') &
            (df["Closure Disposition"] != 'Paid in Full   Post ODA') &
            (df["Closure Disposition"] != 'Paid in Full   Pre-Hearing') &
            (df["Closure Disposition"] != 'Settled   At Conference') &
            (df["Closure Disposition"] != 'Settled   At hearing') &
            (df["Closure Disposition"] != 'Settled   Pre-Conference') &
            (df["Closure Disposition"] != 'Settled   Pre-hearing') &
            (df["Closure Disposition"] != 'Settled   Post Hearing (Post Judgment, Prior to JEU referral)') &
            (df["Closure Disposition"] != 'Settled   Post Hearing (Prior to ODA)') &
            (df["Closure Disposition"] != 'Settled   Post Hearing (Post ODA, Prior to Judgment)') &
            (df["Closure Disposition"] != 'Settled   Post Appeal') &
            (df["Closure Disposition"] != 'Plaintiff rec d ODA') &
            (df["Closure Disposition"] != 'Paid in Full   Post Judgment Entry') &
            (df["Closure Disposition - Other Reason"] != 'Duplicate') &
            (df["Closure Disposition - Other Reason"] != 'duplicate') &
            (df["Closure Disposition - Other Reason"] != 'Duplicate Case') &
            (df["Closure Disposition - Other Reason"] != 'Duplicate Claim') &
            (df["Closure Disposition - Other Reason"] != 'ODA - Dft') &
            (df["Closure Disposition - Other Reason"] != 'Outside settlement between parties') &
            (df["Closure Disposition - Other Reason"] != 'Parties settled outside the office') &
            (df["Closure Disposition - Other Reason"] != 'Payment remitted to Labor Commissioner because employer cannot locate employee') &
            (df["Closure Disposition - Other Reason"] != 'Pd prior to PHC') & 
            (df["Closure Disposition - Other Reason"] != 'nothing owed still employed') &
            
            (df["Reason For Closing"] != 'Plt settled w/Dft - SofJ') &
            (df["Reason For Closing"] != 'conceded wages paid by employer. no claims by plaintiff') &
            (df["Reason For Closing"] != 'duplicate') &
            (df["Reason For Closing"] != 'Duplicate') &
            (df["Reason For Closing"] != 'Duplicate case') &
            (df["Reason For Closing"] != 'ODA - Pd') &
            (df["Reason For Closing"] != 'ODA Pd') &
            (df["Reason For Closing"] != 'Paid in full per settlement') &
            (df["Reason For Closing"] != 'Paid in Full') &
            (df["Reason For Closing"] != 'paid in full post judgment entry.') &
            (df["Reason For Closing"] != 'parties settled') &
            (df["Reason For Closing"] != 'Pd per ODA') &
            (df["Reason For Closing"] != 'Pd') &
            (df["Reason For Closing"] != 'Plaintiff withdrew claim') &
            (df["Reason For Closing"] != 'Plt w/d clm') &
            (df["Reason For Closing"] != 'Parties settled for an amount less on ODA')


        ]

    #cleanup data and replace empty cells w/ zero
        
    if 'bw_amt' not in df.columns:
        df['bw_amt'] = 0
    if 'ee_pmt_recv' not in df.columns:
        df['ee_pmt_recv'] = 0

    df['bw_amt'] = df['bw_amt'].replace('NaN', 0).astype(float)
    df['ee_pmt_recv'] = df['ee_pmt_recv'].replace('NaN', 0).astype(float)

    df['bw_amt'] = df['bw_amt'].fillna(0).astype(float)
    df['ee_pmt_recv'] = df['ee_pmt_recv'].fillna(0).astype(float)

    # df.index # -5 fixes an over count bug and is small enough that it wont introduce that much error into reports
    '''
    bw = df['bw_amt'].tolist()
    pmt = df['ee_pmt_recv'].tolist()
    for i in range(0, len(bw)-5):
        if math.isclose(bw[i], pmt[i], rel_tol=0.20, abs_tol=1.0):  # works
            # if math.isclose(df['bw_amt'][i], df['ee_pmt_recv'][i], rel_tol=0.10, abs_tol=0.0): #error
            # if math.isclose(df.loc[i].at['bw_amt'], df.loc[i].at['ee_pmt_recv'], rel_tol=0.10, abs_tol=0.0): #error
            # if math.isclose(df.at[i,'bw_amt'], df.at[i,'ee_pmt_recv'], rel_tol=0.10, abs_tol=0.0): #error
            try:
                df.drop(df.index[i], inplace=True)
            except IndexError:
                dummy = ""
    '''

    if 'bw_amt' and 'ee_pmt_recv' in df: #take cases that are open
        tolerance = 1.05 # outstanding value must be greater than 105% of payment (less than 95% paid)
        abs_tol = -0.001 # allows zero but no negative values
        df = df.loc[  #remove these cases from the active data
            (df['bw_amt'] >= (df['ee_pmt_recv']*tolerance) ) & #more than 5% of amount still owed
            ((df['bw_amt'] - df['ee_pmt_recv']) >= abs_tol )  #amount owed is not negative and is more than $0
            #&
            #(df["Case Stage"] != 'Judgment') &
            #(df["Case Stage"] != 'JEU Referral') &
            #(df["Case Status"] != 'Judgment Issued') &
            #(df["Closure Disposition"] != 'De Novo Referral') &
            #(df["Closure Disposition"] != 'Judgment') &
            #(df["Closure Disposition"] != 'Judgment   Claimant Collect') &
            #(df["Closure Disposition"] != 'Judgment   JEU') &
            #(df["Closure Disposition - Other Reason"] != 'Appeal filed') &
            #(df["Closure Disposition - Other Reason"] != 'Case has been appealed') & 
            #(df["Closure Disposition - Other Reason"] != 'Defendant appealed, P represented by own attorney') &
            #(df["Closure Disposition - Other Reason"] != 'Ongoing criminal trial, close until resolved and P may re-open if desire.') &
            #(df["Closure Disposition - Other Reason"] != 'Plaintiff appealed') &
            #(df["Closure Disposition - Other Reason"] != 'Plt left U.S. to renew visa, visa request was denied.  Plt does not have return date to USA yet.  Plt may request to reopen upon return to US.') &
            #(df["Closure Disposition - Other Reason"] != 'Plt unable to state a clear, valid claim.  Temporarily closed to give Plt a chance to consult with the Law Center and submit a valid wage claim.') &
            #(df["Closure Disposition - Other Reason"] != 'Requested postponement for settlement talks, no request to re-start process.') &
            #(df["Closure Disposition - Other Reason"] != 'unclaimed wages returned for NSF - unable to contact Plt') 
        ]
    
    return df


def InferPrevailingWageAndColumnFlag(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals):

    if 'Prevailing' not in df.columns:
        df['Prevailing'] = '0'
    else:
        df['Prevailing'] = df.Prevailing.fillna("0")
    
    if "Reason For Closing" not in df.columns:
        df["Reason For Closing"] = ""
    if 'Closure Disposition - Other Reason' not in df.columns:
        df['Closure Disposition - Other Reason'] = ""
    if 'violation_code' not in df.columns:
        df['violation_code'] = ""
    if 'violation' not in df.columns:
        df['violation'] = ""
    if 'Note' not in df.columns:
        df['Note'] = ""
    

    prevailing_wage_pattern = '|'.join(prevailing_wage_terms)
    found_prevailing_0 = (
        ((df['Reason For Closing'].astype(str).str.contains(prevailing_wage_pattern, case = False))) |
        ((df['Closure Disposition - Other Reason'].astype(str).str.contains(prevailing_wage_pattern, case = False)))
    )

    prevailing_wage_labor_code_pattern = '|'.join(prevailing_wage_labor_code)
    found_prevailing_1 = (
        ((df['violation_code'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) |
        ((df['violation'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) |
        ((df['Note'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) 
    )

    prevailing_wage_political_pattern = '|'.join(prevailing_wage_politicals)
    found_prevailing_2 = (
        ((df['legal_nm'].astype(str).str.contains(prevailing_wage_political_pattern, case = False)))
    )

    df.loc[((found_prevailing_0 | found_prevailing_1 | found_prevailing_2) & 
        ((df['industry'] == "Construction") | (df['industry'] == 'Utilities') )), 
        'Prevailing'] = '1'

    #specific to DOL WHD data
    if "dbra_cl_violtn_cnt" in df.columns:
        df.loc[df["dbra_cl_violtn_cnt"] > 0, "violation_code"] = "DBRA"
        df.loc[df["dbra_cl_violtn_cnt"] > 0, "Prevailing"] = "1"

    return df


def InferAgencyFromCaseIDAndLabel(df, LABEL_COLUMN):

    if LABEL_COLUMN in df.columns:
        # find case IDs with a hyphen
        foundAgencybyCaseID_1 = pd.isna(df[LABEL_COLUMN])
        # df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna(foundAgencybyCaseID_1.replace( (True,False), (df['case_id_1'].astype(str).apply(lambda st: st[:st.find("-")]), df[LABEL_COLUMN]) ) ) #https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas
        # https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas
        df.loc[foundAgencybyCaseID_1, LABEL_COLUMN] = df['case_id_1'].astype(
            str).apply(lambda st: st[:st.find("-")])

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


def InferSignatoriesFromNameAndFlag(df, SIGNATORY_INDUSTRY):

    if 'legal_nm' and 'trade_nm' in df.columns:
        if "Signatory" not in df.columns:
            df["Signatory"] = 0

        

        for x in range(1, len(SIGNATORY_INDUSTRY)):
            PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(
                SIGNATORY_INDUSTRY[x][1])
            PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

            foundIt_sig = (
                (
                    df['legal_nm'].str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['legal_nm'].str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)) |
                (
                    df['trade_nm'].str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['trade_nm'].str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True))
            )
            df.loc[foundIt_sig, "Signatory"] = 1

    return df


def InferSignatoriesFromNameAndAddressFlag(df, signatory_name_list, signatory_address_list):

    if "Signatory" not in df.columns:
        df["Signatory"] = 0

    pattern_signatory_name = '|'.join(signatory_name_list)
    pattern_signatory_add = '|'.join(signatory_address_list)

    foundIt_sig = (
        ((df['legal_nm'].str.contains(pattern_signatory_name, na=False, flags=re.IGNORECASE, regex=True)) &
            (df['street_addr'].str.contains(
                pattern_signatory_add, na=False, flags=re.IGNORECASE, regex=True))
         ) |
        ((df['trade_nm'].str.contains(pattern_signatory_name, na=False, flags=re.IGNORECASE, regex=True)) &
         (df['street_addr'].str.contains(
             pattern_signatory_add, na=False, flags=re.IGNORECASE, regex=True))
         )
    )
    df.loc[foundIt_sig,"Signatory"] = 1

    return df


def InferSignatoriesFromAddressAndFlag(df, signatory_address_list):

    if "Signatory" not in df.columns:
        df["Signatory"] = 0

    pattern_signatory = '|'.join(signatory_address_list)
    foundIt_sig = (
        (df['street_addr'].str.contains(
            pattern_signatory, na=False, flags=re.IGNORECASE, regex=True))
        #(df['street_addr'].str.match(pattern_signatory, na=False, flags=re.IGNORECASE) )
    )
    df.loc[foundIt_sig, "Signatory"] = 1

    return df


def Infer_Industry(df, TARGET_INDUSTRY):
    if 'industry' not in df.columns:
        df['industry'] = "" 
    if 'trade2' not in df.columns:
        df['trade2'] = "" #inferred industry

    if not df.empty:  # filter out industry rows
        # exclude cell 0 that contains an industry label cell
        for x in range(1, len(TARGET_INDUSTRY)):
            # add a levinstein distance with distance of two and match and correct: debug 10/30/2020
            PATTERN_IND = '|'.join(TARGET_INDUSTRY[x])
            PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(TARGET_INDUSTRY[x])

            if 'legal_nm' and 'trade_nm' in df.columns: #uses legal and trade names to infer industry
                foundIt_ind1 = (
                    (
                        df['legal_nm'].astype(str).str.contains(
                            PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True)
                        &
                        ~df['legal_nm'].astype(str).str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True))
                    |
                    (
                        df['trade_nm'].astype(str).str.contains(
                            PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True)
                        &
                        ~df['trade_nm'].astype(str).str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True))
                )
                df.loc[foundIt_ind1, 'trade2'] = TARGET_INDUSTRY[x][0]

            foundIt_ind2 = (  # uses the exisiting NAICS descriptions to fill gaps in the data
                ((df['industry'] == "") | df['industry'].isna() ) &
                df['naics_desc.'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                ~df['naics_desc.'].astype(str).str.contains(
                    PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
            )
            df.loc[foundIt_ind2, 'industry'] = TARGET_INDUSTRY[x][0]
            df.loc[foundIt_ind2, 'trade2'] = "" #clear inference if found

            # uses the exisiting NAICS coding
            foundIt_ind3 = (
                df['naic_cd'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                ~df['naic_cd'].astype(str).str.contains(
                    PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
            )
            df.loc[foundIt_ind3, 'industry'] = TARGET_INDUSTRY[x][0]
            df.loc[foundIt_ind3, 'trade2'] = "" #clear inference if found

        # uses the existing NAICS coding and fill blanks with inferred

        df['industry'] = df.apply(
            lambda row:
            row['trade2'] if (row['industry'] == '') and (row['trade2'] != '') #take guessed industry if NAICS is missing
            else row['industry'], axis=1 
        )

        # if all fails, assign 'Undefined' so it gets counted
        df['industry'] = df['industry'].replace(
            r'^\s*$', 'Undefined', regex=True)
        df['industry'] = df['industry'].fillna('Undefined')
        df['industry'] = df['industry'].replace(np.nan, 'Undefined')

        #'trade2' is not used outside this function other than in the print to csv 

    return df


def InferSignatoryIndustryAndLabel(df, SIGNATORY_INDUSTRY):
    if 'signatory_industry' not in df.columns:
        df['signatory_industry'] = ""

    if not df.empty and 'legal_nm' and 'trade_nm' in df.columns:  # filter out industry rows
        for x in range(1, len(SIGNATORY_INDUSTRY)):

            PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(
                SIGNATORY_INDUSTRY[x][1])
            PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

            foundIt_ind1 = (
                (
                    df['legal_nm'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['legal_nm'].astype(str).str.contains(
                        PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
                ) |
                (
                    df['trade_nm'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['trade_nm'].astype(str).str.contains(
                        PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
                ) |
                (df['Signatory'] == 1) &
                (df['industry'] == SIGNATORY_INDUSTRY[x][0][0])
            )
            df.loc[foundIt_ind1, 'signatory_industry'] = SIGNATORY_INDUSTRY[x][0][0]

        # if all fails, assign 'other' so it gets counted
        df['signatory_industry'] = df['signatory_industry'].replace(
            r'^\s*$', 'Undefined', regex=True)
        df['signatory_industry'] = df['signatory_industry'].fillna('Undefined')
        df['signatory_industry'] = df['signatory_industry'].replace(
            np.nan, 'Undefined')

    return df


def InferZipcodeFromCityName(df, cityDict):

    if 'cty_nm' in df.columns: #if no city column then skip

        #check if column is in df
        if 'zip_cd' not in df.columns:
            df['zip_cd'] = '99999'
        df.zip_cd = df.zip_cd.astype(str)

        for city in cityDict:
            #upper_region = city.upper()
            #PATTERN_CITY = '|'.join(upper_region)

            zipcode_is_empty = (
                ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
            )

            test = cityDict[city][0]
            foundZipbyCity = (
                ((df['cty_nm'].astype(str).str.contains(test, na=False, 
                    case=False, flags=re.IGNORECASE)))  # flags=re.IGNORECASE
                #((df['cty_nm'].astype(str).str.contains(PATTERN_CITY, na=False, 
                #    case=False, flags=re.IGNORECASE)))  # flags=re.IGNORECASE
            )

            tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
            tempB = generate_generic_zipcode_for_city(tempA)
            df.loc[zipcode_is_empty * foundZipbyCity, "zip_cd"] = tempB
    
    return df


def InferZipcodeFromAddress(df, cityDict):

    if 'street_addr' in df.columns: 

        for city in cityDict:

            zipcode_is_empty = (
                ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
            )

            test = cityDict[city][0]
            foundCity = (
                ((df['street_addr'].astype(str).str.contains(test, na=False, 
                    case=False, flags=re.IGNORECASE)))
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

    for city in cityDict:
        #PATTERN_CITY = '|'.join(cityDict[city][0])

        citynameisempty = ((pd.isna(df['cty_nm'])) | (df['cty_nm'] == '') )

        zipcode_is_empty = (
            ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
        )
        
        test = cityDict[city][0]

        foundZipbyCompany2 = (
            ((df['trade_nm'].astype(str).str.contains(test, na=False, case=False, flags=re.IGNORECASE))) |
            ((df['legal_nm'].astype(str).str.contains(
                test, na=False, case=False, flags=re.IGNORECASE)))
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

    df.juris_or_proj_nm = df.juris_or_proj_nm.astype(str)
    df.Jurisdiction_region_or_General_Contractor = df.Jurisdiction_region_or_General_Contractor.astype(
        str)

    for city in cityDict:

        #PATTERN_CITY = '|'.join(cityDict[city][0])

        citynameisempty = ((pd.isna(df['cty_nm'])) | (df['cty_nm'] == '') )

        zipcode_is_empty = (
            ((df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == '99999' ) |(df['zip_cd'] == 0 ) ) 
        )

        test = cityDict[city][0]

        foundZipbyCompany2 = (
            (df['juris_or_proj_nm'].str.contains(test, na=False, flags=re.IGNORECASE, regex=True)) |
            (df['Jurisdiction_region_or_General_Contractor'].str.contains(
                    test, na=False, flags=re.IGNORECASE, regex=True))
            #(df['juris_or_proj_nm'].str.contains(PATTERN_CITY, na=False, flags=re.IGNORECASE, regex=True)) |
            #(df['Jurisdiction_region_or_General_Contractor'].str.contains(
            #        PATTERN_CITY, na=False, flags=re.IGNORECASE, regex=True))
        )

        tempA = cityDict[city][len(cityDict[city]) - 1] #take last zipcode
        tempB = generate_generic_zipcode_for_city(tempA)
        df.loc[(citynameisempty & zipcode_is_empty) * foundZipbyCompany2, 'zip_cd'] = tempB

    return df


def FillBlankZipcodes (df):

    zipcode_is_empty = (
        ( (df['zip_cd'].isna()) | (df['zip_cd'] == "" ) | (df['zip_cd'] == '0' ) | (df['zip_cd'] == 0 ) )  
    )
    
    df.loc[zipcode_is_empty, "zip_cd"] = "99999"

    return df


def EXCLUSION_LIST_GENERATOR(SIGNATORY_INDUSTRY):
    target = pd.Series(SIGNATORY_INDUSTRY)
    # https://stackoverflow.com/questions/28679930/how-to-drop-rows-from-pandas-data-frame-that-contains-a-particular-string-in-a-p
    target = target[target.str.contains("!") == True]
    target = target.str.replace('[\!]', '', regex=True)
    if len(target) > 0:
        PATTERN_EXCLUDE = '|'.join(target)
    else:
        PATTERN_EXCLUDE = "999999"
    return PATTERN_EXCLUDE


def lookuplist(trade, list_x, col):
    import os

    trigger = True
    if pd.isna(trade):
        trade = ""
    for x in list_x:
        if x[0].upper() in trade.upper():
            value_out = + x[col]  # += to capture cases with multiple scenarios
            trigger = False
            # break
    if trigger:  # if true then no matching violations
        Other = ['Generic', 26.30, 33.49, 50.24, 50.24]
        # if value not found then return 0 <-- maybe this should be like check or add to a lrearning file
        value_out = Other[col]

        rel_path1 = 'report_output_/'
        # <-- dir the script is in (import os) plus up one
        script_dir1 = os.path.dirname(os.path.dirname(__file__))
        abs_path1 = os.path.join(script_dir1, rel_path1)
        if not os.path.exists(abs_path1):  # create folder if necessary
            os.makedirs(abs_path1)

        log_name = 'log_'
        out_log_report = 'new_trade_names_'
        log_type = '.txt'
        log_name_trades = os.path.join(abs_path1, log_name.replace(
            ' ', '_')+out_log_report + log_type)  # <-- absolute dir and file name

        # append/create log file with new trade names
        tradenames = open(log_name_trades, 'a')

        tradenames.write(trade)
        tradenames.write("\n")
        tradenames.close()
    return value_out


def wages_owed(df):

    df['wages_owed'] = (pd.to_numeric(df['bw_amt'], errors='coerce') - pd.to_numeric(df["ee_pmt_recv"], errors='coerce') ) #added to_numeric() 4/18/2024 to fix str error
    df['wages_owed'] = np.where((df['wages_owed'] < 0), 0, df['wages_owed'])  # overwrite

    return df


def backwages_owed(df):

    #estimated_bw_plug = max(total_bw_atp//total_ee_violtd,1000)

    # montetary penalty only if backwage is zero -- often penalty is in the backwage column
    # df['cmp_assd_cnt'] = np.where((df['bw_amt'].isna() | (df['bw_amt']=="")), estimated_bw_plug * 0.25, df['cmp_assd_cnt'])
    # df['cmp_assd_cnt'] = np.where(df['bw_amt'] == 0, estimated_bw_plug * 0.25, df['cmp_assd_cnt'])
    # df['cmp_assd_cnt'] = np.where(df['bw_amt'] == False, estimated_bw_plug * 0.25, df['cmp_assd_cnt'])
    if 'cmp_assd_cnt' not in df.columns: df['cmp_assd_cnt'] = 0

    df['backwage_owed'] = df['wages_owed'] + \
        df['cmp_assd_cnt'] + df['interest_owed'] #fill backwages as sum of wages, penalty, and interest

    # at some point, save this file for quicker reports and only run up to here when a new dataset is introduced
    return df


def calculate_interest_owed(df):
    #remove negatives that are due to a bug somehwere
    df['wages_owed'] = np.where(df['wages_owed'] < 0, 0, df['wages_owed'])

    if 'findings_start_date' not in df.columns:
         df['findings_start_date'] = ""
    else:
        df['findings_start_date'] = pd.to_datetime(
            df['findings_start_date'], errors='coerce')
    # crashed here 2/5/2022 "Out of bounds nanosecond timestamp: 816-09-12 00:00:00" -- fixed w/ errors='coerce' in Jan 2024
    
    if 'findings_end_date' not in df.columns: 
        df['findings_end_date'] = ""
    else:
        df['findings_end_date'] = pd.to_datetime(
            df['findings_end_date'], errors='coerce')
    
    df['Calculate_end_date'] = df['findings_end_date'].fillna(
        pd.to_datetime('today') )

    df['Calculate_start_date'] = df['findings_start_date'].fillna(
        df['findings_end_date'])
    
    df['Days_Unpaid'] = pd.to_datetime(
        'today') - df['Calculate_start_date']
    # df['Days_Unpaid'] = np.where(df['Days_Unpaid'] < pd.Timedelta(0,errors='coerce'), (pd.Timedelta(0, errors='coerce')), df['Days_Unpaid'] )

    #A = Interest_Owed
    df['Years_Unpaid'] = df['Days_Unpaid'].dt.days.div(365)
    r = 7 #26 U.S. Code § 6621 - Determination of rate of interest -- https://www.dol.gov/agencies/ebsa/employers-and-advisers/plan-administration-and-compliance/correction-programs/vfcp/table-of-underpayment-rates
    n = 365
    if 'Interest_Accrued' not in df.columns: df['Interest_Accrued'] = 0
    df['Interest_Accrued'] = (
        df['wages_owed'] * (((1 + ((r/100.0)/n)) ** (n*df['Years_Unpaid'])))) - df['wages_owed']
    df['Interest_Accrued'] = df['Interest_Accrued'].fillna(
        df['Interest_Accrued'])
    
    #B = Interest_Payments_Recd
    if 'Interest_Payments_Recd' not in df.columns: df['Interest_Payments_Recd'] = 0
    df['Interest_Payments_Recd'] = np.where(
        df['Interest_Payments_Recd'] < 0, 0, df['Interest_Payments_Recd'])
    
    #Interest Owed = A - B
    df['interest_owed'] = (
        df['Interest_Accrued'] - df["Interest_Payments_Recd"])
    #remove negatives that are due to a bug somehwere
    df['interest_owed'] = np.where(
        df['interest_owed'] < 0, 0, df['interest_owed'])

    return df


def infer_backwages(df):

    mean_backwage = df['bw_amt'].mean()

    if (mean_backwage) == 0 or (mean_backwage > 4000):
        mean_backwage = 3368 #plug amount $2,000

    if 'assumed_backwage' not in df.columns: df['assumed_backwage'] = "NO"

    df['ee_violtd_cnt'] = np.where(((df['ee_violtd_cnt'] == 0) | (df['ee_violtd_cnt'] == "") | (
        df['ee_violtd_cnt'] == '0') | (df['ee_violtd_cnt'] == False) | (df['ee_violtd_cnt'].isna()) ), 1, df['ee_violtd_cnt']) #default 1
    
    df['violtn_cnt'] = np.where(((df['violtn_cnt'] == 0) | (df['violtn_cnt'] == "") | (
        df['violtn_cnt'] == '0') | (df['violtn_cnt'] == False) | (df['violtn_cnt'].isna()) ), df['ee_violtd_cnt'], df['violtn_cnt']) #default equal to # of employees

    df['assumed_backwage'] = np.where(((df['bw_amt'] == 0) | (df['bw_amt'] == "") | (
        df['bw_amt'] == '0') | (df['bw_amt'] == False) | (df['bw_amt'].isna()) ), "YES", df['assumed_backwage'])
    
    df['bw_amt'] = np.where(((df['bw_amt'] == 0) | (df['bw_amt'] == "") | (
        df['bw_amt'] == '0') | (df['bw_amt'] == False) | (df['bw_amt'].isna()) ), df['ee_violtd_cnt'] * mean_backwage , df['bw_amt'])

    return df


def infer_wage_penalty(df):

    mean_backwage = df['bw_amt'].mean()
    if (mean_backwage == 0) | (mean_backwage > 4000):
        mean_backwage = 3368 #plug amount $2,000
    #mean_backwage = df[df['bw_amt']!=0].mean()
    generic_penalty = mean_backwage * 0.125 #default is 12.5% of mean wage 

    # lookup term / (1) monetary penalty
    A = ["ACCESS TO PAYROLL", 750]  # $750
    B = ["L.C. 1021", 200]  # $200 per day (plug one day)
    C = ["L.C. 11942", mean_backwage]  # less than minimum wage
    D = ["L.C. 1197", mean_backwage]
    E = ["L.C. 1299", 500]
    F = ["L.C. 1391", 500]
    # improper deduction 30 days of wage (plug $15 x 8 x 30)
    G = ["L.C. 203", 3600]
    H = ["L.C. 2054", mean_backwage]
    I = ["L.C. 2060", mean_backwage]
    J = ["L.C. 223", mean_backwage]  # less than contract (prevailing)
    K = ["L.C. 226(a)", 250]  # paycheck itemized plus $250
    K1 = ["LCS 226(a)", 250]  # paycheck itemized plus $250
    L = ["L.C. 2267", 150]  # Meal periods plug ($150)
    M = ["L.C. 2675(a)", 250]
    # workmans compensation $500 to State Director for work comp fund
    N = ["L.C. 3700", 500]
    O = ["L.C. 510", 200]  # 8 hour workday $50 per pay period plug $200
    P = ["LIQUIDATED DAMAGES", mean_backwage]  # equal to lost wage plug mean
    Q = ["MEAL PERIOD PREMIUM WAGES", 75]  # 1 hour of pay (plug $15 * 5 days)
    R = ["MINIMUM WAGES", 50]  # plug $50
    S = ["Misclassification", mean_backwage]  # plug mean wage
    T = ["Overtime", mean_backwage]  # plus mean wage
    U = ["PAID SICK LEAVE", 200]  # $50 per day plug $200
    # aka PAGA $100 per pay period for 1 year plug $1,200
    V = ["PIECE RATE WAGES", 1200]
    W = ["PRODUCTION BONUS", mean_backwage]  # plug mean wage
    X = ["REGULAR WAGES", mean_backwage]  # plug mean wage
    # daily wage for 30 days plug $1,000 of (15 * 8 * 30)
    Y = ["REPORTING TIME PAY", 1000]
    Z = ["REST PERIOD PREMIUM WAGES", 2000]  # plug $2,000
    AA = ["VACATION WAGES", 2000]  # plug $2,000
    AB = ["SPLIT SHIFT PREMIUM WAGES", 500]  # $500
    AC = ["UNLAWFUL DEDUCTIONS", 2000]  # plug $2000
    AD = ["UNLAWFUL TIP DEDUCTIONS", 1000]  # $1,000
    AE = ["UNREIMBURSED BUSINESS EXPENSES", 2500]  # plug $2,500
    AF = ["WAITING TIME PENALTIES", 2500]  # plug $2,500
    AG = ["LC 1771", 125]  # 5 x $25
    AH = ["L.C. 1771", 125]  # 5 x $25
    AI = ["L.C. 1774", 125]  # 5 x $25
    AJ = ["LC 1774", 125]  # 5 x $25
    AK = ["", generic_penalty]  # <blank>
    AL = [False, generic_penalty]  # <blank>
    AM = [np.nan, generic_penalty]  # <blank>
    AN = [pd.NA, generic_penalty]  # <blank>

    penalties = [['MONETARY_PENALTY'], A, B, C, D, E, F, G, H, I, J, K, K1, L, M, N,
                 O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE, AF, AG, AH, AI, AJ, AK, AL, AM, AN]

    if 'violation' in df.columns: 
        if 'cmp_assd_cnt' not in df.columns: df['cmp_assd_cnt'] = 0 #Civil Monttary Penalties
        if 'assumed_cmp_assd' not in df.columns: df['assumed_cmp_assd'] = "NO" #test to debug

        #needs a multiplier for number fo violation
        df['cmp_assd_cnt'] = df.apply(
            lambda x: lookuplist(x['violation'], (x['ee_violtd_cnt'] * penalties), 1)
            if (x['assumed_cmp_assd'] == "YES")
            else x['cmp_assd_cnt'], axis=1)
    
    df['cmp_assd_cnt'] = np.where((df['cmp_assd_cnt'].isna() | (df['cmp_assd_cnt']=="") | 
                                   (df['cmp_assd_cnt'] == 0) | (df['cmp_assd_cnt'] == '0')),
                                   generic_penalty, df['cmp_assd_cnt'])

    return df


def compute_and_add_violation_count_assumptions(df):
    if not df.empty:
        # employee violated
        # DLSE cases are for one employee -- introduces an error when the dataset is violation records--need to remove duplicates
        df['ee_violtd_cnt'] = df['ee_violtd_cnt'].fillna(1)
        df['ee_violtd_cnt'] = np.where(
            (df['ee_violtd_cnt'].isna() | (df['ee_violtd_cnt']=="")), 1, df['ee_violtd_cnt'])  # catch if na misses
        df['ee_violtd_cnt'] = np.where(
            ((df['ee_violtd_cnt'] == 0) | (df['ee_violtd_cnt'] == '0')), 1, df['ee_violtd_cnt'])  # catch if na misses
        df['ee_violtd_cnt'] = np.where(
            df['ee_violtd_cnt'] == False, 1, df['ee_violtd_cnt'])  # catch if na misses
        total_ee_violtd = df['ee_violtd_cnt'].sum()  # overwrite

        # by issue count
        if 'violation' not in df.columns:
            df['violation'] = ""

        df['violtn_cnt'] = df['violtn_cnt'].fillna(
            df['violation'].astype(str).str.count("Issue"))  # assume mean
        df['violtn_cnt'] = np.where((df['violtn_cnt'].isna() | (df['violtn_cnt']=="")), df['violation'].astype(str).str.count(
            "Issue"), df['violtn_cnt'])  # catch if na misses
        df['violtn_cnt'] = np.where(
            ((df['violtn_cnt'] == 0) | (df['violtn_cnt'] == '0')), df['violation'].astype(str).str.count("Issue"), df['violtn_cnt'])
        df['violtn_cnt'] = np.where(
            df['violtn_cnt'] == False, df['violation'].astype(str).str.count("Issue"), df['violtn_cnt'])
        
        df['violtn_cnt'] = np.where((df['violtn_cnt'].isna() | (df['violtn_cnt']=="") |(df['violtn_cnt']=="0") | 
                                     (df['violtn_cnt'] == 0) | (df['violtn_cnt'] == False) ), df['ee_violtd_cnt'], df['violtn_cnt'])  # catch if all misses
        
        total_case_violtn = df['violtn_cnt'].sum()  # overwrite

        # violations
        # safe assumption: violation count is always more then the number of employees
        total_case_violtn = max(
            df['violtn_cnt'].sum(), df['ee_violtd_cnt'].sum())

        if total_ee_violtd != 0:  # lock for divide by zero error
            # violation estimate by mean
            estimated_violations_per_emp = max(
                total_case_violtn//total_ee_violtd, 1)
            df['violtn_cnt'] = df['violtn_cnt'].fillna(
                estimated_violations_per_emp)  # assume mean
            df['violtn_cnt'] = np.where(
                (df['violtn_cnt'].isna() | (df['violtn_cnt']=="")), (estimated_violations_per_emp), df['violtn_cnt'])  # catch if na misses
            df['violtn_cnt'] = np.where(
                ((df['violtn_cnt'] == 0) | (df['violtn_cnt'] == '0')), (estimated_violations_per_emp), df['violtn_cnt'])
            df['violtn_cnt'] = np.where(
                df['violtn_cnt'] == False, (estimated_violations_per_emp), df['violtn_cnt'])

    return df


def MoveCompanyLiabilityTermsToLiabilityTypeColumn(df):

    df['Liabilitytype'] = ""  # create new column and fill with str

    liability_terms0 = ['(a California)', '(a Californi)', '(a Californ)']
    pattern_liability0 = '|'.join(liability_terms0)
    if 'legal_nm' and 'trade_nm' in df.columns:
        foundIt0 = (df['legal_nm'].str.contains(pattern_liability0, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability0, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt0, 'Liabilitytype'] = 'California'

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a California', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Californi', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a California', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Californi', '', regex=True, case=False)

        liability_terms1 = ['(a Delaware)']
        pattern_liability1 = '|'.join(liability_terms1)
        foundIt1 = (
            df['legal_nm'].str.contains(pattern_liability1, na=False, flags=re.IGNORECASE, regex=True) |
            df['trade_nm'].str.contains(
                pattern_liability1, na=False, flags=re.IGNORECASE, regex=True)
        )
        df.loc[foundIt1, 'Liabilitytype'] = 'Delaware'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Delaware', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Delaware', '', regex=True, case=False)

        liability_terms2 = ['(a Nevada)']
        pattern_liability2 = '|'.join(liability_terms2)
        foundIt2 = (df['legal_nm'].str.contains(pattern_liability2, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability2, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt2, 'Liabilitytype'] = 'Delaware'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Nevada', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Nevada', '', regex=True, case=False)

        liability_terms3 = ['(a Nebraska)']
        pattern_liability3 = '|'.join(liability_terms3)
        foundIt3 = (df['legal_nm'].str.contains(pattern_liability3, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability3, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt3, 'Liabilitytype'] = 'Nebraska'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Nebraska', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Nebraska', '', regex=True, case=False)

        liability_terms4 = ['(a foreign)']
        pattern_liability4 = '|'.join(liability_terms4)
        foundIt4 = (df['legal_nm'].str.contains(pattern_liability4, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability4, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt4, 'Liabilitytype'] = 'Foreign'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Foreign', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Foreign', '', regex=True, case=False)

        liability_terms5 = ['(Which Will Be Doing Business In California)',
                            '(Which Will Be Doing Business In California As)']
        pattern_liability5 = '|'.join(liability_terms5)
        foundIt5 = (df['legal_nm'].str.contains(pattern_liability5, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability5, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt5, 'Liabilitytype'] = 'California'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Foreign', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Foreign', '', regex=True, case=False)

    return df


def MoveBusinessTypeToBusinessTypeColumn(df):

    individual_terms = ['an individual', 'individual ']
    pattern_individual = '|'.join(individual_terms)
    if 'legal_nm' and 'trade_nm' in df.columns:
        foundIt = (df['legal_nm'].str.contains(pattern_individual, na=False, flags=re.IGNORECASE, regex=True) |
                   df['trade_nm'].str.contains(pattern_individual, na=False, flags=re.IGNORECASE, regex=True))

        # df['Businesstype'] = foundIt.replace((True,False), ('Individual',df['Businesstype']), regex=True) #fill column 'industry'
        df.loc[foundIt, 'Businesstype'] = 'Individual'
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'an individual', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'an individual', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'individual ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'individual ', '', regex=True, case=False)
    return df


def MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df):
    company_terms = ['LLC', 'L.L.C.', 'company', 'Comapany', 'a limited liability company', 'a limited liability', 'Co.',
                     'Limited Liability', 'Limited Liability Comapany', 'Limited Liability Company']
    pattern_company = '|'.join(company_terms)

    if 'legal_nm' and 'trade_nm' in df.columns:
        foundIt = (df['legal_nm'].str.contains(pattern_company, na=False, flags=re.IGNORECASE, regex=True) |
                   df['trade_nm'].str.contains(pattern_company, na=False, flags=re.IGNORECASE, regex=True))

        # df['Businesstype'] = foundIt.replace((True,False), ('Company',df['Businesstype']), regex=True) #fill column 'industry'
        df.loc[foundIt, 'Businesstype'] = 'Company'

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            r'\bLLC$', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            r'\bLLC$', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            r'\bL.L.C.$', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            r'\bL.L.C.$', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'L.L.C., ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'L.L.C., ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'LLC, ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'LLC, ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'L.L.C. ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'L.L.C. ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull(
        )]['legal_nm'].str.replace('LLC ', '', regex=False, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull(
        )]['trade_nm'].str.replace('LLC ', '', regex=False, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'company', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'company', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Company', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Company', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'comapany', '', regex=True, case=False)  # common typo
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'comapany', '', regex=True, case=False)  # common typo
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Comapany', '', regex=True, case=False)  # common typo
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Comapany', '', regex=True, case=False)  # common typo

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a limited liability', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a limited liability', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a Limited Liability', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a Limited Liability', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'limited liability', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'limited liability', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Limited Liability', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Limited Liability', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull(
        )]['legal_nm'].str.replace(r'\bCo$', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull(
        )]['trade_nm'].str.replace(r'\bCo$', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull(
        )]['legal_nm'].str.replace('Co. ', '', regex=False, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull(
        )]['trade_nm'].str.replace('Co. ', '', regex=False, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull(
        )]['legal_nm'].str.replace('Co ', '', regex=False, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull(
        )]['trade_nm'].str.replace('Co ', '', regex=False, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull(
        )]['legal_nm'].str.replace('CO ', '', regex=False, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull(
        )]['trade_nm'].str.replace('CO ', '', regex=False, case=False)
    return df


def MovePartnershipBusinessTypeToBusinessTypeColumn(df):
    partnership_terms = ['as partners', 'LLP', 'individually & jointly', 'both individually & as partners', 'individually and as partners',
                         'both individually and as partners', 'both individually and jointly liable']
    pattern_partner = '|'.join(partnership_terms)

    if 'legal_nm' and 'trade_nm' in df.columns:
        foundIt = (df['legal_nm'].str.contains(pattern_partner, na=False, flags=re.IGNORECASE, regex=True) |
                   df['trade_nm'].str.contains(pattern_partner, na=False, flags=re.IGNORECASE, regex=True))

        # df['Businesstype'] = foundIt.replace((True,False), ('Partnership',df['Businesstype']), regex=True) #fill column 'industry'
        df.loc[foundIt, 'Businesstype'] = 'Partnership'

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both jointly and severally as employers', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both jointly and severally as employers', '',  regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'jointly and severally as employers', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'jointly and severally as employers', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each individually and as partners', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each individually and as partners', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both individually and as partners', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both individually and as partners', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'individually and as partners', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'individually and as partners', '', regex=True, case=False)

        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each individually and jointly liable', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each individually and jointly liable', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both individually and jointly liable', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both individually and jointly liable', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'individually and jointly liable', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'individually and jointly liable', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each individually and jointly', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each individually and jointly', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both individually and jointly', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both individually and jointly', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'individually and jointly', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'individually and jointly', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each jointly and severally', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each jointly and severally', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both jointly and severally', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both jointly and severally', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'jointly and severally', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'jointly and severally', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each severally and as joint employers', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each severally and as joint employers', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each severally and as joint employ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each severally and as joint employ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'both severally and as joint employers', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'both severally and as joint employers', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'severally and as joint employers', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'severally and as joint employers', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each severally and as joint', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each severally and as joint', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'severally and as joint', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'severally and as joint', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a general partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a general partnership', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'general partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'general partnership', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a limited partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a limited partnership', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'limited partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'limited partnership', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a partnership', '',  regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'partnership', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'partnership', '',  regex=True, case=False)

        # last ditch cleanup
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'each severally and as', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'each severally and as', '',  regex=True, case=False)

    return df


def MoveCorportationBusinessTypeToBusinessTypeColumn(df):
    corporation_terms = ['inc.', 'corporation', 'corp.', 'corp', 'inc', 'a corporation', 'Incorporated', 'non-profit corporation',
                         'nonprofit', 'non-profit']
    pattern_corp = '|'.join(corporation_terms)

    if 'legal_nm' and 'trade_nm' in df.columns:
        foundIt = (df['legal_nm'].str.contains(pattern_corp, na=False, flags=re.IGNORECASE, regex=True) |
                   df['trade_nm'].str.contains(pattern_corp, na=False,  flags=re.IGNORECASE, regex=True))

        # df['Businesstype'] = foundIt.replace((True,False), ('Corporation', df['Businesstype']), regex=True) #fill column business type
        df.loc[foundIt, 'Businesstype'] = 'Corporation'

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'a corporation', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'a corporation', '', regex=True, case=False)
        #df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('corporation', '', regex=True, case=False)
        #df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('corporation', '', regex=True, case=False)

        #df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('CORPORATION', '', regex=True, case=False)

        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Corporation', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Corporation', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Incorporated', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Incorporated', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Corp ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Corp ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Inc ', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Inc ', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Inc.', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Inc.', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'inc', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'inc', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'Corp.', '', regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'Corp.', '', regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'non-profit corporation', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'non-profit corporation', '',  regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'nonprofit', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'nonprofit', '',  regex=True, case=False)
        df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(
            'non-profit', '',  regex=True, case=False)
        df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(
            'non-profit', '',  regex=True, case=False)
    return df


def RemovePunctuationFromCity(df):  # text cleanup: remove double spaces

    if 'cty_nm' in df.columns:
        df['cty_nm'] = df['cty_nm'].str.replace(',', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('.', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace(';', '', regex=False)
        #df['cty_nm'] = df['cty_nm'].str.replace('-','')
        df['cty_nm'] = df['cty_nm'].str.replace("'", '', regex=False)

        df['cty_nm'] = df['cty_nm'].str.replace('  ', ' ', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('&', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.strip(';,. ')

        df['cty_nm'] = df['cty_nm'].str.replace('(', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace(')', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('|', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('/', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('*', '', regex=False)
        df['cty_nm'] = df['cty_nm'].str.replace('  ', ' ', regex=False)

    return df


def RemovePunctuationFromAddresses(df):  # text cleanup: remove double spaces

    if 'street_addr' in df.columns:
        df['street_addr'] = df['street_addr'].str.replace(',', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace('.', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace(';', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace('-', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace("'", '', regex=False)

        df['street_addr'] = df['street_addr'].str.replace(
            '  ', ' ', regex=False)
        df['street_addr'] = df['street_addr'].str.replace(
            '&', 'and', regex=False)
        df['street_addr'] = df['street_addr'].str.strip(';,. ')

        df['street_addr'] = df['street_addr'].str.replace('(', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace(')', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace('|', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace('/', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace('*', '', regex=False)
        df['street_addr'] = df['street_addr'].str.replace(
            '  ', ' ', regex=False)

    return df


def RemoveDoubleSpacesFromAddresses(df):  # text cleanup: remove double spaces

    if 'street_addr' in df.columns:
        df['street_addr'] = df['street_addr'].str.replace(',,', ',')
        df['street_addr'] = df['street_addr'].str.replace('  ', ' ')
        df['street_addr'] = df['street_addr'].str.strip()
        df['street_addr'] = df['street_addr'].str.strip(';,. ')
    if 'legal_nm' in df.columns:
        df['legal_nm'] = df['legal_nm'].str.replace('  ', ' ')
    return df


def ReplaceAddressAbreviations(df):
    if 'street_addr' in df.columns:
        df['street_addr'] = df['street_addr'].str.replace('  ', ' ')
        df['street_addr'] = df['street_addr'].str.strip()
        df['street_addr'] = df['street_addr'].str.strip(';,. ')
        # add a right space to differentiate 'Ave ' from 'Avenue'
        df['street_addr'] = df['street_addr'].astype(str) + ' '

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CT.', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('WY.', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CTR.', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'AVE.', 'Avenue', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' ST.', ' Street', regex=False)  # bugget with EAST
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'DR.', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('RD.', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('LN.', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ.', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CT,', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('WY,', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CTR,', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'AVE,', 'Avenue', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' ST,', ' Street', regex=False)  # bugget with EAST
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'DR,', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('RD,', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('LN,', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ,', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CT ', 'Court ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('WY ', 'Way ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'CTR ', 'Center ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'AVE ', 'Avenue ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' ST ', ' Street ', regex=False)  # bugget with EAST
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'DR ', 'Drive ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'RD ', 'Road ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'LN ', 'Lane ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ ', 'Plaza ', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bCT$', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bWY$', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bCTR$', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bAVE$', 'Avenue', regex=False)
        #df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bST$', 'Street', regex = False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bDR$', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bRD$', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bLN$', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bPLZ$', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ct.', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Wy.', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ctr.', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ave.', 'Avenue', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'St.', 'Street', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Dr.', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Rd.', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Ln.', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ.', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ct,', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Wy,', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ctr,', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ave,', 'Avenue', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'St,', 'Street', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Dr,', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Rd,', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Ln,', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ,', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ct ', 'Court ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('Wy ', 'Way ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ctr ', 'Center ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ave ', 'Avenue ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'St ', 'Street ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Dr ', 'Drive ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Rd ', 'Road ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ln ', 'Lane ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'PLZ ', 'Plaza ', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bCt$', 'Court', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bWy$', 'Way', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bCtr$', 'Center', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bAve$', 'Avenue', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bSt$', 'Street', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bDr$', 'Drive', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bRd$', 'Road', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bLn$', 'Lane', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            r'\bPLZ$', 'Plaza', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Boulevard', 'Blvd.', False, re.IGNORECASE)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Expressway', 'Expy.', False, re.IGNORECASE)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Building', 'Bldg.', False, re.IGNORECASE)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'STE.', 'Suite', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ste.', 'Suite', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Ste ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'STE ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'APT.', 'Suite', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Apt.', 'Suite', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Apt ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'APT ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Unit ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'Unit', 'Suite', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '# ', 'Suite ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull(
        )]['street_addr'].str.replace('#', 'Suite ', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'S. ', 'South ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'W. ', 'West ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'E. ', 'East ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            'N. ', 'North ', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' S ', ' South ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' W ', ' West ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' E ', ' East ', regex=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            ' N ', ' North ', regex=False)

        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '1ST ', 'First ', regex=True, case=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '2ND ', 'Second ', regex=True, case=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '3RD ', 'Third ', regex=True, case=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '4TH ', 'Fourth ', regex=True, case=False)
        df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(
            '5TH ', 'Fifth ', regex=True, case=False)
    return df


def StripPunctuationFromNames(df):
    if 'legal_nm' in df.columns:
        df['legal_nm'] = df['legal_nm'].astype(
            str)  # convert to str to catch floats
        df['legal_nm'] = df['legal_nm'].str.replace('  ', ' ', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('&', 'and', regex=False)
        df['legal_nm'] = df['legal_nm'].str.strip()
        df['legal_nm'] = df['legal_nm'].str.strip(';,. ')

        df['legal_nm'] = df['legal_nm'].str.replace(';', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace(',', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('.', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('-', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace("'", '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('(', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace(')', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('|', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('/', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('*', '', regex=False)
        df['legal_nm'] = df['legal_nm'].str.replace('  ', ' ', regex=False)

    if 'trade_nm' in df.columns:
        df['trade_nm'] = df['trade_nm'].astype(
            str)  # convert to str to catch floats
        df['trade_nm'] = df['trade_nm'].str.replace('  ', ' ', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('&', 'and', regex=False)
        df['trade_nm'] = df['trade_nm'].str.strip()
        df['trade_nm'] = df['trade_nm'].str.strip(';,. ')

        df['trade_nm'] = df['trade_nm'].str.replace(';', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('.', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace(',', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('-', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace("'", '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('(', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace(')', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('|', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('/', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('*', '', regex=False)
        df['trade_nm'] = df['trade_nm'].str.replace('  ', ' ', regex=False)

    return df


def RemoveDoubleSpacesFromCompanyName(df):
    if 'legal_nm' in df.columns:
        df['legal_nm'] = df['legal_nm'].str.replace(
            '  ', ' ')  # remove double spaces
        df['legal_nm'] = df['legal_nm'].str.replace(', , ', ', ')
        df['legal_nm'] = df['legal_nm'].str.replace(', , , ', ', ')
        df['legal_nm'] = df['legal_nm'].str.strip()
        df['legal_nm'] = df['legal_nm'].str.strip(';,. ')

    if 'trade_nm' in df.columns:
        df['trade_nm'] = df['trade_nm'].str.replace('  ', ' ')
        df['trade_nm'] = df['trade_nm'].str.replace(', , ', ', ')
        df['trade_nm'] = df['trade_nm'].str.replace(', , , ', ', ')
        df['trade_nm'] = df['trade_nm'].str.strip()
        df['trade_nm'] = df['trade_nm'].str.strip(';,. ')

        # add a right space to differentiate 'Inc ' from 'Incenerator'
        df['legal_nm'] = df['legal_nm'].astype(str) + ' '
        # add a right space to differentiate 'Inc ' from 'Incenerator'
        df['trade_nm'] = df['trade_nm'].astype(str) + ' '
    return df


def CleanUpAgency(df, COLUMN):
    #use for case number code prefix
    if not df.empty and COLUMN in df.columns:
        # DLSE_terms = ['01','04', '05', '06', '07', '08', '09', '10', '11', '12', '13',
        # '14', '15', '16', '17', '18', '23','32']

        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '01', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '02', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '03', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '04', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '05', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '06', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '07', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '08', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '09', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '10', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '11', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '12', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '13', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '14', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '15', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '16', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '17', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '18', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '19', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '20', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '21', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '22', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '23', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '24', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '25', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            '35', 'DLSE', False, re.IGNORECASE)
        df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace(
            'WC', 'DLSE', False, re.IGNORECASE)

    return df


def CleanNumberColumns(column):
    #column is a 'series'
    from pandas.api.types import is_string_dtype

    if is_string_dtype(column):
        column = column.str.replace("$", "")
        column = column.str.replace(",", "")

    column = column.replace(
        to_replace="\$([0-9,\.]+).*", value=r"\1", regex=True)
    # #column = column.str.strip()
    column = column.replace(' ', '')
    column = column.replace(',','', regex=True)
    column = column.replace("−", "-", regex=True)
    column = column.replace('[)]', '', regex=True)
    column = column.replace('[(]', '-', regex=True)

    column = pd.to_numeric(column, errors='coerce')

    column = column.fillna(0)

    column = column.abs()

    return column


def Cleanup_Number_Columns(df):

    if 'bw_amt' not in df.columns:
        df['bw_amt'] = 0
    if 'ee_violtd_cnt' not in df.columns:
        df['ee_violtd_cnt'] = 0
    if 'ee_pmt_recv' not in df.columns:
        df['ee_pmt_recv'] = 0
    if 'Interest_Payments_Recd' not in df.columns:
        df['Interest_Payments_Recd'] = 0
    if 'cmp_assd_cnt' not in df.columns:
        df['cmp_assd_cnt'] = 0
    if 'violtn_cnt' not in df.columns:
        df['violtn_cnt'] = 0

    df['bw_amt'] = CleanNumberColumns(df['bw_amt'])
    df['ee_violtd_cnt'] = CleanNumberColumns(df['ee_violtd_cnt'])
    df['ee_pmt_recv'] = CleanNumberColumns(df['ee_pmt_recv'])
    df['Interest_Payments_Recd'] = CleanNumberColumns(
        df['Interest_Payments_Recd'])
    df['cmp_assd_cnt'] = CleanNumberColumns(df['cmp_assd_cnt'])

    df['violtn_cnt'] = df['violtn_cnt'].abs()

    return df


def Clean_Summary_Values(DF_OG_VLN):

    DF_OG_VLN['bw_amt'] = CleanNumberColumns(DF_OG_VLN['bw_amt'])
    DF_OG_VLN['violtn_cnt'] = CleanNumberColumns(DF_OG_VLN['violtn_cnt'])
    DF_OG_VLN['ee_violtd_cnt'] = CleanNumberColumns(DF_OG_VLN['ee_violtd_cnt'])
    DF_OG_VLN['ee_pmt_recv'] = CleanNumberColumns(DF_OG_VLN['ee_pmt_recv'])

    return DF_OG_VLN


def FormatNumbersHTMLRow(df):
    if not None:
        df['bw_amt'] = df.apply(
            lambda x: "{0:,.0f}".format(x['bw_amt']), axis=1)
        df['ee_violtd_cnt'] = df.apply(
            lambda x: "{0:,.0f}".format(x['ee_violtd_cnt']), axis=1)
        df['ee_pmt_recv'] = df.apply(
            lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)
        df['violtn_cnt'] = df.apply(
            lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)

    return df


# Flag duplicated backwage values
def FlagDuplicateBackwage(df, FLAG_DUPLICATE):
    if not df.empty and 'case_id_1' in df.columns:

        #Clean
        df['case_id_1'] = df['case_id_1'].str.strip()  #remove spaces

        #Catch clear duplicated cases -- so far to 500k records none found in DLSE or WHD
        df["_DUP_BW"] = df.duplicated( #catch clear duplicated cases
            subset=['case_id_1', 'legal_nm', 'violation', 'bw_amt'], keep=False)
        
        #Catch scenario where $0 payment -- infrequent to have pmt on some violations but not all
        df["_DUP_PMT_REC"] = df.duplicated(
            subset=['case_id_1', 'legal_nm', 'violation', 'ee_pmt_recv'], keep=False)

        #DO NOT USE -- violation is ignored due to a specific error in the dataset-- 
        #only use for visual check
        df["_DUP_PMT_DUE"] = df.duplicated(
            subset=['case_id_1', 'legal_nm', 'bw_amt'], keep=False)
            
    return df


def DropDuplicateRecords(df, FLAG_DUPLICATE, bug_log_csv):

    if not df.empty and 'case_id_1' in df.columns:
        
        #CLEAN
        df = df.astype({'case_id_1': 'str'})
        df['case_id_1'] = df['case_id_1'].str.strip()  # remove spaces

        # DELETE DUPLICTAES 
        # always remove full duplicates
        df = df.drop_duplicates(keep='first')

        df['Sort_$'] = pd.to_numeric(df['bw_amt'], errors='coerce') + pd.to_numeric(df['interest_owed'], errors='coerce')

        #Sort by ['Sort_$','Businesstype'], to place duplicate with $ and the Corp at the top and kept while $0 and Individuals are removed
        ALL_EMPTY_CASE_ID = (df['case_id_1'].isna().all() | (df['case_id_1']=="").all() )
        ALL_EMPTY_VIOLATION = (df['violation'].isna().all() | (df['violation']=="").all() )
        if FLAG_DUPLICATE == 0 and not ALL_EMPTY_CASE_ID and not ALL_EMPTY_VIOLATION: #delete
            df = df.sort_values(['Sort_$','Businesstype'], ascending=[False,False]).drop_duplicates(
                ['case_id_1', 'violation'], keep='first').sort_index()
        else: #flag w/o removal
            df["_DELETED_DUP"] = df.sort_values(['Sort_$','Businesstype'], ascending=[False,True]).duplicated(
                 subset=['case_id_1', 'violation'], keep='first')
        
        # TEST SCENARIO
        # LABEL DUPLICATES REMAINING -- in 500k records these three scenarios have never occured
        #df["_DUP_Person"] = df.duplicated(subset=['Submitter_Email', 'Responsible_Person_Phone'], keep=False)
        df["_Case_X"] = df.duplicated(
            subset=['case_id_1', 'violation', 'bw_amt'], keep=False)
        df["_T_NM_X"] = df.duplicated(
            subset=['case_id_1', 'trade_nm', 'street_addr', 'violation', 'bw_amt'], keep=False)
        df["_L-NM_X"] = df.duplicated(subset=['case_id_1', 'legal_nm',
                                              'street_addr', 'violation', 'bw_amt'], keep=False)

        # DELETE DUPLICTAES REMAINING -- in 500k records these three scenarios have never occured
        if FLAG_DUPLICATE == 0:
            #"_Case_X"
            df = df.drop_duplicates(
                subset=['case_id_1', 'violation', 'bw_amt'], keep='first')
            #"_T_NM_X"
            df = df.drop_duplicates(
                subset=['case_id_1', 'trade_nm', 'street_addr', 'violation', 'bw_amt'], keep='first')
            #"_L-NM_X"
            df = df.drop_duplicates(
                subset=['case_id_1', 'legal_nm', 'street_addr', 'violation', 'bw_amt'], keep='first')

    return df


# aggregated functions*********************************

def Cleanup_Text_Columns(df, bug_log, LOGBUG, bug_log_csv):

    function_name = "Cleanup_Text_Columns"
    
    time_1 = time.time()
    df = ReplaceAddressAbreviations(df)
    # https://pypi.org/project/pyspellchecker/
    df = RemoveDoubleSpacesFromAddresses(df)
    df = RemovePunctuationFromAddresses(df)  # once more
    # https://pypi.org/project/pyspellchecker/
    df = RemoveDoubleSpacesFromAddresses(df)
    time_2 = time.time()
    log_number = "appreviation, punctuation, and spaces"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    time_1 = time.time()
    df = RemovePunctuationFromCity(df)  # once more
    time_2 = time.time()
    log_number = "RemovePunctuationFromCity"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    time_1 = time.time()
    df = StripPunctuationFromNames(df)
    df = RemoveDoubleSpacesFromCompanyName(df)
    df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
    df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
    df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
    df = MoveBusinessTypeToBusinessTypeColumn(df)
    df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
    df = StripPunctuationFromNames(df)
    time_2 = time.time()
    log_number = "Move Business Type"
    append_log(bug_log, LOGBUG, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n")

    """
    # run a second time
    df = RemoveDoubleSpacesFromCompanyName(df)
    df = StripPunctuationFromNames(df)
    df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
    df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
    df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
    df = MoveBusinessTypeToBusinessTypeColumn(df)
    df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
    df = StripPunctuationFromNames(df)

    # run a third time
    df = RemoveDoubleSpacesFromCompanyName(df)
    df = StripPunctuationFromNames(df)
    df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
    df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
    df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
    df = MoveBusinessTypeToBusinessTypeColumn(df)
    df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
    df = StripPunctuationFromNames(df)
    """

    return df


def Signatory_List_Cleanup(df_signatory):

    df_signatory['legal_nm'] = df_signatory['legal_nm'].str.upper()
    df_signatory['street_addr'] = df_signatory['street_addr'].str.upper()
    df_signatory['cty_nm'] = df_signatory['cty_nm'].str.upper()
    df_signatory['st_cd'] = df_signatory['st_cd'].str.upper()

    # deleted all companiy names shorter than 9 letters

    # add missing columns to allow program to go through expected columns
    #df_signatory['legal_nm'] = ""
    #df_signatory['street_addr'] = ""
    #df_signatory['cty_nm'] = ""
    #df_signatory['st_cd'] = ""
    #df_signatory['zip_cd'] = ""

    df_signatory['trade_nm'] = ""
    df_signatory['industry'] = ""
    df_signatory['Prevailing'] = ""
    df_signatory['Signatory'] = ""
    df_signatory['Businesstype'] = ""
    df_signatory['naic_cd'] = ""
    df_signatory['naics_desc.'] = ""

    df_signatory['length'] = df_signatory.legal_nm.str.len()
    company_name_length = 6
    # df_signatory = df_signatory[df_signatory.length > company_name_length] #https://stackoverflow.com/questions/42895061/how-to-remove-a-row-from-pandas-dataframe-based-on-the-length-of-the-column-valu

    df_signatory = StripPunctuationFromNames(df_signatory)
    df_signatory = RemoveDoubleSpacesFromCompanyName(df_signatory)
    df_signatory = MoveCorportationBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MovePartnershipBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MoveBusinessTypeToBusinessTypeColumn(df_signatory)
    df_signatory = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df_signatory)
    df_signatory = StripPunctuationFromNames(df_signatory)

    df_signatory['zip_cd'] = df_signatory['zip_cd'].where(  # remove zipcode suffix #https://stackoverflow.com/questions/44776115/remove-four-last-digits-from-string-convert-zip4-to-zip-code/44776170
        df_signatory['zip_cd'].str.len() == 5,
        df_signatory['zip_cd'].str[:5]
    )

    df_signatory = ReplaceAddressAbreviations(df_signatory)
    df_signatory = RemoveDoubleSpacesFromAddresses(
        df_signatory)  # https://pypi.org/project/pyspellchecker/
    df_signatory = RemovePunctuationFromAddresses(df_signatory)  # once more
    df_signatory = RemoveDoubleSpacesFromAddresses(
        df_signatory)  # https://pypi.org/project/pyspellchecker/

    return df_signatory


def Setup_Regular_headers(df, abs_path, file_name, bug_log_csv):  # DSLE, WHD, etc headers

    df = df.rename(columns={
        
        'trade': 'industry',

        'case_violtn_cnt': 'violtn_cnt',
        
        'Balance Due to Employee(s)': "DNU_Balance_Due_to_Employee(s)",  #in DLSE, this is wrong -- DNU, Do Not Use
        
        'Judgment Total': 'bw_amt_1',
        'bw_atp_amt': 'bw_amt_2',
        'EE(s) Amt Assessed':'bw_amt_3',
        
        #DLSE specific to conform to WHD -- added 10/2/2022
        'Case Number': 'case_id_1',
        'Case #:': 'case_id_2', #redundant scenario
        'Judgment Name': 'case_id_3', #redundant to 'Case Number' scenario only for new DLSE format
        'DIR_Case_Name': 'case_id_4',
        
        'Judgment Status': 'Case Status 3', #redundant scenario only for new DLSE format
        "Closure Disposition": 'Case Status 2',
        "Case Stage": 'Case Status 1',
        
        'Jurisdiction_or_Project_Name': 'juris_or_proj_nm',

        'Account - DBA':'trade_nm_1',
        'Sub Contractor':'trade_nm_2',#redundant scenario
        
        'Employer':'legal_nm_1',
        'legal_name': 'legal_nm_2',
        'Defendant Name':'legal_nm_3', #redundant to 'Employer' scenario only for new DLSE format
        'Defendant/Employer Name':'legal_nm_4', #redundant to 'Employer' scenario only for new DLSE format
        'Prime Contractor':'legal_nm_5', #redundant scenario only for new DLSE format

        'street_addr_1_txt': 'street_addr_1',
        'Primary Address Street':'street_addr_2',
        'Defendant Address':'street_addr_3',

        'Primary Address City': 'cty_nm',
        'Primary Address State': 'st_cd',
        
        'NAICS Code': 'naic_cd',

        'NAICS Code Title': 'naics_desc._1', #'naics_desc.',
        'NAICS Industry': 'naics_desc._2', #redundant to 'NAICS Code Title' scenario only for new DLSE format
        'Industry (NAICS)': 'naics_desc._3', #redundant to 'NAICS Code Title' scenario only for new DLSE format
        'naics_code_description': 'naics_desc._4',

        #'':'case_violtn_cnt',
        #'':'cmp_assd_cnt',
        #'':'ee_violtd_cnt',
        "EE Payments Rec'd":'ee_pmt_recv_1',
        "total_CWPA_amount_collected":'ee_pmt_recv_2', #redundant scenario only for new DLSE format
        'EE_Payments_Received': 'ee_pmt_recv_3',  # causing trouble 11/1/2020

        'Date of Docket':'findings_start_date_1',
        'Date Filed':'findings_start_date_2', #redundant to 'Date of Docket' scenario only for new DLSE format
        'Date Docketed:':'findings_start_date_3', #redundant scenario only for new DLSE format
        'Judgment Entry Date':'findings_start_date_4', #redundant to 'Date of Docket' scenario only for new DLSE format
        
        'Case Closed Date':'findings_end_date_1',
        'Date Closed:':'findings_end_date_2', #redundant scenario only for new DLSE format

        'Interest Balance Due': 'DNU_Interest_Balance', #this is DLSE outdated -- DNU, Do Not Use
        "Interest Payments Rec'd":'Interest_Payments_Recd',

        'Case Issue Type':'violation',
        'DIR Office':'Jurisdiction_region_or_General_Contractor'
    })

    #df.to_csv(os.path.join(abs_path, (file_name+'test_df_3_report').replace(' ', '_') + '.csv'))

    if 'trade_nm' not in df.columns:
        df['trade_nm'] = ""
    if 'legal_nm' not in df.columns:
        df['legal_nm'] = ""
    if 'Businesstype' not in df.columns:
        df['Businesstype'] = ""

    if 'street_addr' not in df.columns:
        df['street_addr'] = ""
    if 'cty_nm' not in df.columns:
        df['cty_nm'] = ""
    if 'st_cd' not in df.columns:
        df['st_cd'] = ""
    if 'zip_cd' not in df.columns:
        df['zip_cd'] = '0' #10/27/2022 change from 0 to False -- then to '0'

    if 'Prevailing' not in df.columns:
        df['Prevailing'] = ""
    if 'Signatory' not in df.columns:
        df['Signatory'] = ""

    if 'industry' not in df.columns:
        df['industry'] = ""
    if 'naic_cd' not in df.columns:
        df['naic_cd'] = ""

    if 'violation' not in df.columns:
        df['violation'] = ""
    if 'violation_code' not in df.columns:
        df['violation_code'] = ""
    if 'ee_violtd_cnt' not in df.columns:
        df['ee_violtd_cnt'] = 0
    if 'violtn_cnt' not in df.columns:
        df['violtn_cnt'] = 0
    if 'bw_amt' not in df.columns:
        df['bw_amt'] = 0

    if 'ee_pmt_recv' not in df.columns:
        df['ee_pmt_recv'] = 0
    if 'interest_owed' not in df.columns:
        df['interest_owed'] = 0
    if 'cmp_assd_cnt' not in df.columns:
        df['cmp_assd_cnt'] = 0
    if 'DNU_Interest_Balance' not in df.columns:
        df['DNU_Interest_Balance'] = 0
    if 'backwage_owed' not in df.columns:
        df['backwage_owed'] = df['bw_amt'] + df['DNU_Interest_Balance'] # <-- could be a problem

    if 'records' not in df.columns:
        df['records'] = 0

    if 'Note' not in df.columns:
        df['Note'] = ""
    if 'juris_or_proj_nm' not in df.columns:
        df['juris_or_proj_nm'] = ""
    if 'Jurisdiction_region_or_General_Contractor' not in df.columns:
        df['Jurisdiction_region_or_General_Contractor'] = ""

    if "Case Stage" not in df.columns:
        df["Case Stage"] = ""
    if "Case Status" not in df.columns:
        df["Case Status"] = ""
    if "Closure Disposition" not in df.columns:
        df["Closure Disposition"] = ""

    if "findings_start_date" not in df.columns:
        df["findings_start_date"] = ""
    if "findings_end_date" not in df.columns:
        df["findings_end_date"] = ""
    if "case_id_1" not in df.columns:
        df["case_id_1"] = ""
    if "naics_desc." not in df.columns:
        df["naics_desc."] = ""

    if ('st_cd' in df.columns) and ('juris_or_proj_nm' in df.columns):   #hack by F.Peterson 4/20/2024

        df['st_cd'] = np.where(
            (
                ((df['juris_or_proj_nm'] == 'DLSE_J-23') | (df['juris_or_proj_nm'] == 'DLSE_J-5413') 
                 | (df['juris_or_proj_nm'] ==  'DLSE_WageClaim') | (df['juris_or_proj_nm'] == 'DIR_DLSE') 
                 | (df['juris_or_proj_nm'] == 'DLSE')  ) & 
                (df['st_cd'].isnull() | (df['st_cd'] == "") ) 
            ),
            'CA', df['st_cd'] )
    
    if 'findings_end_date_1' in df.columns:
        df['findings_end_date'] = np.where((df['findings_end_date'].isna() | (df['findings_end_date']=="")), df['findings_end_date_1'], df['findings_end_date'])
    if 'findings_end_date_2' in df.columns:
        df['findings_end_date'] = np.where((df['findings_end_date'].isna() | (df['findings_end_date']=="")), df['findings_end_date'])

    if 'Case Status 1' in df.columns:
        df['Case Status'] = np.where((df['Case Status'].isna() | (df['Case Status']=="")), df['Case Status 1'], df['Case Status'])
    if 'Case Status 2' in df.columns:  
        df['Case Status'] = np.where((df['Case Status'].isna() | (df['Case Status']=="")), df['Case Status 2'], df['Case Status'])
    if 'Case Status 3' in df.columns:
        df['Case Status'] = np.where((df['Case Status'].isna() | (df['Case Status']=="")), df['Case Status 3'], df['Case Status'])

    df['bw_amt'] = CleanNumberColumns(df['bw_amt'])
    if 'bw_amt_1' in df.columns:
        df['bw_amt_1'] = CleanNumberColumns(df['bw_amt_1'])
        df['bw_amt'] = np.where((df['bw_amt'].isna() | (df['bw_amt']=="") | (df['bw_amt'] < df['bw_amt_1'])), df['bw_amt_1'], df['bw_amt'])
    if 'bw_amt_2' in df.columns:
        df['bw_amt_2'] = CleanNumberColumns(df['bw_amt_2'])
        df['bw_amt'] = np.where((df['bw_amt'].isna() | (df['bw_amt']=="") | (df['bw_amt'] < df['bw_amt_2'])), df['bw_amt_2'], df['bw_amt'])
    if 'bw_amt_3' in df.columns:
        df['bw_amt_3'] = CleanNumberColumns(df['bw_amt_3'])
        df['bw_amt'] = np.where((df['bw_amt'].isna() | (df['bw_amt']=="") | (df['bw_amt'] < df['bw_amt_3'])), df['bw_amt_3'], df['bw_amt'])

    if 'case_id' in df.columns:
        df['case_id_1'] = np.where((df['case_id_1'].isna() |(df['case_id_1']=="")), df['case_id'], df['case_id_1'])
    if 'case_id_2' in df.columns:
        df['case_id_1'] = np.where((df['case_id_1'].isna() |(df['case_id_1']=="")), df['case_id_2'], df['case_id_1'])
    if 'case_id_3' in df.columns:
        df['case_id_1'] = np.where((df['case_id_1'].isna() |(df['case_id_1']=="")), df['case_id_3'], df['case_id_1'])
    if 'case_id_4' in df.columns:
        df['case_id_1'] = np.where((df['case_id_1'].isna() |(df['case_id_1']=="")), df['case_id_4'], df['case_id_1'])

    if 'trade_nm_1' in df.columns:
        df['trade_nm'] = np.where((df['trade_nm'].isna() |(df['trade_nm']=="")), df['trade_nm_1'], df['trade_nm'])
    if 'trade_nm_2' in df.columns:
        df['trade_nm'] = np.where((df['trade_nm'].isna() |(df['trade_nm']=="")), df['trade_nm_2'], df['trade_nm'])

    if 'legal_nm_1' in df.columns:
        df['legal_nm'] = np.where((df['legal_nm'].isna() | (df['legal_nm']=="")), df['legal_nm_1'], df['legal_nm'])
    if 'legal_nm_2' in df.columns:
        df['legal_nm'] = np.where((df['legal_nm'].isna() | (df['legal_nm']=="")), df['legal_nm_2'], df['legal_nm'])
    if 'legal_nm_3' in df.columns:
        df['legal_nm'] = np.where((df['legal_nm'].isna() | (df['legal_nm']=="")), df['legal_nm_3'], df['legal_nm'])
    if 'legal_nm_4' in df.columns:
        df['legal_nm'] = np.where((df['legal_nm'].isna() | (df['legal_nm']=="")), df['legal_nm_4'], df['legal_nm'])
    if 'legal_nm_5' in df.columns:
        df['legal_nm'] = np.where((df['legal_nm'].isna() | (df['legal_nm']=="")), df['legal_nm_5'], df['legal_nm'])
    

    if 'street_addr_1' in df.columns:
        df['street_addr'] = np.where((df['street_addr'].isna() | (df['street_addr']=="")), df['street_addr_1'], df['street_addr'])
    if 'street_addr_2' in df.columns:  
        df['street_addr'] = np.where((df['street_addr'].isna() | (df['street_addr']=="")), df['street_addr_2'], df['street_addr'])
    if 'street_addr_3' in df.columns:  
        df['street_addr'] = np.where((df['street_addr'].isna() | (df['street_addr']=="")), df['street_addr_3'], df['street_addr'])

    if 'naics_desc._1' in df.columns:  
        df['naics_desc.'] = np.where((df['naics_desc.'].isna() | (df['naics_desc.']=="")), df['naics_desc._1'], df['naics_desc.'])
    if 'naics_desc._2' in df.columns:      
        df['naics_desc.'] = np.where((df['naics_desc.'].isna() | (df['naics_desc.']=="")), df['naics_desc._2'], df['naics_desc.'])
    if 'naics_desc._3' in df.columns:      
        df['naics_desc.'] = np.where((df['naics_desc.'].isna() | (df['naics_desc.']=="")), df['naics_desc._3'], df['naics_desc.'])
    if 'naics_desc._4' in df.columns:      
        df['naics_desc.'] = np.where((df['naics_desc.'].isna() | (df['naics_desc.']=="")), df['naics_desc._4'], df['naics_desc.'])

    df['ee_pmt_recv'] = CleanNumberColumns(df['ee_pmt_recv'])
    #(df['ee_pmt_recv'] < df['ee_pmt_recv_1']) # removed
    if 'ee_pmt_recv_1' in df.columns:
        df['ee_pmt_recv_1'] = CleanNumberColumns(df['ee_pmt_recv_1'])
        df['ee_pmt_recv'] = np.where((df['ee_pmt_recv'].isna() | (df['ee_pmt_recv']=="") | (df['ee_pmt_recv']==0) ), df['ee_pmt_recv_1'], df['ee_pmt_recv'])
    if 'ee_pmt_recv_2' in df.columns:
        df['ee_pmt_recv_2'] = CleanNumberColumns(df['ee_pmt_recv_2'])
        df['ee_pmt_recv'] = np.where((df['ee_pmt_recv'].isna() | (df['ee_pmt_recv']=="") | (df['ee_pmt_recv']==0)  ), df['ee_pmt_recv_2'], df['ee_pmt_recv'])
    if 'ee_pmt_recv_3' in df.columns:
        df['ee_pmt_recv_3'] = CleanNumberColumns(df['ee_pmt_recv_3'])
        df['ee_pmt_recv'] = np.where((df['ee_pmt_recv'].isna() | (df['ee_pmt_recv']=="") | (df['ee_pmt_recv']==0) ), df['ee_pmt_recv_3'], df['ee_pmt_recv'])
    if 'ee_pmt_recv_4' in df.columns:
        df['ee_pmt_recv_4'] = CleanNumberColumns(df['ee_pmt_recv_4'])
        df['ee_pmt_recv'] = np.where((df['ee_pmt_recv'].isna() | (df['ee_pmt_recv']=="") | (df['ee_pmt_recv']==0)  ), df['ee_pmt_recv_4'], df['ee_pmt_recv'])

    if 'findings_start_date_1' in df.columns:
        df['findings_start_date'] = np.where((df['findings_start_date'].isna() | (df['findings_start_date']=="")), df['findings_start_date_1'], df['findings_start_date'])
    if 'findings_start_date_2' in df.columns:
        df['findings_start_date'] = np.where((df['findings_start_date'].isna() | (df['findings_start_date']=="")), df['findings_start_date_2'], df['findings_start_date'])
    if 'findings_start_date_3' in df.columns:
        df['findings_start_date'] = np.where((df['findings_start_date'].isna() | (df['findings_start_date']=="")), df['findings_start_date_3'], df['findings_start_date'])
    if 'findings_start_date_4' in df.columns:
        df['findings_start_date'] = np.where((df['findings_start_date'].isna() | (df['findings_start_date']=="")), df['findings_start_date_4'], df['findings_start_date'])

    #df.to_csv(os.path.join(abs_path, (file_name+'test_df_4_report').replace(' ', '_') + '.csv'))
    

    return df


def Define_Column_Types(df):

    # Define column types**********************
    if 'case_id_1' and 'legal_nm' and 'trade_nm' and 'naic_cd' in df.columns:
        df['naic_cd'] = df['naic_cd'].astype(str)
        df['case_id_1'] = df['case_id_1'].astype(str)
        df['legal_nm'] = df['legal_nm'].astype(str)
        df['trade_nm'] = df['trade_nm'].astype(str)
        df['street_addr'] = df['street_addr'].astype(str)
        df['cty_nm'] = df['cty_nm'].str.upper()
        df['st_cd'] = df['st_cd'].astype(str)
        df['zip_cd'] = df['zip_cd'].astype(str)

        df['zip_cd'] = df['zip_cd'].where(  # remove zip code suffix #https://stackoverflow.com/questions/44776115/remove-four-last-digits-from-string-convert-zip4-to-zip-code/44776170
            df['zip_cd'].str.len() == 5,
            df['zip_cd'].str[:5]
        )
        df['zip_cd'] = df['zip_cd'].replace('nan', '0', regex=True)

        if is_string_series(df['Prevailing']):
            df['Prevailing'] = pd.to_numeric(df['Prevailing'], errors='coerce')
        if is_string_series(df["Signatory"]):
            df["Signatory"] = pd.to_numeric(df["Signatory"], errors='coerce')
        
        if is_string_series(df['bw_amt']):
            df['bw_amt'] = pd.to_numeric(
                df['bw_amt'].str.replace(',', '', regex=True) , errors='coerce')
        if is_string_series(df['ee_pmt_recv']):
            df['ee_pmt_recv'] = pd.to_numeric(
                df['ee_pmt_recv'].str.replace(',', '', regex=True) , errors='coerce')
        if is_string_series(df['cmp_assd_cnt']):    
            df['cmp_assd_cnt'] = pd.to_numeric(
                df['cmp_assd_cnt'].str.replace(',', '', regex=True) , errors='coerce')
        if is_string_series(df['interest_owed']):   
            df['interest_owed'] = pd.to_numeric(
                df['interest_owed'].str.replace(',', '', regex=True) , errors='coerce')

        df['legal_nm'] = df['legal_nm'].str.upper()
        df['trade_nm'] = df['trade_nm'].str.upper()
        df['street_addr'] = df['street_addr'].str.upper()
        df['cty_nm'] = df['cty_nm'].str.upper()
        df['st_cd'] = df['st_cd'].str.upper()

    return df

def is_string_series(s : pd.Series): #https://stackoverflow.com/questions/43049545/python-check-if-dataframe-column-contain-string-type
    if isinstance(s.dtype, pd.StringDtype):
        # The series was explicitly created as a string series (Pandas>=1.0.0)
        return True
    elif s.dtype == 'object':
        # Object series, check each value
        return all((v is None) or isinstance(v, str) for v in s)
    else:
        return False

def Signatory_Library():

    hospital_signatories = [['Health_care'], ['El Camino Hospital', 'El Camino Hospital Los Gatos', 'El Camino HospitalLos Gatos',
                                              'VA Palo Alto Health Care System', 'OConner Hospital', 'Santa Clara Valley Medical Center', 'Good Samaritan Hospital',
                                              'El Camino Hospital Mountain View', 'El Camino HospitalMountain View', 'El Camino Hospital Mountain View',
                                              'Lucile Packard Childrens Hospital', 'LPC Hospital', 'Kaiser Permanente San Jose Medical Center', 'Regional Medical Center of San Jose',
                                              'Kaiser Permanente Hospital', 'Kaiser Permanente Santa Clara Medical Center', 'Kaiser Permanente', 'Kaiser Permanente Medical Center',
                                              'Saint Louise Regional Hospital', 'Saint Louise Hospital', 'Stanford University Hospital', 'Stanford Hospital']]

    construction_signatories = [['Construction'], ["Granite Construction", "A Ruiz Construction", "Central Fence", r"\bQLM\b",
                                                   "Otis Elevator ", "United Technologies", "Kiewit Pacific", "West Valley Construction", r"\bFerma\b", "TEICHERT CONSTRUCTION",
                                                   "Alliance Roofing", "Northern Underground Construction", "Albanese", "Vance Brown", "William ONeill Lath and Plastering",
                                                   "El Camino Paving"]]

    signatories_UCON = [['Construction'], ["Yerba Buena Engineering", "WoodruffSawyer", "WMA Landscape Construction", "William A Guthridge", "Whiteside Concrete Construction", "Westside Underground Pipe", "Westland Contractors",
                                           "Western Traffic Supply", "Western States Tool", "Western Stabilization", "West Valley Construction", "West Coast Sand and Gravel", "Wayne E Swisher Cement Contractor", "Walter C Smith", "Walsh Construction",
                                           "Waller", "W R Forde Associates", "W C Maloney", "W Bradley Electric", "W Contracting", "Vulcan Materials", "Vulcan Construction and Maintenance", "Volvo Construction Equipment", "Vintage Paving",
                                           "Viking Drillers", "Viking Construction", "Veteran Pipeline Construction", "Varela", "Vanguard Construction", "Valverde Construction", r"\bValentine\b", "United Rentals", "Underwater Resources", "Underground Construction",
                                           "Trunxai Construction", "Troutman Sanders", "TriWest Tractor", "TriValley Excavating", "Trinet Construction", "Trench Shoring", "Trench Plate Rental", "Trench and Traffic Supply", "Traffic Management", "Tracy Grading and Paving",
                                           "TPR Traffic Solutions", "Total Traffic Control", "Tony Skulick", "Tom McCready", "Thomas Lum", "The Hartford", "The Guarantee Company of North America", "The Construction Zone", "TerraCon Constructors", "Tennyson Electric",
                                           "Teichert Waterworks", "Teichert Utilities", "Teichert Solar", "Teichert Pipelines", "Teichert", "Team Ghilotti", "TBC Safety", "Talus Construction", "Taber Construction", "TDW Construction", "T and S Construction", "Syar Industries",
                                           "Sweeney Mason", "Super Seal and Stripe", "Sunbelt Rentals", "Sukut Construction", "Suarez and Munoz Construction", "Sturgeon Electric California", "Striping Graphics", "Stormwater Specialists", "Storm Water Inspection and Maint Svcs", r"\bSWIMS\b",
                                           r"\bStomper\b", "Stoloski and Gonzalez", "Stevenson Supply", "Stevens Creek Quarry", "Steve P Rados", "Steelhead Constructors", "Stacy and Witbeck", "St Francis Electric", "SPSG Partners", "Sposeto Engineering", "SpenCon Construction",
                                           "Sonsray Machinery", "SMTD Law", "Smith Denison Construction", "Smith Currie and Hancock", "SITECH NorCal", "Sinclair General Engineering Construction", "Silverado Contractors", "Silvas Pipeline", "Sierra Traffic Markings",
                                           "Sierra Mountain Construction", "Shimmick Construction", "Sherry Montoya", "Shaw Pipeline", "Sharon Alberts", "Seyfarth Shaw", "Serafix Engineering Contractors", "Security Shoring and Steel Plate", "Security Paving",
                                           "Schembri Construction", "SANDIS Civil Engineers Surveyors Planners", "Sanco Pipelines", "S and S Trucking", "Ryan Engineering", "Rutan and Tucker", "Rupert Construction Supply", "Royal Electric", "Rosie Garcia", "Rosendin Electric",
                                           "Rogers Joseph ODonnell", "Robust Network Solutions", "Robert Burns Construction", "Robert A Bothman Construction", "Roadway Construction", "Road Machinery", "RNR Construction", "Rinker MaterialsConcrete Pipe Division", "RGW Equipment Sales",
                                           "RGW Construction", "Revel Environmental Manufacturing", r"\bREM\b", "Reliable Trucking", "Reed and Graham", "Redgwick Construction", "Rebel Equipment Enterprises", "RDOVermeer", "RDO Integrated Controls", "RCI General Engineering",
                                           "RC Underground", "Rays Electric", r"\bRansome\b", "Ranger Pipelines", "Ramos Oil", "RAM Rick Albert Machinery", "Rain for Rent", "Rafael De La Cruz", r"\bRM Harris\b", "RJ Gordon Construction", "RC Fischer",
                                           "RA Nemetz Construction", "R E Maher", "RandS Construction Management", r"\bRandB\b", "R and W Concrete Contractors", "R and R Maher Construction", "R and B Equipment", r"\bQLM\b", "Proven Management", "Preston Pipelines",
                                           "Prestige Printing and Graphics", "Precision Engineering", "Precision Drilling", "Power One", "Power Engineering Construction", "Poms Landscaping", "PMK Contractors", "Platinum Pipeline", "PJs Rebar", "PIRTEK San Leandro",
                                           "Petrinovich Pugh", "Peterson Trucks", "Peterson Cat", "Peter Almlie", "Performance Equipment", "Penhall", "Pedro Martinez", "Pavement Recycling Systems", "Paul V Simpson", "Pape Machinery",
                                           "Pacific International Construction", "Pacific Infrastructure Const", "Pacific Highway Rentals", "Pacific Excavation", "Pacific Coast General Engineering", "Pacific Coast Drilling", "Pacific Boring", "PACE Supply",
                                           "P C and N Construction", "P and F Distributors", "Outcast Engineering", "Org Metrics", "OnSite Health and Safety", "Oldcastle Precast", "Oldcastle Enclosure Solutions", "OGrady Paving", "Odyssey Environmental Services",
                                           "Oak Grove Construction", "OC Jones and Sons", "Northwest Pipe", "NorCal Concrete", "Nor Cal Pipeline Services", "NixonEgli Equipment", "Nevada Cement", "Neary Landscape", "Navajo Pipelines",
                                           "National Trench Safety", "National Casting", "Nada Pacific", "Mozingo Construction", "Mountain F Enterprises", "Mountain Cascade", "Moss Adams", "Moreno Trenching", "Mobile Barriers MBT1", "MK Pipelines",
                                           "MJG Constructors", "Mitchell Engineering", "Mission Constructors", "Mission Clay Products", "MinervaGraniterock", "Minerva Construction", "Mike Brown Electric", "Midstate Barrier", r"\bMichels\b", "McSherry and Hudson",
                                           "MCK Services", "McInerney and Dillon PC", "McGuire and Hester", "Martin General Engineering", "Martin Brothers Construction", "Marques Pipeline", "Marinship Development Interest", "Marina Landscape", "Malcolm International",
                                           "Main Street Underground", "Maggiora and Ghilotti", "MF Maher", "Hernandez Engineering", "M Squared Construction", "M and M Foundation and Drilling", "Luminart Concrete", "Lorang Brothers Construction", "Long Electric",
                                           "Lone Star Landscape", "Liffey Electric", "Liberty Contractors", "Leonidou and Rosin Professional", "Lehigh Hanson", "LeClairRyan", "Last and Faoro", "Las Vegas Paving", "Landavazo Bros", "Labor Services", "Knife River Construction",
                                           "Kerex Engineering", "Kelly Lynch", "KDW Construction", "Karen Wonnenberg", "KJ Woods Construction", r"\bJS Cole\b", "Joseph J Albanese", "Jon Moreno", "Johnston, Gremaux and Rossi", "John S Shelton", "Joe Sostaric",
                                           "Joe Gannon", "JMB Construction", "JLM Management Consultants", "Jimni Rentals", "Jifco", "Jensen Precast", "Jensen Landscape Contractor", "Jeff Peel", "JDB and Sons Construction", "JCC", "James J Viso Engineering", "JAM Services",
                                           "JM Turner Engineering", "JJR Construction", "J Mack Enterprises", "J Flores Construction", "J D Partners Concrete", "J and M", "IronPlanet", "Interstate Grading and Paving", "Interstate Concrete Pumping",
                                           "Integro Insurance Brokers", "Innovate Concrete", "Inner City Demolition", "Industrial Plant Reclamation", "Independent Structures", "ICONIX Waterworks", r"\bHoseley\b", "Horizon Construction", "Hess Construction",
                                           "Harty Pipelines", "Harris Blade Rental", "Half Moon Bay Grading and Paving", "HandE Equipment Services", "Guy F Atkinson Construction", "GSL Construction", "Griffin Soil Group", "Graniterock", "Granite Construction",
                                           "Gordon N Ball", "Goodfellow Bros", "Gonsalves and Santucci", "The Conco Companies", "Golden Gate Constructors", "Golden Bay Construction", "Goebel Construction", "Gilbertson Draglines", "Ghilotti Construction", "Ghilotti Bros",
                                           "GECMS/McGuire and Hester JV", "Garney Pacific", "Gallagher and Burk", "G Peterson Consulting Group", "Fox Loomis", "Forterra", "Ford Construction", "Fontenoy Engineering", "Florez Paving", "Flatiron West", "Fisher Phillips",
                                           "Fine Line Sawing and Drilling", "Fermin Sierra Construction", r"\bFerma\b", "Ferguson Welding Service", "Ferguson Waterworks", "Farwest Safety", "Evans Brothers", "Esquivel Grading and Paving", "Enterprise Fleet Management",
                                           "Eighteen Trucking", "Economy Trucking", "Eagle Rock Industries", "EE Gilbert Construction", "Dynamic Office and Accounting Solutions", "Dutch Contracting", "Duran Construction Group", "Duran and Venables", "Druml Group",
                                           "Drill Tech Drilling and Shoring", "Doyles Work", "Downey Brand", "Dorfman Construction", "DMZ Transit", "DMZ Builders", "DLine Constructors", "Dixon Marine Services", "Ditch Witch West", "Disney Construction",
                                           "DirtMarket", "DHE Concrete Equipment", "DeSilva Gates Construction", "Demo Masters", "Dees Burke Engineering Constructors", "Debbie Ferrari", "De Haro Ramirez Group", "DDM Underground", "D'Arcy and Harty Construction",
                                           "DW Young Construction", "DP Nicoli", "DA Wood Construction", "D and D Pipelines", "Cushman and Wakefield", "Cratus Inc", "County Asphalt", "Corrpro Companies", "Corix Water Products", "Core and Main LP", "Cooper Engineering",
                                           "Contractor Compliance", "Construction Testing Services", "ConQuest Contractors", "CondonJohnson and Associates", "Concrete Demo Works", "ConcoWest", "Compass Engineering Contractors", "Command Alkon", "Columbia Electric",
                                           "CMD Construction Market Data", "CMC Construction", "Clipper International Equipment", "Champion Contractors", "Central Striping Service", "Central Concrete Supply", "Centerline Striping", "Carpenter Rigging", r"\bCarone\b",
                                           r"\bCampanella\b", "CalSierra Pipe", "California Trenchless", "California Portland Cement", "California Engineering Contractors", "Cal State Constructors", "Cal Safety", "CF Archibald Paving", "CandN Reinforcing", "Burnham Brown",
                                           "Bugler Construction", "Bruce Yoder", "Bruce Carone Grading and Paving", "Brosamer and Wall", "BrightView Landscape Development", "Bridgeway Civil Constructors", "Brianne Conroy", "Brian Neary", "Brendan Coyne", r"\bBolton\b", r"\bBob Heal\b",
                                           "BlueLine Rental", "Blue Iron Foundations and Shoring", "Blaisdell Construction", "Bill Crotinger", "Bertco", "Berkeley Cement", "Bentancourt Bros Construction", "Beliveau Engineering Contractors", "Bear Electrical Solutions",
                                           "Bayside Stripe and Seal", "Bay Pacific Pipeline", "Bay Line Cutting and Coring", "Bay Cities Paving and Grading", "Bay Area Traffic Solutions", "Bay Area Concretes", "Bay Area Barricade Service", "Bay Area Backhoes",
                                           "Bauman Landscape and Construction", "Badger Daylighting", "B and C Asphalt Grinding", "Azul Works", "AWSI", "AVAR Construction", "Atlas Peak Construction", "Atkinson", "Argonaut Constructors", "Argent Materials",
                                           "Arcadia Graphix and Signs", "Appian Engineering", "Apex Rents", "APB General Engineering", "Aon Construction Services Group", "Anvil Builders", r"\bAnrak\b", "Andreini Brothers", r"\bAndreini\b", "Andes Construction",
                                           "AMPCO North", "American Pavement Systems", "Alex Moody", "AJW Construction", "Advanced Stormwater Protection", "Advanced Drainage Systems", "Adrian Martin", "A and B Construction"]]

    signatories_CEA = [['Construction'], ["Alcal Specialty Contracting", "Alten Construction", r"\bOveraa\b", "Cahill Contractors", "Clark Construction", "Clark Pacific", "Dolan Concrete Construction", "Dome Construction", "DPR Construction",
                                          "Gonsalves and Stronck Construction", "Hathaway Dinwiddie Construction", "Howard Verrinder", "Obayashi", "Lathrop Construction", "McCarthy Building", "Nibbi Bros Associates", "Peck and Hiller", "Roebbelen Contracting",
                                          "Roy Van Pelt", "Rudolph and Sletten", "SJ Amoroso Construction", "Skanska", "Suffolk Construction", "Swinerton Builders", "Thompson Builders", "Webcor Builders", "XL Construction", "Rosendin Electric", "Boss Electric",
                                          "Cupertino Electric", 'Beltramo Electric', 'The Best Electrical', 'CH Reynolds Electric', 'Cal Coast Telecom', 'Comtel Systems Technology', 'Cupertino Electric', 'CSI Electrical Contractors',
                                          'Delgado Electric', 'Elcor Electric', 'Friel Energy Solutions', 'ICS Integrated Comm Systems', 'Intrepid Electronic Systems', 'MDE Electric', 'MidState Electric', 'Pacific Ridge Electric',
                                          'Pfeiffer Electric', 'Radiant Electric', 'Ray Scheidts Electric', 'Redwood Electric Group', 'Rosendin Electric', 'Sanpri Electric', 'Sasco Electric', 'Selectric Services', 'San Jose Signal Electric',
                                          'Silver Creek Electric', 'Splicing Terminating and Testing', 'Sprig Electric', 'TDN Electric', 'TL Electric', r'\bTherma\b', 'Thermal Mechanical', 'Don Wade Electric', 'ABCO Mechanical Contractors',
                                          'ACCO Engineered Systems', 'Air Conditioning Solutions', 'Air Systems Service and Construction', 'Air Systems', 'Airco Mechanical', 'Allied Heating and AC', 'Alpine Mechanial Service', 'Amores Plumbing',
                                          'Anderson Rowe and Buckley', 'Applied Process Cooling', 'Arc Perfect Solutions', 'Axis Mechanicals', 'Ayoob and Peery', 'Ayoob Mechanical', 'Bacon Plumbing', 'Bay City Mechanical', 'Bayline Mechancial',
                                          'Bell Products', 'Bellanti Plumbing', 'Booth Frank', 'Brady Air Conditioning', 'Broadway Mechanical Contractors', 'Brothers Energy', 'Cal Air', 'Cal Pacific Plumbing Systems', r'\bCARRIER\b',
                                          'City Mechanical', 'CNS Mechanical', 'Cold Room Solutions', 'Comfort Dynamics', 'Commerical Refrigeration Specialist', 'Cool Breeze Refrigeration', 'Critchfield Mechanical', 'Daikin Applied', 'Daniel Larratt Plumbing',
                                          'Desert Mechanical', 'Done Rite Plumbing', 'Dowdle Andrew and Sons', r'\bDPW\b', 'Egan Plumbing', 'Emcor Services', 'Mesa Energy', 'Envise', 'Estes Refrigeration', 'Green Again Landscaping and Concrete', 'Hickey W L Sons',
                                          'Johnson Controls M54', 'KDS Plumbing', 'KEP Plumbing', 'Key Refrigeration', 'Kinectics Mechanical Services', 'KMC Plumbing', 'KOH Mechanical Contractors', r'\bKruse L J\b', 'Larratt Bros Plumbing',
                                          'Lawson Mechanical Contractors', r'\bLescure\b', 'LiquiDyn', 'Marelich Mechanical', 'Masterson Enterprises', 'Matrix HG', 'McPhails Propane Installation', r'\bMcPhails\b', r'\bMitchell E\b', 'Monterey Mechanical',
                                          'MSR Mechanical', 'Murray Plumbing and Heating', 'OC McDonald', 'OBrien Mechanical', 'OMNITemp Refrigeration', 'Pacific Coast Trane', 'PanPacific Mechanical', 'Peterson Mechanical',
                                          r'\bPMI\b', 'POMI Mechanical', 'Pribuss Engineering', 'Quest Mechanical', 'RG Plumbing', 'Redstone Plumbing', 'Refrigeration Solutions', 'Reichel', 'C R Engineering', 'Rigney Plumbing',
                                          'Rountree Plumbing and Heating', 'S and R Mechanical', 'Schram Construction', 'Southland Industries', 'Spencer F W and Sons', 'Temper Insulation', 'Therma', 'Thermal Mechanical', 'United Mechanical',
                                          'Valente A and Sons', 'Westates Mechanical', 'Western Allied Mechanical', 'White Water Plumbing', 'Blues roofing']]

    SIGNATORIES = [['All_SIGNATORIES'], hospital_signatories,
                   signatories_CEA, signatories_UCON, construction_signatories]

    return SIGNATORIES


def Read_Violation_Data(TEST_CASES, url, out_file_report, trigger, bug_log_csv, abs_path, file_name):

    df_csv = pd.DataFrame()
    df_csv = read_from_url(url, TEST_CASES, trigger)

    df_csv['juris_or_proj_nm'] = out_file_report
    df_csv = Setup_Regular_headers(df_csv, abs_path, file_name, bug_log_csv)

    save_backup_to_folder(df_csv, out_file_report + '_backup', "csv_read_backup/") #greedy backukup even if already exists

    return df_csv


def save_backup_to_folder(df_csv, out_file_report_name = 'backup_', out_file_report_path = ""):
    from os.path import exists
    import os

    if out_file_report_path == "": out_file_report_path = out_file_report_path + '_backup/' #defualt folder name
    script_dir = os.path.dirname(os.path.dirname(__file__))
    abs_path = os.path.join(script_dir, out_file_report_path)
    if not os.path.exists(abs_path):  # create folder if necessary
        os.makedirs(abs_path)
    file_type = '.csv'
    
    file_name_backup = os.path.join(abs_path, (out_file_report_name).replace('/', '') + file_type)  # <-- absolute dir and file name
    df_csv.to_csv(file_name_backup) #uncomment for testing


def read_from_local(path, TEST_CASES):
    #df_csv = pd.read_csv(path, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
    df_csv = pd.read_csv(path, encoding = 'utf8', low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
    return df_csv 



def read_from_url(url, TEST_CASES, trigger):
    req = requests.get(url)
    buf = io.BytesIO(req.content)
    if url[-3:] == 'zip':
        if trigger:
            df_csv = pd.read_csv(buf, compression='zip', low_memory=False,
                             thousands=',', encoding="ISO-8859-1", sep=',', nrows=TEST_CASES, on_bad_lines="skip")
        else:
            df_csv = pd.read_csv(buf, compression='zip', low_memory=False,
                             thousands=',', encoding='utf8', sep=',', nrows=TEST_CASES, on_bad_lines="skip")

    else:
        if trigger:
            df_csv = pd.read_csv(buf, low_memory=False, thousands=',', 
                                 encoding="ISO-8859-1", sep=',', nrows=TEST_CASES, on_bad_lines="skip")
        else:
            df_csv = pd.read_csv(buf, low_memory=False, thousands=',', 
                                 encoding='utf8', sep=',', nrows=TEST_CASES, on_bad_lines="skip")
        
    return df_csv

def read_csv_from_url(url):
    df = pd.DataFrame()

    req = requests.get(url)
    buf = io.BytesIO(req.content)

    df = pd.read_csv(buf, low_memory=False, encoding='utf8', sep=',', on_bad_lines="skip")

    return df


def Title_Block(TEST, DF_OG_VLN, DF_OG_ALL, target_jurisdition, TARGET_INDUSTRY, prevailing_wage_report, federal_data, \
                includeStateCases, includeStateJudgements, target_organization, open_cases_only, textFile):
    
    scale = ""
    if open_cases_only:
        scale = "Unpaid"
    else:
        scale = "Total"

    textFile.write(
        f"<h1>DRAFT REPORT: {scale} Wage Theft in the Jurisdiction of {target_jurisdition} for {TARGET_INDUSTRY[0][0]} Industry</h1> \n")
    if prevailing_wage_report == 1:
        textFile.write(
            f"<h2 align=center>***PREVAILING WAGE REPORT***</h2> \n")
    if (federal_data == 1) and ((includeStateCases and includeStateJudgements) == 0):
        textFile.write(
            f"<h2 align=center>***FEDERAL DOL WHD DATA ONLY***</h2> \n")  # 2/5/2022
    if federal_data == 0 and ((includeStateCases or includeStateJudgements) == 1):
        textFile.write(
            f"<h2 align=center>***CA STATE DLSE DATA ONLY***</h2> \n")
    if len(target_organization) > 3:
        textFile.write(f"<h2 align=center> ORGANIZATION SEARCH </h2> \n")
        textFile.write(f"<h2 align=center> {target_organization} </h2> \n")  
    
    textFile.write("\n")

    # all data summary block
    if TEST != 3:
        plural = ""
        if (federal_data == 1) and (includeStateCases == 0) and (includeStateJudgements) == 0:
            plural = "the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments)"
        if (federal_data == 0) and (includeStateCases == 1) and (includeStateJudgements) == 0:
            plural = "the Division of Labor Standards Enforcement Wage Claim Adjudications"
        if (federal_data == 0) and (includeStateCases == 0) and (includeStateJudgements == 1):
            plural = "the Division of Labor Standards Enforcement judgments"

        if (federal_data == 1) and (includeStateCases == 1) and (includeStateJudgements == 0):
            plural = "a combination of the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments) and the Division of Labor Standards Enforcement Wage Claim Adjudications"
        if (federal_data == 1) and (includeStateCases == 0) and (includeStateJudgements == 1):
            plural = "a combination of the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments) and the Division of Labor Standards Enforcement judgments"
        if (federal_data == 0) and (includeStateCases == 1) and (includeStateJudgements == 1):
            plural = "a combination of the Division of Labor Standards Enforcement Wage Claim Adjudications and \
                        the Division of Labor Standards Enforcement judgments"
        if (federal_data == 1) and (includeStateCases == 1) and (includeStateJudgements == 1):
            plural = "a combination of the Division of Labor Standards Enforcement Wage Claim Adjudications, \
                the Division of Labor Standards Enforcement Wage Claim Adjudications, \
                    and the Division of Labor Standards Enforcement judgments"
            
        source_fed = ""
        if (federal_data == 1):
            source_fed = "WHD data were obtained from the DOL"
        source_state = ""
        if (includeStateCases == 1) or (includeStateJudgements == 1):
            source_state = "DLSE data pre-2020 were obtained through a Section 6250 CA Public Records Act request \
                       (does not include purged cases which are those settled and then purged typically after three years), \
                       and then post-2020 DLSE data are from the CA DIR websearch portal"
        tense = ""
        if (federal_data == 1) and ((includeStateCases == 1) or (includeStateJudgements == 1)):
            tense = ", and the "
        
        textFile.write(f"<p>These data are {plural}. The {source_fed} {tense} {source_state}.</p>")
        
    textFile.write("\n")

    textFile.write(
        "<p>The dataset in this report is pulled from a larger dataset that for all regions and sources contains ")
    textFile.write(str.format('{0:,.0f}', DF_OG_ALL['case_id_1'].size))
    textFile.write(" cases")

    if not DF_OG_VLN['violtn_cnt'].sum() == 0:
        textFile.write(", ")
        textFile.write(str.format('{0:,.0f}', DF_OG_VLN['violtn_cnt'].sum()))
        textFile.write(" violations")

    if not DF_OG_ALL['ee_violtd_cnt'].sum() == 0:
        textFile.write(", ")
        textFile.write(str.format(
            '{0:,.0f}', DF_OG_ALL['ee_violtd_cnt'].sum()))
        textFile.write(" employees")

    if not DF_OG_VLN['bw_amt'].sum() == 0:
        textFile.write(", and  $ ")
        textFile.write(str.format('{0:,.0f}', DF_OG_VLN['bw_amt'].sum()))
        textFile.write(" in backwages")

    # <--i have no idea, it works fin above but here I had to do this 11/3/2020
    test_sum = DF_OG_VLN['ee_pmt_recv'].sum()
    if not test_sum == 0:
        textFile.write(", and  $ ")
        textFile.write(str.format('{0:,.0f}', test_sum))
        textFile.write(" in restituted backwages")

    textFile.write(".")

    from datetime import datetime
    textFile.write(" This is approximately a ")
    DF_MIN_ALL = min(pd.to_datetime(
        DF_OG_ALL['findings_start_date'].dropna(), errors='coerce'))
    DF_MAX_ALL = max(pd.to_datetime(
        DF_OG_ALL['findings_start_date'].dropna(), errors='coerce'))
    DF_MAX_ALL_YEARS = (DF_MAX_ALL - DF_MIN_ALL).days / 365

    textFile.write(str.format(
        '{0:,.2f}', ((DF_OG_ALL['bw_amt'].sum()/DF_MAX_ALL_YEARS)/22000000000)*100))
    textFile.write("-percent sample of an estimated actual $2B annually in wage theft that occurs Statewide (see courts.ca.gov/opinions/links/S241812-LINK1.PDF#page=11).</p>")

    textFile.write("\n")

    if TEST != 3:

        fed_range = ""
        state_range = ""
        if (federal_data == 1):
            fed_range = "total Federal WHD dataset goes back to 2000"

        if (includeStateCases == 1) or (includeStateJudgements == 1):
            state_range = "total State DLSE dataset goes back to 2000 (Note: The State purged old closed cases and thus the above is an imperfect ratio)"

        tense = ""
        if (federal_data == 1) and ((includeStateCases == 1) or (includeStateJudgements == 1)):
            tense = ", and "


        textFile.write(f"<p>The {fed_range} {tense} {state_range}.</p>")

    textFile.write("<p> These data are internally incomplete, and do not include private lawsuits, stop notices, and complaints to the awarding agency, contractor, employment department, licensing board, and district attorney. ")
    textFile.write("Therefore, the following is a sample given the above data constraints and the reluctance by populations to file wage and hour claims.</p>")

    textFile.write("\n")
    textFile.write("\n")


def City_Summary_Block_4_Zipcode_and_Industry(df, df_max_check, TARGET_INDUSTRY, SUMMARY_SIG, filename):

    result = '''
	<html>
	<head>
	<style>

		h2 {
			text-align: center;
			font-family: Helvetica, Arial, sans-serif;
		}
		
	</style>
	</head>
	<body>
	'''

    # zip code = loop through
    df = df.reset_index(level=0, drop=True)  # drop city category

    #df = df.groupby(level=0)
    # df = df.agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
    # 	"bw_amt":'sum',
    # 	"violtn_cnt":'sum',
    # 	"ee_violtd_cnt":'sum',
    # 	"records": 'sum',
    # 	}).reset_index().sort_values(["zip_cd"], ascending=True)
    zipcode = ""
    for zipcode, df_zipcode in df.groupby(level=0):  # .groupby(level=0):

        # print theft level in $ and employees
        test_num1 = pd.to_numeric(
            df_zipcode['bw_amt'].sum() )
        test_num2 = pd.to_numeric(
            df_zipcode['ee_violtd_cnt'].sum() )

        if test_num1 < 3000:
            # result +="<p> has no backwage data.</p>""
            dummy = ""  # just does nothing
        else:
            result += "<p>"
            result += "In the "
            result += str(zipcode) #10/26/2022 found bug "can only concatenate str (not "bool") to str"
            result += " zip code, "
            result += (str.format('{0:,.0f}', test_num2))
            if math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
                result += " worker suffered wage theft totaling $ "
            else:
                result += " workers suffered wage theft totaling $ "

            result += (str.format('{0:,.0f}', test_num1))
            result += " "

            # print the industry with highest theft

            # check df_max_check for industry with highest theft in this zip code
            df_zipcode = df_zipcode.reset_index(
                level=0, drop=True)  # drop zip code category
            df_zipcode = df_zipcode.groupby(level=0)

            df_zipcode = df_zipcode.agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
                "bw_amt": 'sum',
                "violtn_cnt": 'sum',
                "ee_violtd_cnt": 'sum',
                "ee_pmt_recv": 'sum',
                "records": 'sum',
            }).reset_index().sort_values(["bw_amt"], ascending=False)

            # check df_max_check that industry in the top three for the County
            industry_list = len(TARGET_INDUSTRY)
            if industry_list > 2:
                result += ". The "
                result += df_zipcode.iloc[0, 0]
                result += " industry is one of the largest sources of theft in this zip code"
                # if math.isclose(df_zipcode.iloc[0,1], df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
                #result += " (this zip code has one of the highest levels of theft in this specific industry across the County)"
                # result += str.format('{0:,.0f}', df_zipcode.iloc[0,1])
                # result += "|"
                # result += str.format('{0:,.0f}', df_max_check['bw_amt'].max() )

            # note if this is the top 3 zip code for the county --> df_max_check
            # if test_num1 == df_max_check['bw_amt'].max():
            if math.isclose(test_num1, df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
                result += " (this zip code has one of the highest overall levels of theft in the County)."
                # result += str.format('{0:,.0f}', test_num1)
                # result += "|"
                # result += str.format('{0:,.0f}',df_max_check['bw_amt'].max() )
            else:
                result += ". "
            result += "</p>"
            result += ("\n")

    result += ("\n")
    result += ("\n")

    result += '''
		</body>
		</html>
		'''
    # with open(filename, mode='a') as f:
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)


def City_Summary_Block(city_cases, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, cty_nm, SUMMARY_SIG, filename):

    result = '''
		<html>
		<head>
		<style>

			h2 {
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
			}
			
		</style>
		</head>
		<body>
		'''
    result += '\n'
    result += '<h2>'
    result += cty_nm
    result += ' CITY SUMMARY</h2>\n'

    # if not df.empty: #commented out 10/26/2020 to remove crash on findings_start_date
    # 	DF_MIN = min(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
    # 	DF_MAX = max(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
    # 	result += ( DF_MIN.strftime("%m/%d/%Y") )
    # 	result += (" to ")
    # 	result += ( DF_MAX.strftime("%m/%d/%Y") )
    # 	result += ("</p> \n")
    # else:
    # 	result += ( "<p>Actual date range: <undefined></p> \n")

    result += "<p>"
    test_num1 = pd.to_numeric(df['bw_amt'].sum() )
    # if test_num1 > 3000:
    # 	result +="Wage theft is a concern in the City of "
    # 	result += cty_nm.title()
    # 	result +=". "

    if city_cases < 1:
        do_nothing = ""
        # result +="No wage theft cases were found in the City of "
        # result += cty_nm.title()
        # result +=". "
    elif math.isclose(city_cases, 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "There is at least one wage theft case"
        if test_num1 <= 3000:
            result += " in the City of "
            result += cty_nm.title()
        result += ", "
    else:
        result += "There are "
        result += (str.format('{0:,.0f}', city_cases))
        result += " wage theft cases"
        if test_num1 <= 3000:
            result += " in the City of "
            result += cty_nm.title()
        result += ", "

    # total theft $
    #test_num1 = pd.to_numeric(df['bw_amt'].sum() )
    if test_num1 < 1 and city_cases < 1:
        do_nothing = ""
        #result +=" and, there is no backwage data. "
    elif test_num1 < 1 and city_cases >= 1:
        result += " however, the backwage data is missing. "
    elif test_num1 > 3000:
        result += " resulting in a total of $ "
        result += (str.format('{0:,.0f}', test_num1))
        result += " in stolen wages. "
    else:
        result += " resulting in stolen wages. "

    # total unpaid theft $
    test_num0 = pd.to_numeric(df['ee_pmt_recv'].sum() )
    if test_num0 < 1:
        do_nothing = ""
        #result +="Of that, an unknown amount is still due to the workers of this city. "
    else:
        result += ("Of that, $ ")
        result += (str.format('{0:,.0f}', test_num0))
        result += " has been returned to the workers of this city. "

    # total number of violations
    test_num2 = pd.to_numeric(df['ee_violtd_cnt'].sum() )
    if test_num2 < 1:
        do_nothing = ""
        #result +="Therefore, there is no case evidence of workers affected by stolen wages. "
    else:
        test_num3 = pd.to_numeric(df['violtn_cnt'].sum() )
        if math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
            result += "The theft comprises at least one discrete wage-and-hour violation "
        else:
            result += "The theft comprises "
            result += (str.format('{0:,.0f}', test_num3))
            result += " discrete wage-and-hour violations "

    if test_num2 < 1:
        do_nothing = ""
    elif math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "affecting at least one worker: "
    else:
        result += "affecting "
        result += (str.format('{0:,.0f}', test_num2))
        result += " workers: "

    # xx companies have multiple violations
    if len(unique_legalname.index) < 1:
        do_nothing = ""
        #result +="No employer was found with more than one case. "
    if math.isclose(len(unique_legalname.index), 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "At least one employer has multiple cases. "
    else:
        result += (str.format('{0:,.0f}', len(unique_legalname.index)))
        result += " employers have multiple cases recorded against them. "

    # xx companies cited by multiple agencies
    if len(agency_df.index) < 1:
        #result +="No employer was found with cases from multiple agencies. "
        do_nothing = ""
    elif math.isclose(len(agency_df.index), 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "at least one employer has cases from multiple agencies. "
    else:
        result += (str.format('{0:,.0f}', len(agency_df.index)))
        result += " employers have cases from multiple agencies. "

    # employer with top theft
    if test_num1 > 3000:
        # df = df.droplevel('legal_nm').copy()
        # df = df.reset_index()
        # df = df.groupby(['legal_nm']).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
        # 	"bw_amt":'sum',
        # 	"violtn_cnt":'sum',
        # 	"ee_violtd_cnt":'sum',
        # 	"ee_pmt_recv": 'sum',
        # 	"records": 'sum',
        # 	}).reset_index().sort_values(["bw_amt"], ascending=True)

        temp_row = unique_legalname.nlargest(
            3, 'backwage_owed').reset_index(drop=True)
        temp_value_1 = temp_row['backwage_owed'].iloc[0]
        if temp_row.size > 0 and temp_value_1 > 0:
            # indexNamesArr = temp_row.index.values[0] #https://thispointer.com/python-pandas-how-to-get-column-and-row-names-in-dataframe/
            #result += indexNamesArr.astype(str)
            result += temp_row['legal_nm'].iloc[0]
            result += " is the employer with the highest recorded theft in this city, "
            result += "it has unpaid wage theft claims of $ "
            result += (str.format('{0:,.0f}', temp_value_1))
            result += ". "

        result += "</p>"

        result += ("\n")
        result += ("\n")

    # close
    result += '''
		</body>
		</html>
		'''
    # with open(filename, mode='a') as f:
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)


def Industry_Summary_Block(out_counts, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, OPEN_CASES, textFile):
    
    textFile.write("<h2>Summary for Reported Industry</h2> \n")

    if not df.empty:

        DF_MIN = min(pd.to_datetime(
            df['findings_start_date'].dropna(), errors='coerce'))
        DF_MAX = max(pd.to_datetime(
            df['findings_start_date'].dropna(), errors='coerce'))

        textFile.write(f"<p>Actual date range: ")
        textFile.write(DF_MIN.strftime("%m/%Y"))
        textFile.write(" to ")
        textFile.write(DF_MAX.strftime("%m/%Y"))
        textFile.write("</p> \n")
    else:
        textFile.write("<p>Actual date range: <undefined></p> \n")

    if OPEN_CASES == 1:
        textFile.write(
            "<p>This report has cases removed that are documented as paid or claimant withdrew or the amount repaid matches the backwages owed.</p> \n")

    if not len(out_counts.index) == 0:
        if OPEN_CASES == 1:
            textFile.write("<p>Active unpaid wage theft cases: ")
        else:
            textFile.write("<p>Total wage theft cases: ")
        textFile.write(str.format('{0:,.0f}', len(out_counts.index)))
        textFile.write(" <i>(Note: sum is of several types of 'open' disposition)</i></p> \n")

    if not out_counts['bw_amt'].sum() == 0:
        textFile.write("<p>Total wage theft:  $ ")
        textFile.write(str.format('{0:,.0f}', out_counts['bw_amt'].sum()))
        if not df.empty:
            if total_ee_violtd == 0:
                textFile.write(
                    " <i> Note: Backwages per employee violated is not calulated in this report</i></p> \n")
            else:
                textFile.write(" <i>(gaps estimated as $")
                textFile.write(str.format(
                    '{0:,.0f}', total_bw_atp//total_ee_violtd))
                textFile.write(" in backwage ")
                textFile.write(" and $")
                textFile.write(str.format(
                    '{0:,.0f}', ((total_bw_atp//total_ee_violtd) * .125) ) )
                textFile.write(" monetary penalty per employee violated ) ")
                textFile.write("</i>")
        textFile.write("</p> \n")

    if 'backwage_owed' not in out_counts.columns: #<-- probably a problem point
        out_counts['backwage_owed'] = 0
    
    if not out_counts['backwage_owed'].sum() == 0:
        textFile.write(
            "<p>Including monetary penalties and accrued interest, the amount owed is:  $ ")
        textFile.write(str.format(
            '{0:,.0f}', out_counts['backwage_owed'].sum()))  # bw_amt
        textFile.write("</p>")
    else: 
        textFile.write(
            " <i> Note: Monetary penalties and accrued interest not calculated in this report</i></p> \n")

    if not out_counts['ee_violtd_cnt'].sum() == 0:
        textFile.write("<p>Total employees: ")
        textFile.write(str.format(
            '{0:,.0f}', out_counts['ee_violtd_cnt'].sum()))
        textFile.write("</p> \n")

    if not out_counts['violtn_cnt'].sum() == 0:
        textFile.write("<p>Total violations: ")
        textFile.write(str.format('{0:,.0f}', out_counts['violtn_cnt'].sum()))
        if not df.empty:
            if total_ee_violtd == 0:
                textFile.write(
                    " <i> Note: Violations per employee is not calculated in this report</i></p> \n")
            else:
                textFile.write(" <i>(gaps estimated as ")
                textFile.write(str.format(
                    '{0:,.2g}', total_case_violtn//total_ee_violtd))
                textFile.write(" violation(s) per employee violated)</i>")
        textFile.write("</p>")

    textFile.write("\n")
    textFile.write("\n")

    textFile.write("<p>Companies that are involved in multiple cases: ")
    textFile.write(str.format('{0:,.0f}', len(unique_legalname.index)))  # here
    textFile.write("</p> \n")

    if not len(agency_df.index) == 0:
        textFile.write("<p>Companies that are cited by multiple agencies: ")
        textFile.write(str.format('{0:,.0f}', len(agency_df.index)))  # here
        textFile.write("</p> \n")

    textFile.write("\n")
    textFile.write("\n")


def Proportion_Summary_Block(out_counts, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, 
                             YEAR_START, YEAR_END, OPEN_CASES, target_jurisdition, TARGET_INDUSTRY, case_disposition_series, textFile, bug_log_csv):

    if not len(out_counts.index) == 0:
        textFile.write("\n")
        textFile.write("\n")

        textFile.write("<p>")
        textFile.write("Number and proportion of wage theft -- ")
    
        #variables
        total_number_of_cases = str.format('{0:,.0f}', len(case_disposition_series ) )
        
        report_type = "cases"
        if OPEN_CASES:
            report_type = "judgments"
        formated_start = YEAR_START.strftime("%m/%Y")

        #TEXT
        textFile.write(f" out of all {total_number_of_cases} wage theft {report_type} against \
                {TARGET_INDUSTRY[0][0]} industry companies in {target_jurisdition} \
                from {formated_start} to the present:")
        textFile.write("</p>")
        
        textFile.write("<ul>")

        case_disposition_series = case_disposition_series.value_counts()
        
        count = 0
        cutoff_size = 1
        for n in case_disposition_series:
            test_spot = case_disposition_series.index[count] #len(test_spot)
            if (len(test_spot) < 3):
                test_spot = '<Not Defined>'
            if n > cutoff_size:
                textFile.write(f"<li>{n} are {test_spot} \
                    ({str.format('{0:,.0%}', float(n)/float(total_number_of_cases.replace(',','')))})</li>")
                textFile.write("\n")
            count = (count +1)
            
            #textFile.write(f"<li>{n} are on {case_disposition_series.index[count] } </li>")

        textFile.write(f"<li><i>*disposition types with less than {cutoff_size + 1} records are not listed</i></li>")
        textFile.write("</ul>") 

        
        
        textFile.write("\n")
        textFile.write("\n")


def Notes_Block(textFile, default_zipcode="####X"):

    textFile.write("<p>Notes:</p>")
    textFile.write("\n")
    textFile.write("<p>")
    textFile.write(
        f"(1) In the tables and city summaries, the zip {default_zipcode} represents data that is missing the zip code field. ")
    textFile.write("(2) There are unlabeled industries, many of these are actually construction, care homes, restaurants, etc. just there is not an ability to label them as such--a label of 'other' could lead one to indicate that they are not these industries and therefore the category of 'undefined.' ")
    textFile.write("(3) Values may deviate by 10% within the report for camparable subcategories: this is due to labeling and relabeling of industry that may overwrite a previous industry label (for example Nail Hamburger could be labeled service or food). ")
    textFile.write("</p>")

    textFile.write("\n")

    textFile.write(
        "<p>Note that categorizations are based on both documented data and intelligent inferences, therefore, there are errors. ")
    textFile.write("For the fields used to prepare this report, please see <a href='https://docs.google.com/spreadsheets/d/19EPT9QlUgemOZBiGMrtwutbR8XyKwnrEhB5rZpZqM98/'>https://docs.google.com/spreadsheets/d/19EPT9QlUgemOZBiGMrtwutbR8XyKwnrEhB5rZpZqM98/</a> . ")
    textFile.write("And for the industry categories, which are given shortened names here, please see <a href='https://www.naics.com/search/'>https://www.naics.com/search/</a>  . ")
    #textFile.write("To see a visualization of the data by zip code and industry, please see (last updated Feb 2020) <a href='https://public.tableau.com/profile/forest.peterson#!/vizhome/Santa_Clara_County_Wage_Theft/SantaClaraCounty'></a> . </p>")

    textFile.write("\n")

    textFile.write("<p>The DIR DLSE uses one case per employee while the DOL WHD combines employees into one case. </p>")


    textFile.write("\n")
    textFile.write("\n")


def Methods_Block(textFile):
    textFile.write("<p>")
    textFile.write("Methods: ")
    textFile.write("</p>")

    textFile.write("<p>")
    textFile.write("backwage_owed:")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write(
        "<li>the sum of wages owed, monetary penalty, and interest</li>")
    textFile.write(
        "<li>df['backwage_owed'] = df['wages_owed'] + df['cmp_assd_cnt'] + df['interest_owed']</li>")
    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("estimate when missing:")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write(
        "<li>estimated backwage per employee = (df['bw_amt'].sum() / df['ee_violtd_cnt'].sum() ) </li>")
    textFile.write(
        "<li>estimated monetary penalty (CMP) assessed per employee = (est_bw_amt * 12.5%) </li>")
    textFile.write(
        "<li>where interest balance due is missing, then infer an interest balance based on a calaculated compounded interest of the backwages owed</li>")
    textFile.write("<ul>")
    textFile.write(
        "<li>df['interest_owed'] = np.where((df['interest_owed'].isna() | (df['interest_owed'] == '')), df['Interest_Accrued'], df['interest_owed'])</li>")
    textFile.write(
        "<li>df['Interest_Accrued'] = (df['wages_owed'] * (((1 + ((r/100.0)/n)) ** (n*df['Years_Unpaid']))) ) - df['wages_owed']</li>")
    textFile.write("</ul>")
    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("wages_owed: ")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write(
        "<li>unpaid backwages less payment recieved by employee</li>")
    textFile.write(
        "<li>df['wages_owed'] = df['bw_amt'] - df['ee_pmt_recv']</li>")
    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("interest_owed: ")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write("<li>interestd due less interest payments recieved</li>")
    textFile.write(
        "<li>df['interest_owed'] = df['Interest_Accrued'] - df['Interest_Payments_Recd])</li>")
    textFile.write("</ul>")

    # textFile.write("bw_amt: ")
    # textFile.write("<li> </li>")
    # textFile.write("violtn_cnt: ")
    # textFile.write("<li> </li>")
    # textFile.write("ee_violtd_cnt: ")
    # textFile.write("<li> </li>")
    # textFile.write("ee_pmt_recv: ")
    # textFile.write("<li> </li>")
    # textFile.write("records: ")
    # textFile.write("<li> </li>")
    textFile.write("</p>")

    textFile.write("\n")
    textFile.write("\n")

def Sources_Block(textFile):
    textFile.write("<p>")
    textFile.write("Data Sources: ")
    textFile.write("</p>")

    textFile.write("<p>")

    textFile.write("<ul>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Judgements from Aug 2019 to Jan 2024 <a href='https://cadir.my.site.com/'>https://cadir.my.site.com/</a>  </li>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Wage Claim Adjudications (WCA) from Aug 2019 to Jan 2024 <a href='https://cadir.my.site.com/wcsearch/s/'>https://cadir.my.site.com/wcsearch/s/</a>  </li>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Wage Claim Adjudications (WCA) from Jan 2001 to Jul 2019 <a href='https://www.researchgate.net/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019'>https://www.researchgate.net/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019</a>  </li>")
    textFile.write(
        "<li>US DOL WHD Enforcement Database Compliance Actions from Jan 2005 to Jan 2024 <a href='https://enforcedata.dol.gov/views/data_catalogs.php'>https://enforcedata.dol.gov/views/data_catalogs.php</a> </li>")
    textFile.write("</ul>")

    textFile.write("</p>")

    textFile.write("\n")
    textFile.write("\n")

def Signatory_to_Nonsignatory_Block(df1, df2, filename):
    # construction
    # medical

    # ratio_construction = df1.query(
    # 	"signatory_industry == 'construction_signatories' and signatory_industry == 'signatories_UCON' and signatory_industry == 'signatories_CEA' "
    # 	)['bw_amt'].sum() / df1['bw_amt'].sum()

    ratio_construction_ = df1.loc[df1['signatory_industry']
                                  == 'Construction', 'backwage_owed'].sum()
    #ratio_construction_ucon = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
    #ratio_construction_cea = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
    construction_industry_backwage = df1.loc[df1['industry']
                                             == 'Construction', 'backwage_owed'].sum()
    ratio_construction = (ratio_construction_) / construction_industry_backwage

    ratio_hospital_ = df1.loc[df1['signatory_industry']
                              == 'Health_care', 'backwage_owed'].sum()
    carehome_industry_backwage = df1.loc[df1['industry']
                                         == 'Carehome', 'backwage_owed'].sum()
    healthcare_industry_backwage = df1.loc[df1['industry']
                                           == 'Health_care', 'backwage_owed'].sum()
    residential_carehome_industry_backwage = df1.loc[df1['industry']
                                                     == 'Residential_carehome', 'backwage_owed'].sum()
    ratio_hospital = ratio_hospital_ / \
        (carehome_industry_backwage + healthcare_industry_backwage +
         residential_carehome_industry_backwage)

    filename.write("<p>")
    filename.write("Of $ ")
    filename.write(str.format('{0:,.0f}', df1['backwage_owed'].sum()))
    filename.write(
        " in all industry back wages owed (inc. monetary penalty and interest), ")
    filename.write(" union signatory employers represent: </p>")
    filename.write("<p>")
    filename.write(str.format('{0:,.0f}', ratio_construction * 100))
    filename.write(
        " percent of total documented theft in the construction industry with $ ")
    filename.write(str.format('{0:,.0f}', (ratio_construction_)))
    filename.write(" of $ ")
    filename.write(str.format('{0:,.0f}', construction_industry_backwage))
    filename.write(" in construction specific backwages. ")
    filename.write("</p>")
    filename.write("<p>")
    filename.write(str.format('{0:,.0f}', ratio_hospital * 100))
    filename.write(
        " percent of total documented theft in the healthcare industry with $ ")
    filename.write(str.format('{0:,.0f}', ratio_hospital_))
    filename.write(" of $ ")
    filename.write(str.format('{0:,.0f}', (carehome_industry_backwage +
                                           healthcare_industry_backwage + residential_carehome_industry_backwage)))
    filename.write(" in healthcare specific backwages. ")
    filename.write("</p>")
    filename.write("<p> Due to the situation of the union worker as fairly paid, educated in labor rights, and represented both in bargaining and enforcement of that bargained for agreement--as well as that these are two heavily unionized industries and that much of the non-union data is lost as undefined industry--then these percentages are likely overly represented as union workers would know how and when to bring a wage and hour case. As such, it is fair to conclude that there effectively is no concernable degree of wage theft in the unionized workforce that requires outside enforcement. </p>")


def Footer_Block(TEST, textFile):
    textFile.write("<p> Report generated ")
    textFile.write(pd.to_datetime('today').strftime("%m/%d/%Y"))
    textFile.write("</p> \n")

    textFile.write("<p>Palo Alto Data Group pulled a list from databases of all sector judgments and adjudications \
                to generate this report using an open source software that was prepared by the Center for Integrated Facility Engineering (CIFE) \
                   at Stanford University in collaboration with the Santa Clara County Wage Theft Coalition. These data \
                   have not been audited and, therefore, are only intended as an indication of wage theft.</p> \n")

#write_to_html_file(new_df_3, header_HTML_EMP3, "", file_path('py_output/A4W_Summary_by_Emp_for_all2.html') )

# https://stackoverflow.com/questions/47704441/applying-styling-to-pandas-dataframe-saved-to-html-file
def write_to_html_file(df, header_HTML, title, filename, rows = 99):
    # added this line to avoid error 8/10/2022 f. peterson
    import pandas.io.formats.style
    import os  # added this line to avoid error 8/10/2022 f. peterso

    result = '''
		<html>
		<head>
		<style>

			h2 {
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
			}
			table { 
				margin-left: auto;
				margin-right: auto;
			}
			table, th, td {
				border: 1px solid black;
				border-collapse: collapse;
			}
			th, td {
				padding: 5px;
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
				font-size: 90%;
			}
			table tbody tr:hover {
				background-color: #dddddd;
			}
			.wide {
				width: 90%; 
			}

		</style>
		</head>
		<body>
		'''
    result += '<h2> %s </h2>\n' % title
    if type(df) == pd.io.formats.style.Styler:
        result += df.render()
    else:
        result += df.to_html(max_rows = rows, classes='wide', columns=header_HTML, escape=False)
    result += '''
		</body>
		</html>
		'''
    # with open(filename, mode='a') as f:
    # added this line to avoid error 8/10/2022 f. peterson
    # create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    # https://stackoverflow.com/questions/27092833/unicodeencodeerror-charmap-codec-cant-encode-characters
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)



def append_log(bug_log, LOGBUG, text):
    if LOGBUG:
        bugFile = open(bug_log, 'a')
        bugFile.write(text)
        bugFile.close()
        print(text)

def debug_fileSetup_def(bug_filename):

    bug_filename.write("<!DOCTYPE html>")
    bug_filename.write("\n")
    bug_filename.write("<html><body>")
    bug_filename.write("\n")

    bug_filename.write("<h1>START</h1>")
    bug_filename.write("\n")


def file_path(relative_path):
    if os.path.isabs(relative_path):
        return relative_path
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


if __name__ == '__main__':
    main()
