import pandas as pd
import requests
import re
import os
import time

from wagetheft_io_utils import (
    Read_Violation_Data,
)

from wagetheft_inference_util import (
     inference_function,
)

from wagetheft_clean_value_utils import (
    clean_function,
)

from debug_utils import (
    save_backup_to_folder,
)


def read_df(
        industriesDict, 
        prep_dict, 
        bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv,
        abs_path0,
        TEST_ = False):
    
    OLD_DATA = False
    if prep_dict['New_Data_On_Run_Test']: 
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
        
        DF_OG = read_new_df(
            industriesDict, 
            prep_dict, 
            bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv,
            TEST_)
    
    out_target = DF_OG.copy()  # new df -- hold df_csv as a backup and only df from this point on

    url_list = build_url_list(TEST_)

    for n in url_list: #filter dataset for this run -- example, remove fed and state
        if n[1] == 0: # if data set should be removed
            out_target = out_target[(out_target.juris_or_proj_nm != n[2])] #save every data set except what is to be removed
        #out_target.to_csv(os.path.join(abs_path, (file_name+f'test_{n[2]}_out_file_report').replace(' ', '_') + '.csv'))

    return out_target, DF_OG


def read_new_df(industriesDict, 
        prep_dict, 
        bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv,
        TEST_ = False):
    
    TEMP_TARGET_INDUSTRY = industriesDict['All NAICS']
    
    url_list = build_url_list(TEST_)

    trigger = False #true is encoding="ISO-8859-1" else false is encoding='utf8'
    count = 1
    for n in url_list:
        if ((n[2] == 'DOL_WHD') or (n[2] == 'DIR_DLSE') ): 
            trigger = True #toggle between (true) encoding="ISO-8859-1" and (false) encoding='utf8'
        url = n[0]
        out_file_report = n[2]

        df_url = pd.DataFrame()

        df_url = Read_Violation_Data(
            prep_dict['TEST_CASES'], 
            url, 
            out_file_report, 
            trigger, 
            bug_log_csv, abs_path, 
            file_name) #save raw copy to csv_read_backup
        
        df_url = df_url.replace('\s', ' ', regex=True)  # remove line returns
        df_url = clean_function(
            prep_dict['RunFast'], 
            df_url, 
            prep_dict['RunFast'], 
            df_url, 
            prep_dict['FLAG_DUPLICATE'], 
            bug_log, LOGBUG, log_number, bug_log_csv)
        
        df_url = inference_function(
                df_url, 
                prep_dict['cityDict'], 
                TEMP_TARGET_INDUSTRY, 
                prep_dict['prevailing_wage_terms'], 
                prep_dict['prevailing_wage_labor_code'], 
                prep_dict['prevailing_wage_politicals'], 
                bug_log, LOGBUG, log_number)
        
        save_backup_to_folder(
            df_url, 
            prep_dict['url_backup_file']+str(count), 
            prep_dict['url_backup_path']
            ) #save copy to url_backup -- cleaned file
        
        count += 1

        DF_OG = pd.concat([df_url, DF_OG], ignore_index=True)

        trigger = False
    
    return DF_OG


def build_url_list(TEST_):

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
    
    return url_list