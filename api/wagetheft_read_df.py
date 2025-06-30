import pandas as pd
import requests
import re
import os
import time
import glob

#import pyarrow.parquet as pq #only activate in Linux VM

from wagetheft_inference_util import inference_function
from wagetheft_clean_value_utils import clean_function
from wagetheft_io_utils import (
    Read_Violation_Data,
    save_backup_to_folder,
)
from util_filter import filter_function
from datetime import datetime, timedelta


def read_df(
        industriesDict, 
        prep_dict, 
        includeFedData, includeStateJudgements, includeStateCases,
        bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv,
        abs_path0,
        TEST_STATUS = False):
    
    OLD_DATA = False
    os.makedirs(abs_path0, exist_ok=True)
    os.makedirs(abs_path0 + '_fast', exist_ok=True)
    dir = os.listdir(abs_path0)
    csv_files = glob.glob(os.path.join(abs_path0, "*.csv"))
    
    #verify age of file
    age_limit = (1 * 30 * 24 * 60 * 60) #seconds = 1 month x 30 days/mo x 24 hr/day x 60 min/hr x 60s/min
    for f in csv_files:
        born_on = os.path.getmtime(f)
        age = (time.time() - born_on)
        if age > age_limit: #if older than one month
            OLD_DATA = True
            os.remove(f) #remove and make new   

    if ( #read new files from url source -- kae all new regardless
        (OLD_DATA
        or len(dir) < 2) #folder empty
        or prep_dict['New_Data_On_Run_Test']):    
        url_list = build_url_list(includeFedData, includeStateJudgements, includeStateCases, TEST_ = TEST_STATUS)
        time.sleep(2) #pause to prevent whatever is causing crash
        df_from_url(
            industriesDict, 
            prep_dict, 
            url_list,
            bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv)
        True
    
    csv_files = glob.glob(os.path.join(abs_path0, "*.csv"))

    for f in csv_files:
        time.sleep(2) #pause to prevent whatever is causing crash
        #df_backup = pd.read_csv(f, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=prep_dict['TEST_CASES'], dtype={'zip_cd': 'str'} )
        df_backup = pd.read_csv(f, encoding = 'utf8', low_memory=False, thousands=',', nrows=prep_dict['TEST_CASES'], dtype={'zip_cd': 'str'} )
        #df_backup_test = pq.read_parquet(f.split('.')[0] + '.parquet')
        prep_dict['DF_OG'] = pd.concat([df_backup, prep_dict['DF_OG']], ignore_index=True)  

    out_target = prep_dict['DF_OG'].copy()  # new df -- hold df_csv as a backup and only df from this point on

    time.sleep(2) #pause to prevent whatever is causing crash
    url_list = build_url_list(includeFedData, includeStateJudgements, includeStateCases, Fast='Fast')

    for url in url_list: #filter dataset for this run -- example, remove fed and state
        time.sleep(2) #pause to prevent whatever is causing crash
        if url[1] == 0: # if dataset has not been selected
            out_target = out_target[(out_target.juris_or_proj_nm != url[2])] #save every data set except what is to be removed
        #out_target.to_csv(os.path.join(abs_path, (file_name+f'test_{n[2]}_out_file_report').replace(' ', '_') + '.csv'))

    return out_target, prep_dict['DF_OG'] 


def df_from_url(
        industriesDict, 
        prep_dict, 
        url_list,
        bug_log, LOGBUG, log_number, abs_path, file_name, bug_log_csv
        ):
    
    TEMP_TARGET_INDUSTRY = industriesDict['All NAICS']

    trigger = False #true is encoding="ISO-8859-1" else false is encoding='utf8'
    count = 1
    for url in url_list:
        if ((url[2] == 'DOL_WHD') or (url[2] == 'DIR_DLSE') ): 
            trigger = True #toggle between (true) encoding="ISO-8859-1" and (false) encoding='utf8'
            
        url_cell = url[0]
        out_file_report = url[2]

        df_url = pd.DataFrame()

        df_url = Read_Violation_Data( #save raw downloads here
            prep_dict['TEST_CASES'], 
            url_cell, 
            out_file_report, 
            trigger, 
            bug_log_csv, abs_path, 
            file_name) #save raw copy to csv_read_backup
        
        df_url = df_url.replace('\s', ' ', regex=True)  # remove line returns
        df_url = clean_function(
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
        
        save_backup_to_folder( #save clean download here
            df_url, 
            prep_dict['url_backup_file']+str(count), 
            prep_dict['url_backup_path']
            ) #save copy to url_backup -- cleaned file
        
        YEAR_END_THEN = pd.to_datetime('today')
        YEAR_START_NOW = (YEAR_END_THEN - timedelta(days=8*365)).strftime('%Y-%m-%d')

        df_url_short = filter_function(
            df_url,
            YEAR_START = YEAR_START_NOW, 
            YEAR_END = YEAR_END_THEN, 
            TARGET_ZIPCODES = '00000',
            TARGET_INDUSTRY = 'Construction',
            infer_zip = True, infer_by_naics = True, 
            target_state = 'California', 
            )

        save_backup_to_folder( #save clean download here
            df_url_short, 
            prep_dict['url_backup_file']+("_ca_23_2018_")+str(count), 
            prep_dict['url_backup_path'] + '_fast'
            ) #save copy to url_backup -- cleaned file
        
        count += 1

        prep_dict['DF_OG']  = pd.concat([df_url, prep_dict['DF_OG'] ], ignore_index=True)

        trigger = False
    


def build_url_list(includeFedData, includeStateJudgements, includeStateCases, TEST_ = False, Fast = False, ):

    includeTestData = 0 #this is always 0
    if (TEST_ == 1): 
        includeTestData = 1
        includeFedData = 0
        includeStateJudgements = 0
        includeStateCases = 0
        #includeLocalData = False -- unused
        #includeOfficeData = False -- unused

    url0 = "temp"
    url1 = "temp"
    url2 = "temp"
    url3 = "temp"
    url4 = "temp"
    url5 = "temp"
    
    if not Fast:
        #Test file
        url0 = "https://stanford.edu/~granite/DLSE_no_returns_Linux_TEST.csv" #<-- open and edit this file with test data

        #find updated url -- #dev by Henry 8/21/2023
        ret = requests.post('https://enforcedata.dol.gov/views/data_summary.php', data={'agency':'whd'})
        m = re.search(r'(https://enfxfr.dol.gov/\.\./data_catalog/WHD/whd_whisard_[0-9]{8}\.csv\.zip)', str(ret.content))   
        #url1 = "https://enfxfr.dol.gov/data_catalog/WHD/whd_whisard_20230710.csv.zip" #update link from here https://enforcedata.dol.gov/views/data_catalogs.php
        url1 = m.group(0)
        time.sleep(2) #pause to prevent whatever is causing crash

        #pre-2019 DLSE data
        # url2 = "https://www.researchgate.net/profile/Forest-Peterson/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019/data/61de6b974e4aff4a643603ae/HQ20009-HQ-2nd-Production-8132019.csv"
        # url2 = "https://drive.google.com/file/d/1TRaixcwTg08bEyPSchyHntkkktG2cuc-/view?usp=sharing"
        url2 = "https://stanford.edu/~granite/HQ20009-HQ2ndProduction8.13.2019_no_returns_Linux_CA.csv" #10/2/2022 added _Linux

        #NEED TO REVISE SO THIS IS A FRESH PULL LIKE WHD PULL
        url3 = "https://stanford.edu/~granite/WageClaimDataExport_State_Construction_Jan_8_2024.csv" #TEST
        url4 = "https://stanford.edu/~granite/ExportData_Judge_state_NAISC_23_Jan_8_2024.csv" #TEST
        url5 = "https://stanford.edu/~granite/ExportData_Judge_state_NAISC_5413_Jan_8_2024.csv" #TEST


    url_list = [
        [url0, includeTestData,'TEST'], 
        [url1, includeFedData,'DOL_WHD'], 
        [url2, includeStateCases,'DIR_DLSE'],
        [url3, includeStateCases,'DLSE_WageClaim'],
        [url4, includeStateJudgements,'DLSE_J-23'],
        [url5, includeStateJudgements,'DLSE_J-5413'],
        #includeLocalData = False -- unused
        #includeOfficeData = False -- unused
    ]
    
    return url_list