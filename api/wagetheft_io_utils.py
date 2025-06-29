
import pandas as pd
import requests
import io
import numpy as np


from debug_utils import save_backup_to_folder

from wagetheft_clean_value_utils import (
    CleanNumberColumns,
)


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



def Read_Violation_Data(TEST_CASES, url, out_file_report, trigger, bug_log_csv, abs_path, file_name):

    df_csv = pd.DataFrame()
    df_csv = read_from_url(url, TEST_CASES, trigger)

    df_csv['juris_or_proj_nm'] = out_file_report
    df_csv = Setup_Regular_headers(df_csv, abs_path, file_name, bug_log_csv)

    save_backup_to_folder(df_csv, out_file_report + '_backup', "csv_read_backup/") #greedy backup even if already exists

    return df_csv


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



