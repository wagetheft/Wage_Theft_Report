
import re
import pandas as pd
import time

import platform
if platform.system() == 'Windows' or platform.system() =='Darwin':
    from debug_utils import append_log
else:
    from api.debug_utils import append_log


def is_string_series(s : pd.Series): #https://stackoverflow.com/questions/43049545/python-check-if-dataframe-column-contain-string-type
    if isinstance(s.dtype, pd.StringDtype):
        # The series was explicitly created as a string series (Pandas>=1.0.0)
        return True
    elif s.dtype == 'object':
        # Object series, check each value
        return all((v is None) or isinstance(v, str) for v in s)
    else:
        return False


def Define_Column_Types(df):

    # Define column types**********************
    if (
        'case_id_1' in df.columns 
        and 'legal_nm' in df.columns 
        and 'trade_nm' in df.columns 
        and 'naic_cd' in df.columns
        ):

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




#CLEANUP
def clean_function(
        RunFast, 
        df, 
        FLAG_DUPLICATE,
        bug_log, LOGBUG, log_number, bug_log_csv):

    function_name = "clean_function"

    if not RunFast:
        
        time_1 = time.time()
        df = Cleanup_Number_Columns(df)
        time_2 = time.time()
        log_number = "Cleanup number columns"
        append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)
        
        time_1 = time.time()
        df = Cleanup_Text_Columns(df, bug_log, LOGBUG, bug_log_csv)
        time_2 = time.time()
        log_number = "Cleanup text columns"
        append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

        #time_1 = time.time()
        #df = CleanUpAgency(df, ) #<-- use for case file codes
        #time_2 = time.time()
        #log_number+=1
        #append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

        time_1 = time.time()
        df = Define_Column_Types(df)
        time_2 = time.time()
        log_number = "Define_Column_Types"
        append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

        # remove duplicate cases using case_id and violation as a unique key
        time_1 = time.time()
        df = DropDuplicateRecords(df, FLAG_DUPLICATE, bug_log_csv)
        df = FlagDuplicateBackwage(df, FLAG_DUPLICATE)
        time_2 = time.time()
        log_number = "DropDuplicateRecords"
        append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)
        
    return df


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
            (df["Case Status"] != 'Closed') &
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

    if 'bw_amt' in df and 'ee_pmt_recv' in df: #take cases that are open
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


def MoveCompanyLiabilityTermsToLiabilityTypeColumn(df):

    df['Liabilitytype'] = ""  # create new column and fill with str

    liability_terms0 = ['(a California)', '(a Californi)', '(a Californ)']
    pattern_liability0 = '|'.join(liability_terms0)
    if 'legal_nm' in df.columns and 'trade_nm' in df.columns:
        foundIt0 = (df['legal_nm'].str.contains(pattern_liability0, na=False, flags=re.IGNORECASE, regex=True) |
                    df['trade_nm'].str.contains(pattern_liability0, na=False, flags=re.IGNORECASE, regex=True))

        df.loc[foundIt0, 'Liabilitytype'] = 'California'

        '''
        refactor when time
        replacements = [
            ('Inc.', ''), 
            ('LLC', ''), 
            # ... more ...
        ]
        for old, new in replacements:
            df['legal_nm'] = df['legal_nm'].str.replace(old, new, regex=False)
        '''

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
    if 'legal_nm' in df.columns and 'trade_nm' in df.columns:
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

    if 'legal_nm' in df.columns and 'trade_nm' in df.columns:
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

    if 'legal_nm' in df.columns and 'trade_nm' in df.columns:
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

    if 'legal_nm' in df.columns and 'trade_nm' in df.columns:
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

    if 'bw_amt' in DF_OG_VLN.columns:
        DF_OG_VLN['bw_amt'] = CleanNumberColumns(DF_OG_VLN['bw_amt'])
    else:
        DF_OG_VLN['bw_amt'] = 0
    if 'violtn_cnt' in DF_OG_VLN.columns:
        DF_OG_VLN['violtn_cnt'] = CleanNumberColumns(DF_OG_VLN['violtn_cnt'])
    else:
        DF_OG_VLN['violtn_cnt'] = 0
    if 'ee_violtd_cnt' in DF_OG_VLN.columns:
        DF_OG_VLN['ee_violtd_cnt'] = CleanNumberColumns(DF_OG_VLN['ee_violtd_cnt'])
    else:
        DF_OG_VLN['ee_violtd_cnt'] = 0
    if 'ee_pmt_recv' in DF_OG_VLN.columns:
        DF_OG_VLN['ee_pmt_recv'] = CleanNumberColumns(DF_OG_VLN['ee_pmt_recv'])
    else:
        DF_OG_VLN['ee_pmt_recv'] = 0

    return DF_OG_VLN



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
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

    time_1 = time.time()
    df = RemovePunctuationFromCity(df)  # once more
    time_2 = time.time()
    log_number = "RemovePunctuationFromCity"
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

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
    append_log(bug_log, f"Time to finish section {log_number} in {function_name} " + "%.5f" % (time_2 - time_1) + "\n", LOGBUG)

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







