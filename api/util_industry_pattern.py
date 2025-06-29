
import re
import pandas as pd
import numpy as np



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

            if 'legal_nm' in df.columns and 'trade_nm' in df.columns: #uses legal and trade names to infer industry
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
        '''
        #refactor: vectorized replacement for when time to swap and test
        df['industry'] = np.where(
            (df['industry'] == '') & (df['trade2'] != ''),
            df['trade2'],
            df['industry']
        )
        '''

        # if all fails, assign 'Undefined' so it gets counted
        df['industry'] = df['industry'].replace(
            r'^\s*$', 'Undefined', regex=True)
        df['industry'] = df['industry'].fillna('Undefined')
        df['industry'] = df['industry'].replace(np.nan, 'Undefined')

        #'trade2' is not used outside this function other than in the print to csv 

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
