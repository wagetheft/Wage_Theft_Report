
import numpy as np


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

