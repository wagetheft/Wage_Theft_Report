
import pandas as pd
import numpy as np


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


def calc_violation_count(df):
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



