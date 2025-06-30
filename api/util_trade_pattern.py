

import pandas as pd


def lookupTrade(trade, list_x, col):
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


