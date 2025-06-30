

from wagetheft_clean_value_utils import (
    DropDuplicateRecords,
    RemoveCompletedCases,
)

from wagetheft_calc_utils import (
    infer_backwages,
    infer_wage_penalty,
    calc_violation_count,
    wages_owed,
    calculate_interest_owed,
    backwages_owed,
)

from util_filter import (
    filter_function,
)

from util_signatory_pattern import (
    infer_signatory_cases,
)


def shape_df(
        out_target, 
        option_dict,
        FLAG_DUPLICATE, 
        bug_log_csv
        ):

    out_target = DropDuplicateRecords(out_target, FLAG_DUPLICATE, bug_log_csv) #look for duplicates across data sets

    out_target = filter_function(
        out_target, 
        option_dict['TARGET_ZIPCODES'], 
        option_dict['TARGET_INDUSTRY'], 
        option_dict['infer_zip'], 
        option_dict['infer_by_naics'], 
        option_dict['YEAR_START'], 
        option_dict['YEAR_END'], 
        option_dict['target_state'])
    
    if option_dict['signatories_report'] == 0:
        out_target = infer_signatory_cases(out_target, option_dict['SIGNATORY_INDUSTRY'])

    # note--estimate back wage, penalty, and interest, based on violation
    if option_dict['use_assumptions']: 
        out_target = calc_violation_count(out_target)
        out_target = infer_backwages(out_target)  # B backwage, M monetary penalty
        out_target = infer_wage_penalty(out_target)  # B backwage, M monetary penalty
        out_target = wages_owed(out_target)
        out_target = calculate_interest_owed(out_target)
        out_target = backwages_owed(out_target)

    if option_dict['open_cases_only'] == 1: 
        out_target = RemoveCompletedCases(out_target)
    


    return out_target