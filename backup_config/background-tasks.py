import time
from wagetheft_report_2020_v3 import generateWageReport


def run_report():
    # Your existing report code here
    print("Running the report...")


    
    
    target_state = ""
    target_county = ""
    target_city = ""
    target_industry = ""
    target_organization = ""
    
    includeFedData
    includeStateJudgements
    includeStateCases, 
    infer_zip, 
    prevailing_wage_report, signatories_report, open_cases_only, 
    include_tables, include_summaries, only_sig_summaries, include_top_viol_tables, 
    use_assumptions, infer_by_naics, 
    YEAR_START_TEXT, YEAR_END_TEXT

    #out it goes
    generateWageReport(
        target_state, target_county, target_city, target_industry, target_organization, 
        includeFedData, includeStateJudgements, includeStateCases, 
        infer_zip, 
        prevailing_wage_report, signatories_report, open_cases_only, 
        include_tables, include_summaries, only_sig_summaries, include_top_viol_tables, 
        use_assumptions, infer_by_naics, 
        YEAR_START_TEXT, YEAR_END_TEXT)
    
    # simulate work
    time.sleep(5)
    print("Report done.")

if __name__ == "__main__":
    run_report()
