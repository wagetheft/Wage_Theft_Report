from requests import request as rq
from flask import Flask, request, send_file
from api.wagetheft_report_2020_v3 import generateWageReport
import os

app = Flask(__name__)


@app.route("/hello")
def helloWorld():
    return "Hello World!"


@app.route('/generateWageReport', methods=['POST'])
def generateFile():
    parameters = request.get_json()
    print(parameters)
    if not checkValidInput(parameters):
        return "Invalid Request. Invalid Input", 400

    target_state = parameters["target_state"] if "target_state" in parameters else ""
    target_county = parameters["target_county"] if "target_county" in parameters else ""
    target_city = parameters["target_city"] if "target_city" in parameters else ""

    target_industry = parameters["target_industry"] if "target_industry" in parameters else ""

    target_organization = parameters["target_organization"] if "target_organization" in parameters else ""
    
    includeFedData = parameters["includeFedData"] if "includeFedData" in parameters else 0
    includeStateJudgements = parameters["includeStateJudgements"] if "includeStateJudgements" in parameters else 0
    includeStateCases = parameters["includeStateCases"] if "includeStateCases" in parameters else 0
    
    infer_zip = parameters["infer_zip"] if "infer_zip" in parameters else 0
    use_assumptions = True #parameters["use_assumptions"] if "use_assumptions" in parameters else "1"
    infer_by_naics = parameters["infer_by_naics"] if "infer_by_naics" in parameters else 0

    open_cases_only = parameters["open_cases_only"] if "open_cases_only" in parameters else 0
    prevailing_wage_report = parameters["prevailing_wage_report"] if "prevailing_wage_report" in parameters else 0
    signatories_report = parameters["signatories_report"] if "signatories_report" in parameters else 0

    include_tables = parameters["include_tables"] if "include_tables" in parameters else 0
    include_summaries = parameters["include_summaries"] if "include_summaries" in parameters else 0
    only_sig_summaries = parameters["only_sig_summaries"] if "only_sig_summaries" in parameters else 0
    include_top_viol_tables = parameters["include_top_viol_tables"] if "include_top_viol_tables" in parameters else 0
    
    YEAR_START_TEXT = parameters["YEAR_START"] if "YEAR_START" in parameters else ''
    YEAR_END_TEXT = parameters["YEAR_END"] if "YEAR_END" in parameters else 'today'

    #valid stuff
    if includeFedData == 0 and includeStateJudgements == 0 and includeStateCases == 0:
        includeFedData = 1
        includeStateJudgements == 1
        includeStateCases == 1
    if include_tables == 0 and include_top_viol_tables == 0:
        include_top_viol_tables = 1
    if only_sig_summaries == 1:
        include_summaries = 1
        include_tables = 1
    if include_summaries == 1 and include_tables == 0:
        include_tables = 1
    
    #if parameters["target_industry"] == "":
    #    target_industry = "ALL NAICS"
    if target_industry == "":
        target_industry = "ALL NAICS"

    if parameters["target_city"] == "" and parameters["target_county"] == "":
        target_state = "California"
    
    if parameters["includeStateJudgements"] == 1 or parameters["includeStateCases"] == 1:
        infer_zip = 1
        infer_by_naics = 1


    #out it goes
    report_file_name = generateWageReport(target_state, target_county, target_city, target_industry, target_organization, \
                                          includeFedData, includeStateJudgements, includeStateCases, infer_zip, prevailing_wage_report, signatories_report, \
                                            open_cases_only, include_tables, include_summaries, only_sig_summaries, \
                                            include_top_viol_tables, use_assumptions, infer_by_naics, YEAR_START_TEXT, YEAR_END_TEXT)

    try:
        return send_file(report_file_name, as_attachment=True)
    except Exception as e:
        return "Server error", 500

def checkValidInput(inputDict: dict) -> bool:
    toCheck = ["target_city", "target_industry", "target_organization", 
               "includeFedData", "includeStateJudgements", "includeStateCases", "infer_zip", "prevailing_wage_report", "signatories_report", 
               "open_cases_only", "include_tables", "include_summaries", "only_sig_summaries", 
               "include_top_viol_tables",  "use_assumptions",  "infer_by_naics", "YEAR_START", "YEAR_END"]
    return all(key in inputDict for key in toCheck)

