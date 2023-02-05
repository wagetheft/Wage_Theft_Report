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
    target_industry = parameters["target_industry"]
    includeFedData = parameters["includeFedData"]
    includeStateData = parameters["includeStateData"]
    infer_zip = parameters["infer_zip"]
    prevailing_wage_report = parameters["prevailing_wage_report"]
    signatories_report = parameters["signatories_report"]
    all_industry_summary_block = parameters["all_industry_summary_block"]
    open_cases_only = parameters["open_cases_only"]
    include_tables = parameters["include_tables"]
    include_summaries = parameters["include_summaries"]
    only_sig_summaries = parameters["only_sig_summaries"]
    include_top_viol_tables = parameters["include_top_viol_tables"]
    use_assumptions = 1 #parameters["use_assumptions"] if "use_assumptions" in parameters else 1 #temp fix to force assumptions
    infer_by_naics = parameters["infer_by_naics"]

    #valid stuff
    if includeFedData == 0 and includeStateData == 0:
        includeFedData = 1
        includeStateData = 1
    if include_tables == 0 and include_top_viol_tables == 0:
        include_top_viol_tables = 1
    if only_sig_summaries == 1:
        include_summaries = 1
        include_tables = 1
    if include_summaries == 1:
        include_tables = 1
    if parameters["target_industry"] == "":
        target_industry = "WTC NAICS"
    if parameters["target_city"] == "":
        target_county = "Santa_Clara_County"
    if parameters["includeStateData"] == 1:
        infer_zip = 1

    #out it goes
    report_file_name = generateWageReport(target_state, target_county, target_city, target_industry, includeFedData, includeStateData, infer_zip, prevailing_wage_report, signatories_report,
                                          all_industry_summary_block, open_cases_only, include_tables, include_summaries, only_sig_summaries, include_top_viol_tables, use_assumptions, infer_by_naics)

    try:
        return send_file(report_file_name, as_attachment=True)
    except Exception as e:
        return "Server error", 500


def checkValidInput(inputDict) -> bool:
    if "target_city" not in inputDict or "target_industry" not in inputDict or "includeFedData" not in inputDict or "includeStateData" not in inputDict or "infer_zip" not in inputDict or "prevailing_wage_report" not in inputDict or "signatories_report" not in inputDict or "all_industry_summary_block" not in inputDict or "open_cases_only" not in inputDict or "include_tables" not in inputDict or "include_summaries" not in inputDict or "only_sig_summaries" not in inputDict or "include_top_viol_tables" not in inputDict or "use_assumptions" not in inputDict or "infer_by_naics" not in inputDict:
        return False

    return True
