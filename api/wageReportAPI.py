from requests import request as rq
from flask import Flask, request, send_file
from api.wagetheft_report_2020_v3 import generateWageReport, generateFakeFile
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

    target_city = parameters["target_city"]
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
    use_assumptions = parameters["use_assumptions"]
    infer_by_naics = parameters["infer_by_naics"]

    report_file_name = generateWageReport(target_city, target_industry, includeFedData, includeStateData, infer_zip, prevailing_wage_report, signatories_report,
                                          all_industry_summary_block, open_cases_only, include_tables, include_summaries, only_sig_summaries, include_top_viol_tables, use_assumptions, infer_by_naics)

    try:
        rel_path = 'report_output_/'
        script_dir = os.path.dirname(os.path.dirname(__file__))
        abs_path = os.path.join(script_dir, rel_path)
        pathName = abs_path + report_file_name
        return send_file(pathName, as_attachment=True)
    except Exception as e:
        return "Server error", 500


def checkValidInput(inputDict) -> bool:
    if "target_city" not in inputDict or "target_industry" not in inputDict or "includeFedData" not in inputDict or "includeStateData" not in inputDict or "infer_zip" not in inputDict or "prevailing_wage_report" not in inputDict or "signatories_report" not in inputDict or "all_industry_summary_block" not in inputDict or "open_cases_only" not in inputDict or "include_tables" not in inputDict or "include_summaries" not in inputDict or "only_sig_summaries" not in inputDict or "include_top_viol_tables" not in inputDict or "use_assumptions" not in inputDict or "infer_by_naics" not in inputDict:
        return False

    return True
