from flask import Flask, request, send_file
import os
import platform
if platform.system() == 'Windows' or platform.system() =='Darwin':
    from wagetheft_report_2020_v3 import generateWageReport

else:
    from api.wagetheft_report_2020_v3 import generateWageReport

app = Flask(__name__)


@app.route("/hello")
def helloWorld():
    return "Hello World!"


@app.route("/cloud_b2_storage/create")
def create_bucket():
    # This is a placeholder for the create bucket functionality
    return

#@app.route('/generateWageReport', methods=['POST'])
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
    use_assumptions = 1 #parameters["use_assumptions"] if "use_assumptions" in parameters else "1"
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
    #if target_industry == "":
    #    target_industry = "ALL NAICS"

    #if parameters["target_county"] == "" and parameters["target_city"] == "" :
    if target_county == "" and target_city ==  "":
        target_state = "California"
    
    #if parameters["includeStateJudgements"] == 1 or parameters["includeStateCases"] == 1:
    if includeStateJudgements == 1 or includeStateCases == 1:
        infer_zip = 1
        infer_by_naics = 1

    #log
    '''
    logfile(target_state, target_county, target_city, target_industry, \
                target_organization, \
                includeFedData, includeStateJudgements, includeStateCases, infer_zip, prevailing_wage_report, signatories_report, \
                open_cases_only, include_tables, include_summaries, only_sig_summaries, \
                include_top_viol_tables, use_assumptions, infer_by_naics, YEAR_START_TEXT, YEAR_END_TEXT)
    '''
    
    #out it goes
    report_file_name = generateWageReport(
        target_state, target_county, target_city, target_industry, target_organization, 
        includeFedData, includeStateJudgements, includeStateCases, 
        infer_zip, 
        prevailing_wage_report, signatories_report, open_cases_only, 
        include_tables, include_summaries, only_sig_summaries, include_top_viol_tables, 
        use_assumptions, infer_by_naics, 
        YEAR_START_TEXT, YEAR_END_TEXT)
    
    #download_name_temp = os.path.basename(report_file_name)
    error_msg = "send an email to information@paloaltodatagroup.com"
        
    try:
        return send_file(report_file_name, as_attachment=True) #, download_name=download_name_temp)
    
    except Exception as e:
        return error_msg + ": " + str(e), 500



def checkValidInput(inputDict: dict) -> bool:
    toCheck = ["target_city", "target_industry", "target_organization", 
               "includeFedData", "includeStateJudgements", "includeStateCases", 
               "infer_zip", 
               "prevailing_wage_report", "signatories_report","open_cases_only", 
               "include_tables", "include_summaries", "only_sig_summaries", "include_top_viol_tables",  
               "use_assumptions",  "infer_by_naics", 
               "YEAR_START", "YEAR_END"]
    
    return all(key in inputDict for key in toCheck)


def logfile(target_state, target_county, target_city, target_industry, \
                target_organization, \
                includeFedData, includeStateJudgements, includeStateCases, infer_zip, prevailing_wage_report, signatories_report, \
                open_cases_only, include_tables, include_summaries, only_sig_summaries, \
                include_top_viol_tables, use_assumptions, infer_by_naics, YEAR_START_TEXT, YEAR_END_TEXT):
    import os
    
    rel_path = 'api_logs_/'

    # <-- dir the script is in (import os) plus up one
    script_dir = os.path.dirname(os.path.dirname(__file__))
    abs_path = os.path.join(script_dir, rel_path)
    os.chdir(script_dir)

    if not os.path.exists(script_dir):  # create folder if necessary
        os.makedirs(script_dir)

    if not os.path.exists(abs_path):  # create folder if necessary
        os.makedirs(abs_path)

    file_name = 'api_query'
    file_type = '.txt'

    api_log_file = os.path.join(abs_path, (file_name).replace(
        ' ', '_') + file_type)  # <-- absolute dir and file name

    bugFile = open(api_log_file, 'a')

    bugFile.write("<!DOCTYPE html>")
    bugFile.write("\n")
    bugFile.write("<html><body>")
    bugFile.write("\n")
    bugFile.write("<h1>API CALL</h1>")
    bugFile.write("\n")
    bugFile.write("\n target_state \n")
    bugFile.write(target_state)
    bugFile.write("\n target_county \n")
    bugFile.write(target_county)
    bugFile.write("\n target_city \n")
    bugFile.write(target_city)
    bugFile.write("\n target_industry \n")
    bugFile.write(target_industry)
    bugFile.write("\n target_organization \n")
    bugFile.write(target_organization)
    bugFile.write("\n includeFedData \n")
    bugFile.write(str(includeFedData) )
    bugFile.write("\n includeStateJudgements \n")
    bugFile.write(str(includeStateJudgements))
    bugFile.write("\n includeStateCases \n")
    bugFile.write(str(includeStateCases))
    bugFile.write("\n infer_zip \n")
    bugFile.write(str(infer_zip))
    bugFile.write("\n prevailing_wage_report \n")
    bugFile.write(str(prevailing_wage_report))
    bugFile.write("\n signatories_report \n")
    bugFile.write(str(signatories_report))
    bugFile.write("\n open_cases_only \n")
    bugFile.write(str(open_cases_only))
    bugFile.write("\n include_tables \n")
    bugFile.write(str(include_tables))
    bugFile.write("\n include_summaries \n")
    bugFile.write(str(include_summaries))
    bugFile.write("\n only_sig_summaries \n")
    bugFile.write(str(only_sig_summaries))
    bugFile.write("\n include_top_viol_tables \n")
    bugFile.write(str(include_top_viol_tables))
    bugFile.write("\n use_assumptions \n")
    bugFile.write(str(use_assumptions)) 
    bugFile.write("\n infer_by_naics \n")
    bugFile.write(str(infer_by_naics))
    bugFile.write("\n")
    bugFile.write(YEAR_START_TEXT)
    bugFile.write("\n")
    bugFile.write(YEAR_END_TEXT)
    bugFile.write("\n")
    bugFile.write("<h1>DONE</h1> \n" + "</html></body> \n")
    bugFile.close()

    return

