from requests import request as rq
from flask import Flask, request, send_file
from api.wagetheft_report_test_v3 import generateWageReport
import os

app = Flask(__name__)


@app.route("/hello")
def helloWorld():
    return "Hello World!"


@app.route('/generateWageReport-test', methods=['POST'])
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
    
    report_file_name = generateWageReport(target_state, target_county, target_city, target_industry, target_organization )

    try:
        return send_file(report_file_name, as_attachment=True)
    except Exception as e:
        return "Server error", 500

def checkValidInput(inputDict: dict) -> bool:
    toCheck = ["target_city", "target_industry", "target_organization" ]
    return all(key in inputDict for key in toCheck)

