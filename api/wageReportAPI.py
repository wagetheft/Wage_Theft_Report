from requests import request as rq
from flask import Flask, request, send_file
from api.wagetheft_report_2020_v2 import generateWageReport

app = Flask(__name__)


@app.route("/hello")
def helloWorld():
    return "Hello World!"


@app.route('/generateWageReport', methods=['POST'])
def generateFile():
    parameters = request.get_json()
