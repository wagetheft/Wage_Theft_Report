#wageReportAPI.py

#Last updated
# 6/28/2022 by F. Peterson first file creation 30 lines of code


from flask import Flask, request, send_file
from requests import request #https://pypi.org/project/requests/

from wagetheft_report_2020_v2 import generateWageReport
#generateWageReport from wagetheft_report_2020_v2
"""
def generateWageReport(INPUT_PARAM):
	
    [BODY OF WAGE THEFT REPORT CODE]
    
	return temp_file_name
"""

app = Flask(__name__)
   
@app.route('/generateWageReport', methods=['POST'])

def generateFile():
    parameters = request.get_json()
    param_1_Ziprcode = parameters["param1"] #user input from website field_1
    param_2_Industry = parameters["param1"] #user input from website field_2
    #...
    
    wageReportFileName = generateWageReport(param_1_Ziprcode, param_2_Industry) #returns PDF file
    
    try:
        file_path = "./"  +  wageReportFileName
        return  send_file(file_path, 'fileName')
    
    except Exception as e:
        return  str(e)

generateFile()




        