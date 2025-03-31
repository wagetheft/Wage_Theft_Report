
import warnings
import os
warnings.filterwarnings("ignore", 'This pattern has match groups')


def generateWageReport(target_state, target_county, target_city, target_industry, target_organization):

    warnings.filterwarnings("ignore", category=UserWarning)

    annotation = ("hello world: " + target_state + " " + target_county + " " + 
            target_city + " " + target_industry + " " + target_organization + " !" )

    #rel_path = 'report_output_/'
    #script_dir = os.path.dirname(os.path.dirname(__file__))
    #abs_path = os.path.join(script_dir, rel_path)

    #file_type = ".txt"
    #temp_file_name_PDF = os.path.join(abs_path, ("test report").replace(
    #    ' ', '_') + file_type)  # <-- absolute dir and file name

    temp_file_name_PDF = "test report.txt"

    with open(temp_file_name_PDF, "w+") as file:
        file.write(annotation)

    return temp_file_name_PDF

