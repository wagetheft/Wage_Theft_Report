
from pypdf import PdfWriter
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')


def generateWageReport(target_state, target_county, target_city, target_industry, target_organization):

    warnings.filterwarnings("ignore", category=UserWarning)

    annotation = ("hello world: " + target_state + " " + target_county + " " + 
            target_city + " " + target_industry + " " + target_organization + " !" )


    with open("output.txt", "w") as file:
        file.write(annotation)

    return file

