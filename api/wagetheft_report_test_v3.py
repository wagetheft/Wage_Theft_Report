
from pypdf import PdfWriter
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')

def generateWageReport(target_state, target_county, target_city, target_industry, target_organization):

    warnings.filterwarnings("ignore", category=UserWarning)
    
    pdf_writer = PdfWriter()
    page = pdf_writer.get_page(0)

    annotation = page.new_annotation("hello world: " + target_state + " " + target_county + " " + 
            target_city + " " + target_industry + " " + target_organization + " !" )


    page.annotations.append(annotation)

    with open("output.pdf", "wb") as output_pdf:
        pdf_writer.write(annotation)

    return output_pdf
