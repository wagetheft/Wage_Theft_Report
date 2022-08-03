from requests import request as rq
from flask import Flask, request, send_file
from wagetheft_report_2020_v2 import generateWageReport

app = Flask(__name__)


@app.route('/generateWageReport', methods=['POST'])
def generateFile():
    parameters = request.get_json()

    # # ***TEST PARAMETERS HERE FROM GDOC 'test_parameters_generateWageReport'
    # def Signatory_Library():

    #     signatories_CEA = [['Construction'], ["Alcal Specialty Contracting", "Alten Construction", r"\bOveraa\b", "Cahill Contractors", "Clark Construction", "Clark Pacific", "Dolan Concrete Construction", "Dome Construction", "DPR Construction",
    #                                           "Gonsalves and Stronck Construction", "Hathaway Dinwiddie Construction", "Howard Verrinder", "Obayashi", "Lathrop Construction", "McCarthy Building", "Nibbi Bros Associates", "Peck and Hiller", "Roebbelen Contracting",
    #                                           "Roy Van Pelt", "Rudolph and Sletten", "SJ Amoroso Construction"]]

    #     SIGNATORIES = [['All_SIGNATORIES'], signatories_CEA]
    #     return SIGNATORIES

    # def ALL_NAICS_LIBRARY():
    #     construction_terms = ['Construction', '^(23)', r'plumb.*', r'construct.*', 'MANPOWER', 'build', 'floor', r'landscap.*',
    #                           'mechanical', 'elevator', 'plaster', 'frame', 'concrete', 'roof', 'glass', 'tile', r'dry.*wall', 'painting', 'painter', 'remodel', 'cabinet',
    #                           'carpet', 'drafting', 'carpentry', 'AIR SYSTEMS', r'sheet.*metal', 'pipe', 'HOMES', 'asbestos', r'custom.*home', 'maintenance', 'window', r'tree.*trim', 'iron', 'heating',
    #                           r'air.*condition', 'instal', 'insulation', 'MOUNTAIN AIR', 'contract', 'brick', 'paving', 'sprinkler', 'improvement', 'renovat', 'energy',
    #                           'mason', 'marble', 'cooling', '161000', '162000', "^(23)", '230000', "^(3323)", "^(3334)", '333415', '337110', '337212', '339950',
    #                           '423320', '532410', '541350', '561730', '561790', '562910', '623200', r'tree.*service', 'Excavating', 'Restoration', 'MOULDING',
    #                           'electric', 'labor', 'ASPHALT', 'CEMENT', 'FENCING', r'HOME.*REPAIR', r'WOOD.*WORKING', 'ROOTER', r'\bHVAC\b', '^(54132)', r"(!Farm\b)",
    #                           r"(!.*CARE.HOME.*)",
    #                           r"(!RETIREMENT)", r"(!.*GROUP.HOME.*)", r"(!.*RESIDENTIAL.*CARE.*)",
    #                           r"(!CATERING)", r"(!AUTO.*BODY)", r"(!FOOD.*SERVICE)", r"(!CLEANING)", r"(!ELDERLY)"]  # 'PLUMBING',
    #     SPECIFIC_NAICS_INDUSTRY = [['Construction'], construction_terms]
    #     return SPECIFIC_NAICS_INDUSTRY

    # santa_clara_county_cities = ['County of Santa Clara', 'Alum Rock', 'Cambrian Park', 'Campbell', 'Cupertino', 'East Foothills',
    #                              'Fruitdale', 'Gilroy', 'Lexington Hills', 'Los Altos', 'Los Altos Hills', 'Los Gatos', 'Loyola', 'Milpitas', 'Monte Sereno',
    #                              'Morgan Hill', 'Mountain View', 'Palo Alto', 'San Jose', 'San Martin', 'Santa Clara', 'Saratoga', 'Stanford', 'Sunnyvale', '99999']
    # san_diego_county_cities = ['County of San Diego', 'ALPINE', 'WARNER SPRINGS', 'VISTA', 'VALLEY CENTER', 'TEMECULA', 'TECATE', 'SPRING VALLEY', 'SOLANA BEACH', 'SANTEE', 'SANTA YSABEL', 'SANTA CLARITA', 'SAN YSIDRO', 'SAN MARCOS', 'SAN LUIS REY', 'SAN DIEGO', 'SAN CLEMENTE', 'RANCHO SANTA FE', 'RAMONA', 'RAINBOW', 'POWAY', 'PAUMA VALLEY', 'PALA', 'OCEANSIDE', 'NATIONAL CITY',
    #                            'MIRA MESA', 'LEUCADIA', 'LEMON GROVE', 'LAKESIDE', 'LA MESA', 'LA JOLLA', 'JULIAN', 'JAMUL', 'JACUMBA', 'IMPERIAL BEACH', 'IMPERIAL BCH', 'FALLBROOK', 'ESCONDIDO', 'ENCINITAS', 'EL CAJON', 'DULZURA', 'DEL MAR', 'CUDAHY', 'CORONADO', 'CHULA VISTA', 'CARLSBAD', 'CARDIFF BY THE SEA', 'CARDIFF', 'CAMPO', 'CAMP PENDLETON', 'BOULEVARD', 'BORREGO SPRINGS', 'BONSALL', 'BONITA']

    # Cupertino_City_Zipcode = ['Cupertino', '95014', '95015']
    # Santa_Clara_Unincorporated_Zipcode = [
    #     'Santa Clara Unincorporated', '95046', '95044', '95026']
    # SANTA_CLARA_COUNTY_ZIPCODE = ['Santa Clara County'] + \
    #     Santa_Clara_Unincorporated_Zipcode + Cupertino_City_Zipcode

    # zip_codes_backup = {
    #     "san_diego_county_cities": san_diego_county_cities,
    #     "santa_clara_county_cities": santa_clara_county_cities,
    #     "santa_clara_county_zipcode": SANTA_CLARA_COUNTY_ZIPCODE
    # }

    # TARGET_ORGANIZATIONS = [['organizations'], ['GOODWILL']]  # use uppercase

    # TARGET_STATES = [['states'], ['CA']]

    # TEST = 3

    # PARAM_1_TARGET_ZIPCODE = zip_codes_backup["santa_clara_county_zipcode"]

    # # <-- #COPY and PASTE FROM wagetheft_report_2020_v2
    # PARAM_2_TARGET_INDUSTRY = ALL_NAICS_LIBRARY()

    # STATE_FILTER = False
    # ORGANIZATION_FILTER = False
    # CLEAN_OUTPUT = 0
    # FLAG_DUPLICATE = 0  # 1 FLAG_DUPLICATE duplicate, #0 drop duplicates
    # # 1 for open cases only (or nearly paid off), 0 for all cases
    # OPEN_CASES = 0

    # USE_ASSUMPTIONS = 1  # 1 to fill violation and ee gaps with assumed values
    # INFER_NAICS = 1  # 1 to infer code by industry NAICS sector
    # INFER_ZIP = 1  # 1 to infer zip code

    # federal_data = 1  # 1 to include federal data
    # state_data = 1  # 1 to include state data

    # prevailing_wage_terms = [
    #     "(L.C. 223)", "(section 223)", "(LC 223)",  # frequently used
    #     "(L.C. 1771)", "(section 1771)", "(LC 1771)",  # frequently used
    #     "(L.C. 1773.1)", "(section 1773.1)", "(LC 1773.1)",  # frequently used
    #     "(L.C. 1774)", "(section 1774)", "(LC 1774)",  # frequently used
    #     "(L.C. 1775)", "(section 1775)", "(LC 1775)",  # frequently used
    #     "(underpayment)", "(misclassification)", "(prevailing)", "(incorrect)", "(increase)", "(fringe)", "(apprentice)", "(apprenticeship)", "(Public Contract)", "(City of )", "(School District)", "(County)", "(College)", "(University)", "(State of )", "(Library)", "(Fire station)", "(Fire depart)", "(Sheriff)", "(Police)", "(Water District)", "(DBRA)"
    # ]

    # # <-- #COPY and PASTE FROM wagetheft_report_2020_v2
    # SIGNATORY_INDUSTRY = Signatory_Library()

    # prevailing_wage_report = 0
    # signatories_report = 0
    # All_Industry_Summary_Block = False
    # Nonsignatory_Ratio_Block = False

    # TABLES = 1  # 1 for tables and 0 for just text description
    # SUMMARY = 1  # 1 for summaries and 0 for none
    # SUMMARY_SIG = 1
    # TOP_VIOLATORS = 1  # 1 for tables of top violators and 0 for none

    # wageReportFileName = generateWageReport(TEST, zip_codes_backup, PARAM_1_TARGET_ZIPCODE, PARAM_2_TARGET_INDUSTRY, prevailing_wage_terms, SIGNATORY_INDUSTRY, TARGET_ORGANIZATIONS,
    #                                         federal_data, state_data, FLAG_DUPLICATE, STATE_FILTER, ORGANIZATION_FILTER, TARGET_STATES, INFER_ZIP, prevailing_wage_report, signatories_report,
    #                                         All_Industry_Summary_Block, Nonsignatory_Ratio_Block, CLEAN_OUTPUT, OPEN_CASES, TABLES, SUMMARY, SUMMARY_SIG,
    #                                         TOP_VIOLATORS, USE_ASSUMPTIONS, INFER_NAICS)  # returns PDF file
