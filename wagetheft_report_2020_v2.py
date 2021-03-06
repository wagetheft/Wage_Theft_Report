#wagetheft_report_2020_v2
#Last updated
# 3/3/2019 by F. Peterson first file--170 lines
# 7/8/2019 by F. Peterson
# 9/7/2019 by F. Peterson (v2)
# 1/14/2020 by F. Peterson
# 2/4/2020 by F. Peterson
# 5/1/2019 by F. Peterson numerous updates
# 2/25/2020 by F. Peterson
# 3/5/2020 by F. Peterson locked code before forking into OLSE and SCC WTC versions (donation of ~1,500 lines--@$73,500--from WTC to OLSE)
# 10/11/2020 by F. Peterson
# 10/22/2020 by F. Peterson
# 10/26/2020 by F. Peterson (finally debugged 4am on 10/30/2020)
# 11/1/2020 by F. Peterson still working on this a week later
# 11/9/2020 by F. Peterson fixed infer zip code and added proximity match to allow filtering unpaid theft
# 11/15/2020 by F. Peterson removed duplicates
# 11/16/2020 by F. Peterson adjusted care home keywords
# 11/21/2020 by F. Peterson add name of highest theft business to city summary
# 11/22/2020 by F. Peterson add hospital/care signatory employer list
# 12/16/2020 by F. Peterson add revised industry keyword exclusion function, and revise NAICS library
# 12/24/2020 by F. Peterson revised industry NAICS keyword library w/ regexp and update industry label function to include state NAICS labels
# 1/8/2021 by F. Peterson revised city and zipcode summary per Michael Tayag edits to text (2,711 lines)
# 1/12/2021 by F. Peterson added signatory by industry label (does not work?) --> works 1/13/2021
# 1/13/2021 by F. Peterson added industry summary to regional summary (2,867 lines)
# 1/14/2021 by F. Peterson added a summary of ratio of union to non-union company wage theft (2,880 lines-->@$140k in work with $5 per line and ~10 lines per 8hr day)
# 1/22/2021 by F. Peterson careful output check with 100 x 1,000 dataset
# 1/24/2021 by F. Peterson debug details in signatory library access to industry labels (2,969 lines)
# 9/21/2021 by F. Peterson
# 10/25/2021 by F. Peterson added summary block 1/0 triggers, 4 LoC and added formating to prev wage table
# 2/3/2022 by F. Peterson added organization filter -- add State filter, added new WHD file and debugged, fix date bug, industry lebel adjusted (3,195 lines)
# 2/14/2022 by F. Peterson add WHD prevailing wage inference
# 3/3/2022 by F. Peterson added exclusion terms to construction industry
# 3/4/2022 by F. Peterson fixed a bug with trade/industry header mixup
# 3/4/2022 by F. Peterson fixed a bug with double output
# 3/7/2022 by F. Peterson added county of San Diego
# 6/28/2022 by F. Peterson started adding API (does not run)
# 6/29/2022 by I. Kolli added parameters to API code
# 7/2/2022 by F. Peterson added several more parameters to API code and create output folder if missing: tested and works
# 7/2/2022 by F. Peterson added url pulls for WHD and DOL violations
# 7/14/2022 by F. Peterson update DOL WHD url to updated url 7/13/2022

# Note: add an edit distance comparison
# to fix replace() https://stackoverflow.com/questions/64843109/compiler-issue-assertionerror-on-replace-given-t-or-f-condition-with-string/64856554#64856554

#DUPLICTAE CHECKS:
#duplicated values for ee_pmt_due are zeroed
#no duplicate by case numbers
#no duplicate by legal or trade names
#no duplicates by bw_amt
#no duplicates by ee_pmt_recv (duplicated zero false positives but nbd)

import re

import pandas as pd
import numpy as np

import csv
import math
import editdistance #in the works
from datetime import datetime

import os

import pandas.io.formats.style #https://github.com/pandas-dev/pandas/issues/24884

from string import ascii_letters

import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')


def main():

	#Libraries*********************************************************
	#region by zip codes: (add that) the last zip code is a default for the region
	Santa_Clara_Unincorporated_Zipcode = ['Santa Clara Unincorporated','95046','95044','95026']
	Campbell_City_Zipcode = ['Campbell', '95008', '95009', '95011']
	Cupertino_City_Zipcode = ['Cupertino', '95014', '95015']
	Gilroy_City_Zipcode = ['Gilroy', '95020', '95021']
	Los_Altos_City_Zipcode = ['Los Altos', '94022', '94023', '94024']
	Los_Altos_Hills_City_Zipcode = ['Los Altos Hills', '94022', '94024', '94304', '94306']
	Los_Gatos_City_Zipcode = ['Los Gatos', '95030', '95031', '95032', '95033']
	Milpitas_City_Zipcode = ['Milpitas', '95035', '95036']
	Morgan_Hill_City_Zipcode = ['Morgan Hill', '95037', '95038']
	Mountain_View_City_Zipcode = ['Mountain View','94035', '94039', '94040', '94041', '94042', '94043']
	Palo_Alto_City_Zipcode = ['Palo Alto', '94301', '94302', '94303', '94304', '94306']
	San_Jose_City_Zipcode = ['San Jose', '95002', '95013', '95042', '95101', '95103', '95106', '95108', '95109', '95110', '95111', '95112', '95113', '95115', '95116', '95117', '95118', '95119', '95120', '95121', '95122', '95123', '95124', '95125', 
	'95126', '95127', '95128', '95129', '95130', '95131', '95132', '95133', '95134', '95135', '95136', '95138', '95139', '95140', '95141', '95148', '95150', '95151', '95152', '95153', '95154', '95155', '95156', '95157', '95158', '95159', 
	'95160', '95161', '95164', '95170', '95172', '95173', '95190', '95191', '95192', '95193', '95194', '95196', '95100'] #added 95008 Campbell zipcode due to San Jose violations from that City based companies; added 95100 as generic San Jose code
	Santa_Clara_City_Zipcode = ['Santa Clara', '95050', '95051', '95054', '95052', '95055', '95056', '95053']
	Saratoga_City_Zipcode = ['Saratoga', '95070', '95071']
	Stanford_City_Zipcode = ['Stanford University', '94305', '94309']
	Sunnyvale_City_Zipcode = ['Sunnyvale', '94085', '94086', '94087', '94088', '94089']
	inferred_zipcode = ['99999','99999'] #this tells the system to add records that the zipcode is infered by a city name

	SANTA_CLARA_COUNTY_ZIPCODE = ['Santa Clara County'] + Santa_Clara_Unincorporated_Zipcode + Campbell_City_Zipcode + Cupertino_City_Zipcode + Gilroy_City_Zipcode + Los_Altos_City_Zipcode + Los_Gatos_City_Zipcode + Milpitas_City_Zipcode + Morgan_Hill_City_Zipcode + Mountain_View_City_Zipcode + Palo_Alto_City_Zipcode + San_Jose_City_Zipcode + Santa_Clara_City_Zipcode + Saratoga_City_Zipcode + Stanford_City_Zipcode + Sunnyvale_City_Zipcode + inferred_zipcode
	#region by cities (don't use this one except for specific situation)
	santa_clara_county_cities = ['County of Santa Clara', 'Alum Rock', 'Cambrian Park', 'Campbell', 'Cupertino', 'East Foothills', 
	'Fruitdale', 'Gilroy', 'Lexington Hills', 'Los Altos', 'Los Altos Hills', 'Los Gatos', 'Loyola', 'Milpitas', 'Monte Sereno', 
	'Morgan Hill', 'Mountain View', 'Palo Alto', 'San Jose', 'San Martin', 'Santa Clara', 'Saratoga', 'Stanford', 'Sunnyvale', '99999']

	San_Diego_City_Zipcode = ['San Diego', '92105', '92106', '92107', '92108', '92113', '92114', '92115', '92116', '92117', '92122', '92123', '92124', '92126', '92127', '92101', '92102', '92103', '92104', '92109', '92110', '92111', '92112', '92119', '92120', '92121', '92128', '92129', '92130', '92131', '92136', '92137', '92145', '92147', '92132', '92134', '92135', '92138', '92139', '92140', '92142', '92149', '92150', '92152', '92158', '92159', '92160', '92161', '92162', '92163', '92164', '92165', '92171', '92172', '92174', '92179', '92182', '92184', '92192', '92193', '92195', '92196', '92197', '92153', '92154', '92155', '92166', '92167', '92168', '92169', '92170', '92175', '92176', '92177', '92186', '92187', '92190', '92191', '92198', '92199']
	San_Diego_Unincorporated_Zipcode = ['San Diego Unincorporated','92672', '92592', '92194', '92178', '92173', '92143', '92133', '92118', '92096', '92093', '92092', '92091', '92090', '92088', '92086', '92085', '92084', '92083', '92082', '92081', '92079', '92078', '92075', '92074', '92072', '92071', '92070', '92069', '92068', '92067', '92066', '92065', '92064', '92061', '92060', '92059', '92058', '92057', '92056', '92055', '92054', '92052', '92051', '92049', '92046', '92040', '92039', '92038', '92037', '92036', '92033', '92030', '92029', '92028', '92027', '92026', '92025', '92024', '92023', '92022', '92021', '92020', '92019', '92018', '92014', '92013', '92011', '92010', '92009', '92008', '92007', '92004', '92003', '91990', '91987', '91980', '91979', '91978', '91977', '91976', '91963', '91962', '91951', '91950', '91948', '91947', '91946', '91945', '91944', '91943', '91942', '91941', '91935', '91934', '91933', '91932', '91931', '91921', '91917', '91916', '91915', '91914', '91913', '91912', '91911', '91910', '91909', '91908', '91906', '91905', '91903', '91902', '91901'] 

	SAN_DIEGO_COUNTY_ZIPCODE = ['San Diego County'] + San_Diego_City_Zipcode + San_Diego_Unincorporated_Zipcode + inferred_zipcode
	san_diego_county_cities = ['County of San Diego', 'ALPINE','WARNER SPRINGS', 'VISTA', 'VALLEY CENTER', 'TEMECULA', 'TECATE', 'SPRING VALLEY', 'SOLANA BEACH', 'SANTEE', 'SANTA YSABEL', 'SANTA CLARITA', 'SAN YSIDRO', 'SAN MARCOS', 'SAN LUIS REY', 'SAN DIEGO', 'SAN CLEMENTE', 'RANCHO SANTA FE', 'RAMONA', 'RAINBOW', 'POWAY', 'PAUMA VALLEY', 'PALA', 'OCEANSIDE', 'NATIONAL CITY', 'MIRA MESA', 'LEUCADIA', 'LEMON GROVE', 'LAKESIDE', 'LA MESA', 'LA JOLLA', 'JULIAN', 'JAMUL', 'JACUMBA', 'IMPERIAL BEACH', 'IMPERIAL BCH', 'FALLBROOK', 'ESCONDIDO', 'ENCINITAS', 'EL CAJON', 'DULZURA', 'DEL MAR', 'CUDAHY', 'CORONADO', 'CHULA VISTA', 'CARLSBAD', 'CARDIFF BY THE SEA', 'CARDIFF', 'CAMPO', 'CAMP PENDLETON', 'BOULEVARD', 'BORREGO SPRINGS', 'BONSALL', 'BONITA']

	Stockton_City_Zipcode = ['Stockton', '95201', '95202', '95203', '95204', '95205', '95206', '95207', '95208', '95209', '95210', '95211', '95212', '95213', '95215', '95219', '95267', '95269', '95296', '95297']
	Fresno_City_Zipcode = ['Fresno', '93611', '93612', '93619', '93650', '93701', '93702', '93703', '93704', '93705', '93706', '93710', '93711', '93720', '93721', '93722', '93723', '93725', '93726', '93727', '93728', '93730']
	
	ALL_ZIPCODES = ['00000']
	Custom_Zipcode = ['95112', '95113']

	zip_codes_backup = { 
		"san_diego_city_zipcode": San_Diego_City_Zipcode, 
		"san_diego_unincorporated_zipcode": San_Diego_Unincorporated_Zipcode,
		"san_diego_county_cities": san_diego_county_cities,
		"san_diego_county_zipcode":SAN_DIEGO_COUNTY_ZIPCODE,
		"santa_clara_county_cities": santa_clara_county_cities,
		"santa_clara_county_zipcode": SANTA_CLARA_COUNTY_ZIPCODE,
		}
	zipcode = {} # add a function to read jurisdiction and zipode list from csv file

	delete_terms = ['County of Santa Clara']

	#organizations
	TARGET_ORGANIZATIONS = [['organizations'],['GOODWILL']] #use uppercase
	#state
	TARGET_STATES = [['states'],['CA']]

	#industries
	#ALL_INDUSTRIES = [['industry'],['industry']]


	#settings****************************************************
	TEST = 2 #0 for normal run; 1 for custom test dataset (unified_test); 2 for small dataset (first 1,000 of each file)
	
	#TARGET_ZIPCODE = SAN_DIEGO_COUNTY_ZIPCODE #SANTA_CLARA_COUNTY_ZIPCODE #enter *_zipcode list; use ALL_ZIPCODES for all zip codes
	PARAM_1_TARGET_ZIPCODE = zip_codes_backup["santa_clara_county_zipcode"]
	
	PARAM_2_TARGET_INDUSTRY = ALL_NAICS_LIBRARY() #NAICS Industies -- change in ALL_NAICS_LIBRARY()

	STATE_FILTER = False #True for all of california, else False for just santa_clara_county_cities
	ORGANIZATION_FILTER  = False # True to filter, False no filter for specific organizantion name, see TARGET_ORGANIZATIONS
	
	CLEAN_OUTPUT = 0 #1 to output cleaned files for violations and suppliers ## this is a draft and does nothing for now
	FLAG_DUPLICATE = 0 #1 FLAG_DUPLICATE duplicate, #0 drop duplicates
	OPEN_CASES = 0 #1 for open cases only (or nearly paid off), 0 for all cases
	
	USE_ASSUMPTIONS = 1 #1 to fill violation and ee gaps with assumed values
	INFER_NAICS = 1 #1 to infer code by industry NAICS sector
	INFER_ZIP = 1 #1 to infer zip code

	federal_data = 1 #1 to include federal data
	state_data = 1 #1 to include state data

	#unused YEAR_START_FILTER = '01/01/2000' #1995 is earliest case -- this does not work
	#unused YEAR_END_FILTER = '09/01/2019'

	prevailing_wage_terms = [ #https://www.dir.ca.gov/public-works/californiaprevailingwagelaws.pdf
		"(L.C. 223)", "(section 223)", "(LC 223)", #frequently used
		"(L.C. 1720)", "(section 1720)", "(LC 1720)", 
		"(L.C. 1720.2)", "(section 1720.2)", "(LC 1720.2)", 
		"(L.C. 1720.3)", "(section 1720.3)", "(LC 1720.3)", 
		"(L.C. 1720.4)", "(section 1720.4)", "(LC 1720.4)",
		"(L.C. 1720.5)", "(section 1720.5)", "(LC 1720.5)",
		"(L.C. 1720.6)", "(section 1720.6)", "(LC 1720.6)", 
		"(L.C. 1720.7)", "(section 1720.7)", "(LC 1720.7)", 
		"(L.C. 1721)", "(section 1721)", "(LC 1721)",
		"(L.C. 1722)", "(section 1722)", "(LC 1722)",
		"(L.C. 1722.1)", "(section 1722.1)", "(LC 1722.1)",
		"(L.C. 1723)", "(section 1723)", "(LC 1723)",
		"(L.C. 1724)", "(section 1724)", "(LC 1724)",
		"(L.C. 1725.5)", "(section 1725.5)", "(LC 1725.5)",
		"(L.C. 1726)", "(section 1726)", "(LC 1726)",
		"(L.C. 1727)", "(section 1727)", "(LC 1727)",
		"(L.C. 1728)", "(section 1728)", "(LC 1728)",
		"(L.C. 1729)", "(section 1729)", "(LC 1729)",
		"(L.C. 1730)", "(section 1730)", "(LC 1730)",
		"(L.C. 1734)", "(section 1734)", "(LC 1734)",
		"(L.C. 1735)", "(section 1735)", "(LC 1735)",
		"(L.C. 1736)", "(section 1736)", "(LC 1736)",
		"(L.C. 1740)", "(section 1740)", "(LC 1740)",
		"(L.C. 1741)", "(section 1741)", "(LC 1741)",
		"(L.C. 1741.1)", "(section 1741.1)", "(LC 1741.1)",
		"(L.C. 1742)", "(section 1742)", "(LC 1742)",
		"(L.C. 1742.1)", "(section 1742.1)", "(LC 1742.1)",
		"(L.C. 1743)", "(section 1743)", "(LC 1743)",
		"(L.C. 1750)", "(section 1750)", "(LC 1750)",
		"(L.C. 1770)", "(section 1770)", "(LC 1770)",
		"(L.C. 1771)", "(section 1771)", "(LC 1771)", #frequently used
		"(L.C. 1771.1)", "(section 1771.1)", "(LC 1771.1)",
		"(L.C. 1771.2)", "(section 1771.2)", "(LC 1771.2)",
		"(L.C. 1771.3)", "(section 1771.3)", "(LC 1771.3)",
		"(L.C. 1771.4)", "(section 1771.4)", "(LC 1771.4)",
		"(L.C. 1771.5)", "(section 1771.5)", "(LC 1771.5)",
		"(L.C. 1771.6)", "(section 1771.6)", "(LC 1771.6)",
		"(L.C. 1771.7)", "(section 1771.7)", "(LC 1771.7)",
		"(L.C. 1772)", "(section 1772)", "(LC 1772)",
		"(L.C. 1773)", "(section 1773)", "(LC 1773)",
		"(L.C. 1773.1)", "(section 1773.1)", "(LC 1773.1)", #frequently used
		"(L.C. 1773.2)", "(section 1773.2)", "(LC 1773.2)",
		"(L.C. 1773.3)", "(section 1773.3)", "(LC 1773.3)",
		"(L.C. 1773.4)", "(section 1773.4)", "(LC 1773.4)",
		"(L.C. 1773.5)", "(section 1773.5)", "(LC 1773.5)",
		"(L.C. 1773.6)", "(section 1773.6)", "(LC 1773.6)",
		"(L.C. 1773.7)", "(section 1773.7)", "(LC 1773.7)",
		"(L.C. 1773.8)", "(section 1773.8)", "(LC 1773.8)",
		"(L.C. 1773.9)", "(section 1773.9)", "(LC 1773.9)",
		"(L.C. 1773.11)", "(section 1773.11)", "(LC 1773.11)",
		"(L.C. 1774)", "(section 1774)", "(LC 1774)", #frequently used
		"(L.C. 1775)", "(section 1775)", "(LC 1775)", #frequently used
		"(L.C. 1776)", "(section 1776)", "(LC 1776)",
		"(L.C. 1777)", "(section 1777)", "(LC 1777)",
		"(L.C. 1777.1)", "(section 1777.1)", "(LC 1777.1)",
		"(L.C. 1777.5)", "(section 1777.5)", "(LC 1777.5)",
		"(L.C. 1777.6)", "(section 1777.6)", "(LC 1777.6)",
		"(L.C. 1777.7)", "(section 1777.7)", "(LC 1777.7)",
		"(L.C. 1778)", "(section 1778)", "(LC 1778)",
		"(L.C. 1779)", "(section 1779)", "(LC 1779)",
		"(L.C. 1780)", "(section 1780)", "(LC 1780)",
		"(L.C. 1781)", "(section 1781)", "(LC 1781)",
		"(L.C. 1782)", "(section 1782)", "(LC 1782)", 
		"(L.C. 1784)", "(section 1784)", "(LC 1784)",
		"(L.C. 1810)", "(section 1810)", "(LC 1810)",
		"(L.C. 1811)", "(section 1811)", "(LC 1811)",
		"(L.C. 1812)", "(section 1812)", "(LC 1812)",
		"(L.C. 1813)", "(section 1813)", "(LC 1813)",
		"(L.C. 1814)", "(section 1814)", "(LC 1814)",
		"(L.C. 1815)", "(section 1815)", "(LC 1815)",
		"(L.C. 1860)", "(section 1860)", "(LC 1860)",
		"(L.C. 1861)", "(section 1861)", "(LC 1861)",

		"(P.C. 4104)", "(section 4104)", "(PC 4104)",
		"(P.C. 6823)", "(section 6823)", "(PC 6823)",
		"(P.C. 6953)", "(section 6953)", "(PC 6953)",
		"(P.C. 10128)", "(section 10128)", "(PC 10128)",
		"(P.C. 20672)", "(section 20672)", "(PC 20672)",
		"(P.C. 20919.27)", "(section 20919.27)", "(PC 20919.27)",

		"(P.R.C. 5366)", "(section 5366)", "(PRC 5366)",
		"(P.R.C. 43501)", "(section 43501)", "(PRC 43501)",
		"(P.R.C. 75075)", "(section 75075)", "(PRC 75075)",

		"(P.U.C. 399.13)", "(section 399.13)", "(PUC 399.13)",
		"(P.U.C. 3354.1)", "(section 3354.1)", "(PUC 3354.1)",
		"(P.U.C. 16461.5)", "(section 16461.5)", "(PUC 16461.5)",
		"(P.U.C. 100152)", "(section 100152)", "(PUC 100152)",
		"(P.U.C. 102284)", "(section 102284)", "(PUC 102284)",
		"(P.U.C. 102288)", "(section 102288)", "(PUC 102288)",
		"(P.U.C. 103396)", "(section 103396)", "(PUC 103396)",
		"(P.U.C. 130242)", "(section 130242)", "(PUC 130242)",

		"(S.H.C. 670.1)", "(section 670.1)", "(SHC 670.1)",
		"(S.H.C. 27189)", "(section 27189)", "(SHC 27189)",

		"(W.C. 8007)", "(section 8007)", "(WC 8007)",

		"(W.I.C. 1752.9)", "(section 1752.9)", "(WIC 1752.9)",

		"(E.C. 17066)", "(section 17066)", "(EC 17066)",
		"(E.C. 81704)", "(section 81704)", "(EC 81704)",

		"(F.G.C. 1350)", "(section 1350)", "(FGC 1350)",
		"(F.G.C. 1501.5)", "(section 1501.5)", "(FGC 1501.5)",

		"(G.C. 5956.8)", "(section 5956.8)", "(GC 5956.8)",
		"(G.C. 14453)", "(section 14453)", "(GC 14453)",
		"(G.C. 14670.36)", "(section 14670.36)", "(GC 14670.36)",
		"(G.C. 14955)", "(section 14955)", "(GC 14955)",
		"(G.C. 14975)", "(section 14975)", "(GC 14975)",
		"(G.C. 14976)", "(section 14976)", "(GC 14976)",
		"(G.C. 54253)", "(section 54253)", "(GC 54253)",
		"(G.C. 63036)", "(section 63036)", "(GC 63036)",
		"(G.C. 91533)", "(section 91533)", "(GC 91533)",

		"(H.S.C. 33425)", "(section 33425)", "(HSC 33425)",
		"(H.S.C. 50675.4)", "(section 50675.4)", "(HSC 50675.4)",
		"(H.S.C. 50746)", "(section 50746)", "(HSC 50746)",
		"(H.S.C. 50749)", "(section 50749)", "(HSC 50749)",
		"(H.S.C. 50766)", "(section 50766)", "(HSC 50766)",
		"(H.S.C. 50898.2)", "(section 50898.2)", "(HSC 50898.2)",
		"(H.S.C. 50953)", "(section 50953)", "(HSC 50953)",
		"(H.S.C. 125290.65)", "(section 125290.65)", "(HSC 125290.65)",

		"(underpayment)", "(misclassification)", "(prevailing)", "(incorrect)", "(increase)", "(fringe)", 
		"(apprentice)", "(apprenticeship)", "(Public Contract)"
		"(City of )", "(School District)", "(County)", "(College)", "(University)", "(State of )", "(Library)", 
		"(Fire station)", "(Fire depart)","(Sheriff)", "(Police)", "(Water District)", 
		"(DBRA)"
		]

	#add for WHD 
	# Special Minimum Wages - Apprentices: flsa_smwap_violtn_cnt, flsa_smwap_bw_atp_amt, flsa_smwap_ee_atp_cnt

	SIGNATORY_INDUSTRY = Signatory_Library()

	#report output block settings****************************************************
	prevailing_wage_report = 0 #1 to label prevailing wage violation records and list companies with prevailing wage violations, 0 not to
	signatories_report = 0 #1 to include signatories (typically, this report is only for union compliance officers) 0 to exclude signatories

	All_Industry_Summary_Block = False
	Nonsignatory_Ratio_Block = False
	
	TABLES = 1 #1 for tables and 0 for just text description
	SUMMARY = 1 #1 for summaries and 0 for none
	SUMMARY_SIG = 1 #1 for summaries only of regions with significant wage theft (more than $10,000)
	TOP_VIOLATORS = 1 #1 for tables of top violators and 0 for none
	
	#!!!manually add to report***********************************************************
		# (1)generate report from http://wagetheftincitieslikemine.site/
		# (2)geocode data https://www.geocod.io
		# (3)generate Tableau bubble plot with location data https://public.tableau.com/profile/forest.peterson#!

	#checks
		#https://webapps.dol.gov/wow/
		#https://www.goodjobsfirst.org/violation-tracker


	#API call***************************************************************************
	generateWageReport(TEST, zip_codes_backup, PARAM_1_TARGET_ZIPCODE, PARAM_2_TARGET_INDUSTRY, prevailing_wage_terms, SIGNATORY_INDUSTRY, TARGET_ORGANIZATIONS, 
	federal_data, state_data, FLAG_DUPLICATE, STATE_FILTER, ORGANIZATION_FILTER, TARGET_STATES, INFER_ZIP, prevailing_wage_report, signatories_report, 
	All_Industry_Summary_Block, Nonsignatory_Ratio_Block, CLEAN_OUTPUT, OPEN_CASES, TABLES, SUMMARY, SUMMARY_SIG, 
	TOP_VIOLATORS, USE_ASSUMPTIONS, INFER_NAICS)

#Functions*************************************************
	
def generateWageReport(TEST, zip_codes_backup, TARGET_ZIPCODE, TARGET_INDUSTRY, prevailing_wage_terms, SIGNATORY_INDUSTRY, TARGET_ORGANIZATIONS, 
federal_data, state_data, FLAG_DUPLICATE, STATE_FILTER, ORGANIZATION_FILTER, TARGET_STATES, INFER_ZIP, prevailing_wage_report, signatories_report, 
All_Industry_Summary_Block, Nonsignatory_Ratio_Block, CLEAN_OUTPUT, OPEN_CASES, TABLES, SUMMARY, SUMMARY_SIG, 
TOP_VIOLATORS, USE_ASSUMPTIONS, INFER_NAICS):
	#3/7/2022 bugFile = open('report_output/debug.html', 'w')
	#3/7/2022 debug_fileSetup_def(bugFile)

	#TEST
	if TEST == 0 or TEST == 1: 
		TEST_CASES = 1000000000 # read all records
	else: #TEST == 2 #short set--use first 1000 for debugging 
		TEST_CASES = 100

	#region definition
	#default
	JURISDICTON_NAME = "City of "
	default_region_name = TARGET_ZIPCODE[len(TARGET_ZIPCODE) -1] 
	default_region_name = default_region_name.replace(default_region_name[len(default_region_name) - 3], "XXX")
	default_region = [TARGET_ZIPCODE[0], TARGET_ZIPCODE[0]] #options: santa_clara_county_cities for region or for a city use the city name "san jose"; this constant is used to search for missing zip codes

	if (TARGET_ZIPCODE[0] == "Santa Clara County"):
		JURISDICTON_NAME = " "
		default_region_name = '99999'
		default_region = zip_codes_backup["santa_clara_county_cities"] # backup in case forget to put city list

	if (TARGET_ZIPCODE[0] == "San Diego County"):
		JURISDICTON_NAME = " "
		default_region_name = '99999'
		default_region = zip_codes_backup["san_diego_county_cities"] # backup in case forget to put city list
	
	if (TARGET_ZIPCODE[0] == '00000'):
		JURISDICTON_NAME = "All regions " 
		default_region_name = '99999'
		default_region = ""
	
	target_city = JURISDICTON_NAME + TARGET_ZIPCODE[0]  #target jurisdiction: Report Title block and file name "<h1>DRAFT REPORT: Wage Theft in the jurisdiction of... "
	target_industry = TARGET_INDUSTRY[0][0]

	temp_file_name = 'report_output/' + target_city.replace(' ','_') +'_theft_summary_'+ target_industry.replace(' ','_') +'.html'
	if not os.path.exists('report_output/'):
		os.makedirs('report_output/')

	#Concat data***************************************************************************
	df_csv = Read_Violation_Data(TEST, TEST_CASES, federal_data, state_data) # file names hard punched in function
	df_csv = df_csv.replace('\s',' ', regex=True) # remove line returns

	##rename columns************************************
	df_csv = Cleanup_Number_Columns(df_csv)
	df_csv = Cleanup_Text_Columns(df_csv)
	df_csv = Define_Column_Types(df_csv)

	DF_OG = df_csv.copy() #hold a copy of the original data

	df_csv = DropDuplicateRecords(df_csv, FLAG_DUPLICATE) #remove duplicate cases using case_id and violation as a unique key
	df_csv = DropDuplicateBackwage(df_csv, FLAG_DUPLICATE)

	#unused df_csv = FilterForDate(df_csv, YEAR_START, YEAR_END) #filter for date

	#Inferences*********************************
	#zip codes
	if not df_csv.empty and 'zip_cd' in df_csv.columns: 
		#df_csv['zip_cd'] = df_raw['zip_cd'].astype(str).str.zfill(5)
		
		#def generate_placeholder_zip(row): https://stackoverflow.com/questions/57048689/update-or-replace-value-in-df-when-conditions-are-met/57048934#57048934
		#if pd.isnull(row['zip'] ):
		#row['zip'] =row['city']+'_ZIPCODE'
		#return row
		#df.apply(generate_placeholder_zip, axis =1)

		#unused if isinstance(df_csv['zip_cd'][len(df_csv['zip_cd'])-1], (int, float, complex)): #removed long

		##CLEAN
		df_csv['zip_cd'] = df_csv['zip_cd'].replace('nan', False, regex=True) #clean out string 'nan' so they don't mess stuff up
		##FILTER
		if STATE_FILTER is True:
			df_csv = Filter_for_State(df_csv, TARGET_STATES)
		
		if INFER_ZIP == 1: #infer zip codes
			df_csv = InferZipcodeFromCityName(df_csv, default_region, default_region_name) #zipcodes by city name
			#df_csv = InferZipcodeFromCompanyName(df_csv, default_region, default_region_name) #zipcodes by company name
			#df_csv = InferZipcodeFromJurisdictionName(df_csv, default_region, default_region_name) #zipcodes by jurisdiction name
		
		if TARGET_ZIPCODE[0] is not '00000':
			df_csv = Filter_for_Zipcode(df_csv, TARGET_ZIPCODE)

	#PREVAILING WAGE
	if federal_data == 1:
		df_csv = infer_WHD_prevailing_wage_violation(df_csv)
	if prevailing_wage_report == 1:
		df_csv = InferPrevailingWageAndColumnFlag(df_csv, prevailing_wage_terms)
	
	df_csv = Label_Industry(df_csv, INFER_NAICS, TARGET_INDUSTRY)
	
	#unused df_csv = Filter_for_Target_Industry(df_csv,TARGET_INDUSTRY) ##debug 12/23/2020 <-- run here for faster time but without global summary

	df_csv = DropDuplicateRecords(df_csv, FLAG_DUPLICATE) #remove duplicate cases using case_id and violation as a unique key
	df_csv = DropDuplicateBackwage(df_csv, FLAG_DUPLICATE)

	#infer signatories
	if "Signatory" not in df_csv.columns: df_csv["Signatory"] = 0
	if 'signatory_industry' not in df_csv.columns: df_csv['signatory_industry'] = ""
	df_csv = InferSignatoriesFromNameAndFlag(df_csv, SIGNATORY_INDUSTRY)
	df_csv = InferSignatoryIndustryAndLabel(df_csv, SIGNATORY_INDUSTRY)
	#unused df_csv_temp_address = InferSignatoriesFromAddressAndFlag(df_csv, signatory_address_list)
	#unused df_csv = InferSignatoriesFromNameAndAddressFlag(df_csv, signatory_list, signatory_address_list)
	#unused df_csv = df_csv.append(df_csv_temp_address)
	df_csv = InferAgencyFromCaseIDAndLabel(df_csv, 'juris_or_proj_nm')

	#use for debugging df_csv.to_csv('report_output/bug_report_temp_in.csv') #debug outfile

	#Cleanup********************************************************
	df = df_csv.copy() #new df

	df = Cleanup_Number_Columns(df)
	df = Cleanup_Text_Columns(df)
	df = CleanUpAgency(df, 'juris_or_proj_nm')

	#add data**********************************************************
	df['wages_owed'] = (df['bw_amt'] - df["ee_pmt_recv"])
	df['wages_owed'] = np.where(df['wages_owed'] < 0, 0, df['wages_owed'] )
	df['cmp_assd_cnt'] = np.where(df['cmp_assd_cnt'] < 0, 0, df['cmp_assd_cnt'])

	df['interest_owed'] = (df['Interest_Balance_Due'] - df["Interest_Payments_Recd"])
	df['interest_owed'] = np.where(df['interest_owed'] < 0, 0, df['interest_owed'] )

	#make assumptions****************************************************

	##note--estimate back wage, penaly, and interest, based on violation

	total_ee_violtd = df['ee_violtd_cnt'].sum()
	total_bw_atp = df['bw_amt'].sum()
	total_case_violtn = df['violtn_cnt'].sum()
	if USE_ASSUMPTIONS == 1 and not df.empty:
		
		#employee violated
		df['ee_violtd_cnt'] = df['ee_violtd_cnt'].fillna(1) #DLSE cases are for one employee -- introduces an error when the dataset is violation records--need to remove duplicates
		df['ee_violtd_cnt'] = np.where(df['ee_violtd_cnt'] == "", 1 , df['ee_violtd_cnt']) # catch if na misses
		df['ee_violtd_cnt'] = np.where(df['ee_violtd_cnt'] == 0, 1 , df['ee_violtd_cnt']) # catch if na misses
		df['ee_violtd_cnt'] = np.where(df['ee_violtd_cnt'] == False, 1 , df['ee_violtd_cnt']) # catch if na misses
		total_ee_violtd = df['ee_violtd_cnt'].sum() #overwrite

		#by issue count
		if 'violation' in df.columns:
			df['violtn_cnt'] = df['violtn_cnt'].fillna(df['violation'].str.count("Issue")) #assume mean
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == "", df['violation'].str.count("Issue"), df['violtn_cnt']) # catch if na misses
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == 0, df['violation'].str.count("Issue"), df['violtn_cnt'])
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == False, df['violation'].str.count("Issue"), df['violtn_cnt'])
			total_case_violtn = df['violtn_cnt'].sum() #overwrite

		#violations
		total_case_violtn = max(df['violtn_cnt'].sum(), df['ee_violtd_cnt'].sum() ) #safe assumption: violation count is always more then the number of employees
		
		df['assumed_backwage'] = 0 
		df = infer_wage_penalty(df) # B backwage, M monetary penalty

		if total_ee_violtd != 0: #lock for divide by zero error
			#violation estimate by mean
			estimated_violations_per_emp = max(total_case_violtn//total_ee_violtd, 1)
			df['violtn_cnt'] = df['violtn_cnt'].fillna(estimated_violations_per_emp ) #assume mean
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == "", (estimated_violations_per_emp ), df['violtn_cnt']) # catch if na misses
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == 0, (estimated_violations_per_emp ), df['violtn_cnt']) 
			df['violtn_cnt'] = np.where(df['violtn_cnt'] == False, (estimated_violations_per_emp ), df['violtn_cnt']) 

			#estimated_bw_plug = max(total_bw_atp//total_ee_violtd,1000)

			#montetary penalty only if backwage is zero -- often penalty is in the backwage column
			# df['cmp_assd_cnt'] = np.where(df['bw_amt'] == "", estimated_bw_plug * 0.25, df['cmp_assd_cnt'])
			# df['cmp_assd_cnt'] = np.where(df['bw_amt'] == 0, estimated_bw_plug * 0.25, df['cmp_assd_cnt'])
			# df['cmp_assd_cnt'] = np.where(df['bw_amt'] == False, estimated_bw_plug * 0.25, df['cmp_assd_cnt'])

			#backwage estimate
			# df['bw_amt'] = df['bw_amt'].fillna(estimated_bw_plug) #assume mean or $1000 minimum to estimate in zero situation
			# df['bw_amt'] = np.where(df['bw_amt'] == "", estimated_bw_plug, df['bw_amt']) # catch if na misses
			# df['bw_amt'] = np.where(df['bw_amt'] == 0, estimated_bw_plug, df['bw_amt']) # catch if na misses
			# df['bw_amt'] = np.where(df['bw_amt'] == False, estimated_bw_plug, df['bw_amt']) # catch if na misses
			# total_bw_atp = df['bw_amt'].sum() #overwrite

		df['wages_owed'] = (df['bw_amt'] - df["ee_pmt_recv"])
		df['wages_owed'] = np.where((df['wages_owed'] < 0), 0, df['wages_owed'] ) #overwrite

		#-->
		#df_csv.to_csv('report_output/bug_report_temp_in.csv') #debug outfile
		df = calculate_interest_owed(df)


	#final sum after assumptions
	df['wages_owed'] = np.where(df['wages_owed'] < 0, 0, df['wages_owed'] )

	df['backwage_owed'] = df['wages_owed'] + df['cmp_assd_cnt'] + df['interest_owed']
	#add ee_pmt_due check if equal ?

	if 'Paid' in df.columns:
		df['backwage_owed'] = np.where(df['Paid'] == 'Y', 0, df['backwage_owed'] ) # zero out if documented as paid
		df['cmp_assd_cnt'] = np.where(df['Paid'] == 'Y', 0, df['cmp_assd_cnt'] ) # zero out if documented as paid
		df['interest_owed'] = np.where(df['Paid'] == 'Y', 0, df['interest_owed'] ) # zero out if documented as paid

	out_target = df.copy() #at some point, save this file for quicker reports and only run up to here when a new dataset is introduced

	##filter for reporting************************************
	out_target = Filter_for_Target_Industry(out_target,TARGET_INDUSTRY)
	if OPEN_CASES == 1: out_target = RemoveCompletedCases(out_target)
	if ORGANIZATION_FILTER: out_target = Filter_for_Target_Organization(out_target,TARGET_ORGANIZATIONS)
	
	##filter
	unique_legalname_sig = GroupByX(out_target, 'legal_nm')

	#unique_legalname_sig  = unique_legalname_sig[~unique_legalname_sig.index.duplicated()]

	if 'Prevailing' in df.columns:
		out_prevailing_target = unique_legalname_sig.loc[unique_legalname_sig['Prevailing'] == 1]
	if "Signatory" in df.columns:
		out_signatory_target = unique_legalname_sig.loc[unique_legalname_sig["Signatory"] == 1]

	if signatories_report == 0 and 'Signatory' and 'legal_nm' and 'trade_nm' in out_target.columns:
		#unused out_target = out_target.loc[out_target['Signatory']!=1] #filter
		out_target['legal_nm'] = np.where(out_target['Signatory']==1, "masked", out_target['legal_nm'] )
		out_target['trade_nm'] = np.where(out_target['Signatory']==1, "masked", out_target['trade_nm'] )
		out_target['street_addr'] = np.where(out_target['Signatory']==1, "masked", out_target['street_addr'] )
		out_target['case_id'] = np.where(out_target['Signatory']==1, "masked", out_target['case_id'] )
		if 'DIR_Case_Name' in out_target.columns:
			out_target['DIR_Case_Name'] = np.where(out_target['Signatory']==1, "masked", out_target['DIR_Case_Name'] )

	#create csv output file**********************************
	#option to_csv_test_header = ['trade_nm','legal_nm','industry']
	#option out_target[to_csv_test_header]
	
	out_target = out_target.drop_duplicates(keep='last') #added to prevent bug that outputs 2x
	out_target.to_csv('report_output/' + target_city.replace(' ','_') + '_Theft_'+ TARGET_INDUSTRY[0][0].replace(' ','_') +'.csv', encoding="utf-8-sig")
	
	#summary
	all_unique_legalname = GroupByMultpleCases(out_target, 'legal_nm')
	all_unique_legalname = all_unique_legalname.sort_values(by=['records'], ascending=False )
	all_agency_df = GroupByMultpleAgency(out_target)
	all_agency_df = all_agency_df.sort_values(by=['records'], ascending=False )

	#group repeat offenders************************************
	out_counts = out_target.copy() #hold for case counts

	unique_legalname = GroupByX(out_target, 'legal_nm')

	unique_address = GroupByMultpleCases(out_target, 'street_addr')
	unique_legalname2 = GroupByMultpleCases(out_target, 'legal_nm')
	unique_tradename = GroupByMultpleCases(out_target, 'trade_nm')
	unique_agency = GroupByMultpleCases(out_target, 'juris_or_proj_nm')
	unique_owner = GroupByMultpleCases(out_target, 'Jurisdiction_region_or_General_Contractor')
	agency_df = GroupByMultpleAgency(out_target)

	out_target = unique_legalname.copy()

	#sort and format************************************

	##sort for table
	out_sort_ee_violtd = out_target.sort_values(by=['ee_violtd_cnt'], ascending=False )
	out_sort_bw_amt = out_target.sort_values(by=['bw_amt'], ascending=False )
	out_sort_repeat_violtd = out_target.sort_values(by=['records'], ascending=False )
	
	unique_address = unique_address.sort_values(by=['records'], ascending=False )
	unique_legalname = unique_legalname.sort_values(by=['records'], ascending=False )
	unique_legalname2 = unique_legalname2.sort_values(by=['records'], ascending=False )
	unique_tradename = unique_tradename.sort_values(by=['records'], ascending=False )
	unique_agency = unique_agency.sort_values(by=['records'], ascending=False )
	unique_owner = unique_owner.sort_values(by=['records'], ascending=False )
	agency_df = agency_df.sort_values(by=['records'], ascending=False )

	##Format for summary
	DF_OG_ALL = DF_OG.copy()
	DF_OG_ALL = DropDuplicateRecords(DF_OG_ALL, FLAG_DUPLICATE)

	DF_OG_VLN = DF_OG.copy()
	DF_OG_VLN = DropDuplicateRecords(DF_OG_VLN, FLAG_DUPLICATE)
	DF_OG_VLN = Clean_Summary_Values(DF_OG_VLN)

	#report headers***************************************************
	#note that some headers have been renamed at the top of this program
	
	header_two_way_table = ["violtn_cnt", "ee_violtd_cnt", "bw_amt", "records", "ee_pmt_recv"]
	header = ["legal_nm", "trade_nm", "cty_nm"] + header_two_way_table
	header_two_way = header_two_way_table + ["zip_cd", 'legal_nm', "juris_or_proj_nm", 'case_id', 'violation', 'violation_code','backwage_owed']
	
	header += ["naics_desc."]

	prevailing_header = header + ["juris_or_proj_nm", "Note"]
	
	if signatories_report is 1:
		header += ["Signatory"]
		prevailing_header += ["Signatory"]

	dup_header = header + ["street_addr"]
	dup_agency_header = header_two_way_table + ["juris_or_proj_nm"]
	dup_owner_header = header_two_way_table + ["Jurisdiction_region_or_General_Contractor"]

	multi_agency_header = header + ["agencies", "agency_names", "street_addr"]


	#textfile output***************************************
	#3/7/2022 bugFile.write("<h1>Here 5G</h1> \n")

	#HTML opening
	textFile = open(temp_file_name, 'w')
	textFile.write("<!DOCTYPE html> \n")
	textFile.write("<html><body> \n")

	Title_Block(TEST, DF_OG_VLN, DF_OG_ALL, target_city, TARGET_INDUSTRY, prevailing_wage_report, federal_data, state_data, textFile)

	if All_Industry_Summary_Block == True:
		Regional_All_Industry_Summary_Block(df, df, total_ee_violtd, total_bw_atp, total_case_violtn, all_unique_legalname, all_agency_df, OPEN_CASES, textFile)

	if Nonsignatory_Ratio_Block == True:
		Signatory_to_Nonsignatory_Block(df, df, textFile)

	if math.isclose(df['bw_amt'].sum(), out_counts['bw_amt'].sum(), rel_tol=0.03, abs_tol=0.0):
		do_nothing = "<p>Purposful Omission of Industry Summary Block</p>"
	else:
		Industry_Summary_Block(out_counts, out_counts, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, OPEN_CASES, textFile)

	textFile.write("<HR> </HR>" ) #horizontal line

	Notes_Block(textFile, default_region_name)

	Methods_Block(textFile)

	Footer_Block(TEST, textFile)

	#HTML closing
	textFile.write("<P style='page-break-before: always'>" )
	textFile.write("</html></body>")
	textFile.close()

	#TABLES

	if TABLES == 1:
		textFile = open(temp_file_name, 'a')
		textFile.write("<h2>Wage theft by industry and city region</h2> \n")
		textFile.close()

	df_all_industry = unique_legalname.groupby(['industry', pd.Grouper(key='cty_nm')]).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
		"bw_amt":'sum',
		"violtn_cnt":'sum',
		"ee_violtd_cnt":'sum',
		"ee_pmt_recv": 'sum',
		"records": 'sum',
	}).reset_index().sort_values(['industry', 'cty_nm'], ascending=[True, True])

	df_all_industry = df_all_industry.set_index(['industry', 'cty_nm'])
	df_all_industry = df_all_industry.sort_index()

	df_all_industry = df_all_industry.reindex(columns=header_two_way_table)
	for trade, new_df in df_all_industry.groupby(level=0):
		new_df = pd.concat([
			new_df, 
			new_df.sum().to_frame().T.assign(trade='', cty_nm='COUNTYWIDE').set_index(['trade', 'cty_nm'])
			], sort=True).sort_index()

		new_df["bw_amt"] = new_df.apply(lambda x: "{0:,.0f}".format(x["bw_amt"] ), axis=1 )
		new_df["violtn_cnt"] = new_df.apply(lambda x: "{0:,.0f}".format(x["violtn_cnt"] ), axis=1 )
		new_df["ee_violtd_cnt"] = new_df.apply(lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"] ), axis=1 )
		new_df["ee_pmt_recv"] = new_df.apply(lambda x: "{0:,.0f}".format(x["ee_pmt_recv"] ), axis=1 )
		new_df["records"] = new_df.apply(lambda x: "{0:,.0f}".format(x["records"] ), axis=1 )
		
		if TABLES == 1:
			write_to_html_file(new_df, header_two_way_table, "", file_path(temp_file_name) )


	if TABLES == 1:
		textFile = open(temp_file_name, 'a')
		textFile.write("<h2>Wage theft by zip code region and industry</h2> \n")
		textFile.close()

	df_all_industry_3 = unique_legalname.groupby(["zip_cd", pd.Grouper(key='industry')]).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
		"bw_amt":'sum',
		"violtn_cnt":'sum',
		"ee_violtd_cnt":'sum',
		"ee_pmt_recv": 'sum',
		"records": 'sum',
	}).reset_index().sort_values(['zip_cd', 'industry'], ascending=[True, True])

	df_all_industry_3 = df_all_industry_3.set_index(['zip_cd', 'industry'])
	df_all_industry_3 = df_all_industry_3.sort_index()

	df_all_industry_3 = df_all_industry_3.reindex(columns=header_two_way_table)
	for zip_cd, new_df_3 in df_all_industry_3.groupby(level=0):
		
		new_df_3 = pd.concat([
			new_df_3, 
			new_df_3.sum().to_frame().T.assign(zip_cd='', trade='ZIPCODEWIDE').set_index(['zip_cd', 'trade'])
			], sort=True).sort_index()

		new_df_3["bw_amt"] = new_df_3.apply(lambda x: "{0:,.0f}".format(x["bw_amt"] ), axis=1 )
		new_df_3["ee_pmt_recv"] = new_df_3.apply(lambda x: "{0:,.0f}".format(x["ee_pmt_recv"] ), axis=1 )
		new_df_3["records"] = new_df_3.apply(lambda x: "{0:,.0f}".format(x["records"] ), axis=1 )
		new_df_3["violtn_cnt"] = new_df_3.apply(lambda x: "{0:,.0f}".format(x["violtn_cnt"] ), axis=1 )
		new_df_3["ee_violtd_cnt"] = new_df_3.apply(lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"] ), axis=1 )

		if TABLES == 1:
			write_to_html_file(new_df_3, header_two_way_table, "", file_path(temp_file_name) )


	if TABLES == 1:
		textFile = open(temp_file_name, 'a')
		textFile.write("<h2>Wage theft by city and industry</h2> \n")
		textFile.close()

	df_all_industry_n = unique_legalname.groupby(["cty_nm", pd.Grouper(key="zip_cd"), pd.Grouper(key='industry'),  pd.Grouper(key='legal_nm')]).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
		"bw_amt":'sum',
		"violtn_cnt":'sum',
		"ee_violtd_cnt":'sum',
		"ee_pmt_recv": 'sum',
		'backwage_owed':'sum',
		"records": 'sum',
		}).reset_index().sort_values(['cty_nm', "zip_cd", 'industry', 'legal_nm'], ascending=[True, True, True, True])

	df_all_industry_n = df_all_industry_n.set_index(['cty_nm', "zip_cd", 'industry', 'legal_nm'])
	df_all_industry_n = df_all_industry_n.sort_index()

	df_all_industry_n = df_all_industry_n.reindex(columns=header_two_way)
	for cty_nm, new_df_n, in df_all_industry_n.groupby(level=0):

		#new_df_2 = new_df_n.reset_index(level=1, drop=True).copy() #make a copy without zipcode level 0
		new_df_2 = new_df_n.droplevel("zip_cd").copy()

		new_df_legal_nm = new_df_2.drop(columns=['legal_nm']) #delete empty column
		new_df_legal_nm = new_df_legal_nm.reset_index() #pull out legal_nm column from level
		city_unique_legalname = GroupByX(new_df_legal_nm, 'legal_nm')
		city_total_bw_atp = new_df_2['bw_amt'].sum()
		city_cases = new_df_2['records'].sum() #debug 10/30/2020 this is an approximation based on records which is actually an overtstated mix of case and violations counts

		new_df_drop1 = new_df_n.droplevel("zip_cd").copy()
		new_df_drop1 = new_df_drop1.droplevel('legal_nm')
		city_agency_df = GroupByMultpleAgency(new_df_drop1)

		if SUMMARY == 1 and (city_cases > 10 or city_total_bw_atp > 10000):
			City_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn, city_unique_legalname, city_agency_df, cty_nm, SUMMARY_SIG, file_path(temp_file_name))
			City_Summary_Block_4_Zipcode_and_Industry(new_df_n, df_all_industry_n, TARGET_INDUSTRY, SUMMARY_SIG, file_path(temp_file_name) )

			#Industry_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn, city_unique_legalname, city_agency_df, cty_nm, SUMMARY_SIG, file_path(temp_file_name))
			#Industry_Summary_Block_4_Zipcode_and_City(new_df_n, df_all_industry_n, TARGET_INDUSTRY, SUMMARY_SIG, file_path(temp_file_name) )

		new_df_2 = new_df_2.groupby(["cty_nm", 'industry']).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
			"bw_amt":'sum',
			"violtn_cnt":'sum',
			"ee_violtd_cnt":'sum',
			"ee_pmt_recv": 'sum',
			"records": 'sum',
			}).reset_index().sort_values(['cty_nm', 'industry'], ascending=[True, True])

		new_df_2 = new_df_2.set_index(['cty_nm', 'industry'])
		new_df_2 = new_df_2.sort_index()

		new_df_2 = pd.concat([ 
			new_df_2.sum().to_frame().T.assign(cty_nm='', trade='CITYWIDE').set_index(['cty_nm', 'trade']),
			new_df_2
			], sort=True).sort_index()

		#new_df_2 = new_df_2.loc[:,~new_df_2.columns.duplicated()] #https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns

		new_df_2["bw_amt"] = new_df_2.apply(lambda x: "{0:,.0f}".format(x["bw_amt"] ), axis=1 )
		new_df_2["ee_pmt_recv"] = new_df_2.apply(lambda x: "{0:,.0f}".format(x["ee_pmt_recv"] ), axis=1 )
		new_df_2["records"] = new_df_2.apply(lambda x: "{0:,.0f}".format(x["records"] ), axis=1 )
		new_df_2["violtn_cnt"] = new_df_2.apply(lambda x: "{0:,.0f}".format(x["violtn_cnt"] ), axis=1 )
		new_df_2["ee_violtd_cnt"] = new_df_2.apply(lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"] ), axis=1 )

		if TABLES == 1 and (city_cases > 10 or city_total_bw_atp > 10000):
			write_to_html_file(new_df_2, header_two_way_table, "", file_path(temp_file_name) )
	
	
	if TOP_VIOLATORS == 1 and not df.empty: #report tabless*************************************************************

		## format
		unique_address = Clean_Repeat_Violator_HTML_Row(unique_address, 'street_addr')
		unique_address = FormatNumbersHTMLRow(unique_address)
		
		unique_legalname2 = Clean_Repeat_Violator_HTML_Row(unique_legalname2, 'legal_nm')
		unique_legalname2 = FormatNumbersHTMLRow(unique_legalname2)
		
		unique_tradename = Clean_Repeat_Violator_HTML_Row(unique_tradename, 'trade_nm')
		unique_tradename = FormatNumbersHTMLRow(unique_tradename)

		unique_agency = Clean_Repeat_Violator_HTML_Row(unique_agency, 'juris_or_proj_nm')
		unique_agency = FormatNumbersHTMLRow(unique_agency) 
				
		unique_owner = Clean_Repeat_Violator_HTML_Row(unique_owner, 'Jurisdiction_region_or_General_Contractor')
		unique_owner = FormatNumbersHTMLRow(unique_owner)

		agency_df = FormatNumbersHTMLRow(agency_df)

		out_sort_ee_violtd = FormatNumbersHTMLRow(out_sort_ee_violtd)
		out_sort_bw_amt = FormatNumbersHTMLRow(out_sort_bw_amt)
		out_sort_repeat_violtd = FormatNumbersHTMLRow(out_sort_repeat_violtd)

		df.plot() #table setup

		#tables top 10 violators
		with open(temp_file_name, 'a', encoding='utf-8') as f:

			if not out_sort_bw_amt.empty:
				#by backwages
				f.write("<h2>Top violators by amount of backwages stolen (by legal name)</h2> \n")
				out_sort_bw_amt.head(6).to_html(f, columns = header, index=False)
				f.write("\n")

			if not out_sort_ee_violtd.empty:
				#by employees
				f.write("<h2>Top violators by number of employees violated (by legal name)</h2> \n")
				out_sort_ee_violtd.head(6).to_html(f, columns = header, index=False)
				f.write("\n")

			if not out_sort_repeat_violtd.empty:
				#by repeated
				f.write("<h2>Top violators by number of repeat violations (by legal name)</h2> \n")
				out_sort_repeat_violtd.head(6).to_html(f, columns = header, index=False)
				f.write("\n")

			#by repeat violators******************
			row_head = 24
			if not unique_address.empty:
				#by unique_address
				f.write("<h2>Group by address and sort by records</h2> \n")
				unique_address.head(row_head).to_html(f, columns = dup_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no groups by address to report</p> \n")

			if not unique_legalname2.empty:
				#by 'legal_name'
				f.write("<h2>Group by legal name and sort by records</h2> \n")
				unique_legalname2.head(row_head).to_html(f, columns = dup_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no groups by legal name to report</p> \n")

			if not unique_tradename.empty and TEST != 3:
				#by unique_trade_nm
				f.write("<h2>Group by trade name and sort by records</h2> \n")
				unique_tradename.head(row_head).to_html(f, columns = dup_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no groups by trade name to report</p> \n")

			if not agency_df.empty:
				#report for cases from multiple agencies
				f.write("<h2>Group by company and sort by number of agencies involved</h2> \n")
				agency_df.head(row_head).to_html(f, columns = multi_agency_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no groups by companies with violations by multiple agencies to report</p> \n")

			if not unique_agency.empty:
				#report agency counts
				#by unique_agency
				f.write("<h2>Cases by agency or owner</h2> \n")
				unique_agency.head(row_head).to_html(f, columns = dup_agency_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no case groups by agency or owner to report</p> \n")

			if not unique_owner.empty:
				#by 'unique_owner'
				f.write("<h2>Cases by jurisdiction (includes private jurisdictions)</h2> \n")
				unique_owner.head(row_head).to_html(f, columns = dup_owner_header, index=False)
				f.write("\n")
			else: f.write("<p> There are no case groups by jurisdiction to report</p> \n")

			#signatory
			if signatories_report == 1 and not out_signatory_target.empty:
				
				out_sort_signatory = pd.DataFrame()
				out_sort_signatory = out_signatory_target.sort_values('legal_nm', ascending=True)

				out_sort_signatory['violtn_cnt'] = out_sort_signatory.apply(lambda x: "{0:,.0f}".format(x['violtn_cnt'] ), axis=1 )
				out_sort_signatory['ee_pmt_recv'] = out_sort_signatory.apply(lambda x: "{0:,.0f}".format(x['ee_pmt_recv'] ), axis=1 )


				f.write("<P style='page-break-before: always'>" )
				target_industry = TARGET_INDUSTRY[0][0]
				out_sort_signatory.to_csv('report_output/' + target_city.replace(' ','_') + '_signatory_wage_theft_'+ target_industry.replace(' ','_') +'.csv')

				f.write("<h2>All signatory wage violators</h2> \n")
				
				if not len(out_sort_signatory.index )==0:
					f.write("<p>Signatory wage theft cases: ")
					f.write(str.format('{0:,.0f}',len(out_sort_signatory.index ) ) )
					f.write("</p> \n")

				if not out_sort_signatory['bw_amt'].sum()==0:
					f.write("<p>Total signatory wage theft: $")
					f.write(str.format('{0:,.0f}',out_sort_signatory['bw_amt'].sum() ) )
					f.write("</p> \n")
				'''
				if not out_sort_signatory['ee_violtd_cnt'].sum()==0:
					f.write("<p>Signatory wage employees violated: ")
					out_sort_signatory['ee_violtd_cnt'] = pd.to_numeric(out_sort_signatory['ee_violtd_cnt'], errors = 'corece')
					f.write(str.format('{0:,.0f}',out_sort_signatory['ee_violtd_cnt'].sum() ) )
					f.write("</p> \n")

				if not out_sort_signatory['violtn_cnt'].sum()==0:
					f.write("<p>Signatory wage violations: ")
					out_sort_signatory['violtn_cnt'] = pd.to_numeric(out_sort_signatory['violtn_cnt'], errors = 'corece')
					f.write(str.format('{0:,.0f}',out_sort_signatory['violtn_cnt'].sum() ) )
					f.write("</p> \n")
				'''

				f.write("\n")

				out_sort_signatory.to_html(f, max_rows=3000, columns = prevailing_header, index=False)

				f.write("\n")

			#prevailing wage
			if prevailing_wage_report == 1 and not out_prevailing_target.empty:
				out_sort_prevailing_wage = pd.DataFrame()
				#out_sort_prevailing_wage = out_prevailing_target.sort_values('records', ascending=False)
				out_sort_prevailing_wage = out_prevailing_target.sort_values('legal_nm', ascending=True)

				out_sort_prevailing_wage['violtn_cnt'] = out_sort_prevailing_wage.apply(lambda x: "{0:,.0f}".format(x['violtn_cnt'] ), axis=1 )
				out_sort_prevailing_wage['ee_pmt_recv'] = out_sort_prevailing_wage.apply(lambda x: "{0:,.0f}".format(x['ee_pmt_recv'] ), axis=1 )

				f.write("<P style='page-break-before: always'>" )
				target_industry = TARGET_INDUSTRY[0][0]
				out_sort_prevailing_wage.to_csv('report_output/' + target_city.replace(' ','_') + '_prevailing_wage_theft_'+ target_industry.replace(' ','_') +'.csv')

				f.write("<h2>All prevailing wage violators</h2> \n")
				

				f.write("<p>Prevailing wage theft cases: ")
				f.write(str.format('{0:,.0f}',len(out_sort_prevailing_wage.index ) ) )
				#f.write(str.format('{0:,.0f}',len(out_sort_prevailing_wage['records'].sum() ) ) )
				f.write("</p> \n")


				f.write("<p>Total prevailing wage theft: $")
				f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['bw_amt'].sum() ) )
				f.write("</p> \n")

				f.write("<p>Total prevailing wage theft: $") 
				f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['bw_amt'].sum() ) )
				f.write("</p> \n")

				#buggy 6/14/2021
				# f.write("<p>Prevailing wage employees violated: ")
				# out_sort_prevailing_wage['ee_violtd_cnt'] = pd.to_numeric(out_sort_prevailing_wage['ee_violtd_cnt'], errors = 'corece')
				# f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['ee_violtd_cnt'].sum() ) )
				# f.write("</p> \n")

				# f.write("<p>Prevailing wage violations: ")
				# out_sort_prevailing_wage['violtn_cnt'] = pd.to_numeric(out_sort_prevailing_wage['violtn_cnt'], errors = 'corece')
				# f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['violtn_cnt'].sum() ) )
				# f.write("</p> \n")


				f.write("\n")
				# 12/25/2021 added "float_format=lambda x: '%10.2f' % x" per https://stackoverflow.com/questions/14899818/format-output-data-in-pandas-to-html
				out_sort_prevailing_wage.to_html(f, max_rows=3000, columns = prevailing_header, index=False, float_format=lambda x: '%10.2f' % x)

				f.write("\n")

			#clean output file
			if CLEAN_OUTPUT == 1:
				
				# df_signatory['legal_nm'] = df_signatory['legal_nm'].str.upper()
				# df_signatory['street_addr'] = df_signatory['street_addr'].str.upper()
				# df_signatory['cty_nm'] = df_signatory['cty_nm'].str.upper()
				# df_signatory['st_cd'] = df_signatory['st_cd'].str.upper()

				do_nothing = ""

		#end indent

	#3/7/2022 bugFile.write("<h1>Done</h1> \n")
	#3/7/2022 bugFile.write("</html></body> \n")
	#3/7/2022 bugFile.close()
	
	return temp_file_name #the temp json returned from API


def Clean_Repeat_Violator_HTML_Row(df, COLUMN_NAME):
	#df = df.dropna(subset=[COLUMN_NAME]) #drop NAs
	df = df[df.records > 1] #https://stackoverflow.com/questions/18172851/deleting-dataframe-row-in-pandas-based-on-column-value
	
	if df.empty:
		df = df.append({
			COLUMN_NAME: "no records",
			}, ignore_index=True)
	
	return df

def GroupByX(df, COLUMN_NAME):

	if COLUMN_NAME in df.columns and not df[COLUMN_NAME].isnull().all():

		df = df[df[COLUMN_NAME].notnull() ]
		df = df[df[COLUMN_NAME] != 'nan']
		df = df[df[COLUMN_NAME] != 'NAN']
		df = df[df[COLUMN_NAME] != '']
		df = df[df[COLUMN_NAME] != 0]
		df = df[df[COLUMN_NAME] != False]

		df[COLUMN_NAME] = df[COLUMN_NAME].str.upper()

		df['records'] = 1
		df['records'] = df.groupby(COLUMN_NAME)[COLUMN_NAME].transform('count') #count duplicates

		df['bw_amt'] = df.groupby(COLUMN_NAME)["bw_amt"].transform('sum')
		#df['ee_pmt_due'] = df.groupby(COLUMN_NAME)["ee_pmt_due"].transform('sum')
		#df['backwage_owed'] = df.groupby(COLUMN_NAME)['backwage_owed'].transform('sum')
		df['violtn_cnt'] = df.groupby(COLUMN_NAME)["violtn_cnt"].transform('sum')
		df['ee_violtd_cnt'] = df.groupby(COLUMN_NAME)["ee_violtd_cnt"].transform('sum')
		df['ee_pmt_recv'] = df.groupby(COLUMN_NAME)["ee_pmt_recv"].transform('sum')

		df = df.drop_duplicates(subset=[COLUMN_NAME], keep = 'first')

		#df[COLUMN_NAME] = df[COLUMN_NAME].str.title()
	return df

def GroupByMultpleCases(df, COLUMN_NAME):

	if COLUMN_NAME in df.columns and not df[COLUMN_NAME].isnull().all():

		df = df[df[COLUMN_NAME].notnull() ]
		df = df[df[COLUMN_NAME] != 'nan']
		df = df[df[COLUMN_NAME] != 'NAN']
		df = df[df[COLUMN_NAME] != '']
		df = df[df[COLUMN_NAME] != 0]
		df = df[df[COLUMN_NAME] != False]

		df[COLUMN_NAME] = df[COLUMN_NAME].str.upper()

		df['records'] = 1
		df['records'] = df.groupby(COLUMN_NAME)[COLUMN_NAME].transform('count') #count duplicates

		df['bw_amt'] = df.groupby(COLUMN_NAME)["bw_amt"].transform('sum')
		#df['ee_pmt_due'] = df.groupby(COLUMN_NAME)["ee_pmt_due"].transform('sum')
		df['backwage_owed'] = df.groupby(COLUMN_NAME)['backwage_owed'].transform('sum')
		df['violtn_cnt'] = df.groupby(COLUMN_NAME)["violtn_cnt"].transform('sum')
		df['ee_violtd_cnt'] = df.groupby(COLUMN_NAME)["ee_violtd_cnt"].transform('sum')
		df['ee_pmt_recv'] = df.groupby(COLUMN_NAME)["ee_pmt_recv"].transform('sum')

		df = df.drop_duplicates(subset=[COLUMN_NAME], keep = 'first')
		df = df[df.records > 1]

		#df[COLUMN_NAME] = df[COLUMN_NAME].str.title()
	return df

def GroupByMultpleAgency(df):

	df['legal_nm'] = df['legal_nm'].astype(str).str.upper()
	df['juris_or_proj_nm'] = df['juris_or_proj_nm'].astype(str).str.upper()

	df = df[df['legal_nm'].notnull() ]
	df = df[df['legal_nm'] != 'nan']
	df = df[df['legal_nm'] != 'NAN']
	df = df[df['legal_nm'] != '']
	
	df = df[df['juris_or_proj_nm'].notnull() ]
	df = df[df['juris_or_proj_nm'] != 'nan']
	df = df[df['juris_or_proj_nm'] != 'NAN']
	df = df[df['juris_or_proj_nm'] != '']

	df['records'] = 1
	df['records'] = df.groupby(['legal_nm','juris_or_proj_nm'])['legal_nm'].transform('count')
	df['bw_amt'] = df.groupby(['legal_nm','juris_or_proj_nm'])["bw_amt"].transform('sum')
	df['violtn_cnt'] = df.groupby(['legal_nm','juris_or_proj_nm'])["violtn_cnt"].transform('sum')
	df['ee_violtd_cnt'] = df.groupby(['legal_nm','juris_or_proj_nm'])["ee_violtd_cnt"].transform('sum')
	df['ee_pmt_recv'] = df.groupby(['legal_nm','juris_or_proj_nm'])["ee_pmt_recv"].transform('sum')

	df = df.drop_duplicates(subset=['legal_nm','juris_or_proj_nm'], keep = 'first')

	df['agencies'] = 0
	df['agencies'] = df.groupby('legal_nm')['legal_nm'].transform('count') #count duplicates across agencies
	df['records'] = df.groupby('legal_nm')['records'].transform('sum')
	df['bw_amt'] = df.groupby('legal_nm')["bw_amt"].transform('sum')
	df['violtn_cnt'] = df.groupby('legal_nm')["violtn_cnt"].transform('sum')
	df['ee_violtd_cnt'] = df.groupby('legal_nm')["ee_violtd_cnt"].transform('sum')
	df['ee_pmt_recv'] = df.groupby('legal_nm')["ee_pmt_recv"].transform('sum')
	
	df['agency_names'] = np.nan
	if not df.empty:
		df['agency_names'] = df.groupby('legal_nm')['juris_or_proj_nm'].transform(", ".join) #count duplicates across agencies

	df = df.drop_duplicates(subset=['legal_nm'], keep = 'first')
	df = df[df.agencies > 1]

	#df['legal_nm'] = df['legal_nm'].str.title()
	df['juris_or_proj_nm'] = df['juris_or_proj_nm'].str.title()

	return df


def FilterForDate(df, YEAR_START, YEAR_END):
	df = df[
		( ( pd.to_datetime(df['findings_start_date'], dayfirst=True ) > YEAR_START ) & (pd.to_datetime(df['findings_start_date'], dayfirst=True ) < YEAR_END ) ) |
		( pd.isnull(df['findings_start_date']) & ( pd.to_datetime(df['findings_end_date'], dayfirst=True ) > YEAR_START ) & (pd.to_datetime(df['findings_end_date'], dayfirst=True ) < YEAR_END ) ) |
		( pd.isnull(df['findings_start_date']) & pd.isnull(df['findings_end_date']) )
		]
	return df

def Filter_for_Target_Industry(df, TARGET_INDUSTRY):
	appended_data = pd.DataFrame()
	for x in range(len(TARGET_INDUSTRY)): #https://stackoverflow.com/questions/28669482/appending-pandas-dataframes-generated-in-a-for-loop
		temp_term = TARGET_INDUSTRY[x][0]
		df_temp = df.loc[df['industry'].str.upper()==temp_term.upper() ]
		appended_data = appended_data.append(df_temp)
	return appended_data

def Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS):

	df_temp_0 = df.loc[df['legal_nm'].str.contains('|'.join(TARGET_ORGANIZATIONS[1] )) ]
	df_temp_1 = df.loc[df['trade_nm'].str.contains('|'.join(TARGET_ORGANIZATIONS[1] )) ]

	df_temp = pd.concat([df_temp_0, df_temp_1], ignore_index=True)
	return df_temp

def Filter_for_Zipcode(df_csv, TARGET_ZIPCODE):
	if TARGET_ZIPCODE[0] is not '00000':
		df_csv = df_csv.loc[df_csv['zip_cd'].isin(TARGET_ZIPCODE ) ] #Filter on region by zip code
	return df_csv

def Filter_for_State(df_csv, TARGET_STATES):
	df_temp = df_csv.loc[df_csv['st_cd'].isin(TARGET_STATES[1]) ] #Filter on state
	return df_temp

def RemoveCompletedCases(df):
	'''
	DLSE categories
	'Closed - Paid in Full (PIF)/Satisfied'
	'Full Satisfaction'
	'Open'
	'Open - Bankruptcy Stay'
	'Open - Payment Plan'
	'Partial Satisfaction'
	'Pending/Open'
	'Preliminary'
	'Vacated'
	'''

	if "Note" in df:
		df = df.loc[
			(df["Note"] != 'Closed - Paid in Full (PIF)/Satisfied') & 
			(df["Note"] != 'Full Satisfaction') & 
			(df["Note"] != 'Vacated')
			]
	
	
	df['bw_amt'] = df['bw_amt'].replace('NaN',0).astype(float)
	df['ee_pmt_recv'] = df['ee_pmt_recv'].replace('NaN',0).astype(float)

	df['bw_amt'] = df['bw_amt'].fillna(0).astype(float)
	df['ee_pmt_recv'] = df['ee_pmt_recv'].fillna(0).astype(float)

	df = df[df['bw_amt'] != df['ee_pmt_recv'] ]

	bw = df['bw_amt'].tolist()
	pmt = df['ee_pmt_recv'].tolist()

	for i in range (0, len(bw)-5 ): #df.index # -5 fixes an over count bug and is small eough that it wont introduce that much error into reports
		if math.isclose(bw[i], pmt[i], rel_tol=0.10, abs_tol=0.0): #works
		#if math.isclose(df['bw_amt'][i], df['ee_pmt_recv'][i], rel_tol=0.10, abs_tol=0.0): #error
		#if math.isclose(df.loc[i].at['bw_amt'], df.loc[i].at['ee_pmt_recv'], rel_tol=0.10, abs_tol=0.0): #error
		#if math.isclose(df.at[i,'bw_amt'], df.at[i,'ee_pmt_recv'], rel_tol=0.10, abs_tol=0.0): #error
			df.drop(df.index[i], inplace = True)
	
	return df


def InferPrevailingWageAndColumnFlag(df, prevailing_wage_terms):
	
	df['Prevailing'] = df.Prevailing.fillna("0")

	prevailing_wage_pattern = '|'.join(prevailing_wage_terms)
	found_prevailing = ( 
		( (df['violation_code'].astype(str).str.contains(prevailing_wage_pattern, flags=re.IGNORECASE, regex = True) ) ) |
		( (df['violation'].astype(str).str.contains(prevailing_wage_pattern, flags=re.IGNORECASE, regex = True) ) ) | 
		( (df['Note'].astype(str).str.contains(prevailing_wage_pattern, flags=re.IGNORECASE, regex = True) ) ) |  
		( (df['juris_or_proj_nm'].astype(str).str.contains(prevailing_wage_pattern, flags=re.IGNORECASE, regex = True) ) )
	)
	df['Prevailing'] = pd.to_numeric(df['Prevailing'], errors='coerce')
	df['Prevailing'][found_prevailing] = 1

	return df

def InferAgencyFromCaseIDAndLabel(df, LABEL_COLUMN):

	if LABEL_COLUMN in df.columns:
		foundAgencybyCaseID_1 = pd.isna(df[LABEL_COLUMN]) #find case IDs with a hyphen
		#df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna(foundAgencybyCaseID_1.replace( (True,False), (df['case_id'].astype(str).apply(lambda st: st[:st.find("-")]), df[LABEL_COLUMN]) ) ) #https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas
		df[LABEL_COLUMN][foundAgencybyCaseID_1] = df['case_id'].astype(str).apply(lambda st: st[:st.find("-")])  #https://stackoverflow.com/questions/51660357/extract-substring-between-two-characters-in-pandas

		foundAgencybyCaseID_2 = pd.isna(df[LABEL_COLUMN]) #cind case ID when no hyphen
		#df[LABEL_COLUMN] = df[LABEL_COLUMN].fillna(foundAgencybyCaseID_2.replace( (True,False), (df['case_id'].astype(str).str[:3], df[LABEL_COLUMN]) ) )
		df[LABEL_COLUMN][foundAgencybyCaseID_2] = df['case_id'].astype(str).str[:3]

		#hardcode for DLSE nomemclature with a note * for assumed
		DLSE_terms = ['01', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '23', '32', '35', 'WC']
		pattern_DLSE = '|'.join(DLSE_terms)
		found_DLSE = (df[LABEL_COLUMN].str.contains(pattern_DLSE ) )
		#df[LABEL_COLUMN] = found_DLSE.replace( (True,False), ("DLSE", df[LABEL_COLUMN] ) )
		df[LABEL_COLUMN][found_DLSE] = "DLSE"

	return df

def InferSignatoriesFromNameAndFlag(df, SIGNATORY_INDUSTRY):

	if 'legal_nm' and 'trade_nm' in df.columns:
		if "Signatory" not in df.columns: df["Signatory"] = 0

		for x in range(1, len(SIGNATORY_INDUSTRY)):
			PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(SIGNATORY_INDUSTRY[x][1])
			PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

			foundIt_sig = ( 
				(
					df['legal_nm'].str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
					~df['legal_nm'].str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) )|
				(	
					df['trade_nm'].str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
					~df['trade_nm'].str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) )
				)
			df["Signatory"][foundIt_sig] = 1

	return df

def InferSignatoriesFromNameAndAddressFlag(df, signatory_name_list, signatory_address_list):

	if "Signatory" not in df.columns: df["Signatory"] = 0

	pattern_signatory_name = '|'.join(signatory_name_list)
	pattern_signatory_add = '|'.join(signatory_address_list)

	foundIt_sig = ( 
		((df['legal_nm'].str.contains(pattern_signatory_name, flags=re.IGNORECASE, regex = True) ) &
		(df['street_addr'].str.contains(pattern_signatory_add, flags=re.IGNORECASE, regex = True) ) 
		)|
		((df['trade_nm'].str.contains(pattern_signatory_name, flags=re.IGNORECASE, regex = True) ) &
		(df['street_addr'].str.contains(pattern_signatory_add, flags=re.IGNORECASE, regex = True) ) 
		)
		)
	df["Signatory"][foundIt_sig] = 1

	return df

def InferSignatoriesFromAddressAndFlag(df, signatory_address_list):

	if "Signatory" not in df.columns: df["Signatory"] = 0

	pattern_signatory = '|'.join(signatory_address_list)
	foundIt_sig = ( 
		(df['street_addr'].str.contains(pattern_signatory, flags=re.IGNORECASE, regex = True) )
		#(df['street_addr'].str.match(pattern_signatory, flags=re.IGNORECASE) ) 
		)
	df["Signatory"][foundIt_sig] = 1

	return df

def Label_Industry(df, INFER, TARGET_INDUSTRY):
	if 'industry' not in df.columns: df['industry'] = ""
	if 'trade2' not in df.columns: df['trade2'] = ""

	if not df.empty and 'legal_nm' and 'trade_nm' in df.columns: #filter out industry rows
		for x in range(1, len(TARGET_INDUSTRY)): #exclude cell 0 that contains an industry label cell
			#add a levinstein distance with distance of two and match and correct: debug 10/30/2020
			PATTERN_IND = '|'.join(TARGET_INDUSTRY[x])
			PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(TARGET_INDUSTRY[x])
			
			if INFER == 1:
				foundIt_ind1=(

					(
					df['legal_nm'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) 
					&
					~df['legal_nm'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) )
					
					|

					(
					df['trade_nm'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) 
					&
					~df['trade_nm'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) ) 

					)

				df['industry'][foundIt_ind1] = TARGET_INDUSTRY[x][0]

			#second round fills in anything missed with the exisiting NAICS coding
			foundIt_ind2=(
				#(df['industry'] == "") &
				df['naic_cd'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
				~df['naic_cd'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) 
			)
			df['trade2'][foundIt_ind2] = TARGET_INDUSTRY[x][0]
					
			foundIt_ind3=( #commented out 12/24/2020 because the decriptions are too easily miscoded
				(df['trade2'] == "") &
				df['naics_desc.'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
				~df['naics_desc.'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True)
			)
			df['trade2'][foundIt_ind3] = TARGET_INDUSTRY[x][0]

			# foundIt_ind4=( #commented out 12/24/2020 because the labels were just not useful
			# 	(#pd.isna(df['industry']) &
			# 	df['industry'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
			# 	~df['industry'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) )
			# 	)
			# df['trade2'][foundIt_ind4] = TARGET_INDUSTRY[x][0]

		df['industry'] = df.apply(
			lambda row: 
				row['trade2'] if (row['industry'] == '') and (row['trade2'] != '')
				else row['industry']
				,axis=1
			)
		
		#if all fails, assign 'other' so it gets counted
		df['industry'] = df['industry'].replace(r'^\s*$', 'Undefined', regex=True)
		df['industry'] = df['industry'].fillna('Undefined')
		df['industry'] = df['industry'].replace(np.nan, 'Undefined')

		#when the above misses the fillna
		#mode = df['industry'].mode().values[0]
		#df['industry'].fillna(value=mode, inplace=True) #https://stackoverflow.com/questions/58631524/fillna-not-replacing-nan-values-in-the-dataframe

	return df

def InferSignatoryIndustryAndLabel(df, SIGNATORY_INDUSTRY):
	if 'signatory_industry' not in df.columns: df['signatory_industry'] = ""

	if not df.empty and 'legal_nm' and 'trade_nm' in df.columns: #filter out industry rows
		for x in range(1, len(SIGNATORY_INDUSTRY)):

			PATTERN_EXCLUDE = EXCLUSION_LIST_GENERATOR(SIGNATORY_INDUSTRY[x][1])
			PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

			foundIt_ind1=(
				(
				df['legal_nm'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
				~df['legal_nm'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) 
				) |
				(
				df['trade_nm'].astype(str).str.contains(PATTERN_IND, flags=re.IGNORECASE, regex = True) &
				~df['trade_nm'].astype(str).str.contains(PATTERN_EXCLUDE, flags=re.IGNORECASE, regex = True) 
				) |
				(df['Signatory'] == 1) & 
				(df['industry'] == SIGNATORY_INDUSTRY[x][0][0])
				)
			df['signatory_industry'][foundIt_ind1] = SIGNATORY_INDUSTRY[x][0][0]
		
		#if all fails, assign 'other' so it gets counted
		df['signatory_industry'] = df['signatory_industry'].replace(r'^\s*$', 'Undefined', regex=True)
		df['signatory_industry'] = df['signatory_industry'].fillna('Undefined')
		df['signatory_industry'] = df['signatory_industry'].replace(np.nan, 'Undefined')

	return df

def InferZipcodeFromCityName(df, region, region_name):
	if region is not 'all':
		upper_region = [x.upper() for x in region]
		PATTERN_CITY = '|'.join(upper_region)
		foundZipbyCity1 = (  
			( (df['zip_cd'] == "") | pd.isna(df['zip_cd']) | (df['zip_cd'] == False) ) & 
			( df['st_cd'].str.match('CA') ) 
		)
		foundZipbyCity2 = (
			( (df['cty_nm'].astype(str).str.contains(PATTERN_CITY, case = False, flags=re.IGNORECASE) ) ) #flags=re.IGNORECASE
			)
		df["zip_cd"][foundZipbyCity1 * foundZipbyCity2] = region_name
	return df
		
def InferZipcodeFromCompanyName(df, region, region_name): #fill nan zip code by assumed zip by city name in trade name; ex. "Cupertino Elec."
	
	if region is not 'all':
		PATTERN_CITY = '|'.join(region)
		foundZipbyCompany1 = ( 
			( pd.isna(df['cty_nm']) ) & 
			( (df['zip_cd'] == "") | pd.isna(df['zip_cd']) | (df['zip_cd'] == False) ) & 
			( df['st_cd'].str.match('CA') )
		)
		foundZipbyCompany2 = ( 
			( (df['trade_nm'].astype(str).str.contains(PATTERN_CITY, case = False, flags=re.IGNORECASE) ) ) | 
			( (df['legal_nm'].astype(str).str.contains(PATTERN_CITY, case = False, flags=re.IGNORECASE) ) ) 
		)
		df['zip_cd'][foundZipbyCompany1 * foundZipbyCompany2] = region_name
	return df

def InferZipcodeFromJurisdictionName(df, region, region_name):

	if region is not 'all' and region[0] is not 'County of Santa Clara' and 'juris_or_proj_nm' in df.columns:
		PATTERN_CITY = '|'.join(region)

		df.st_cd = df.st_cd.astype(str)
		df.juris_or_proj_nm = df.juris_or_proj_nm.astype(str)
		df.Jurisdiction_region_or_General_Contractor = df.Jurisdiction_region_or_General_Contractor.astype(str)

		foundZipbyCompany = ( 
			(
				( (df['zip_cd'] == "") | pd.isna(df['cty_nm']) | (df['zip_cd'] == False) )  & 
				( df['st_cd'].str.match('CA') ) 
			) & 
			(
				( df['juris_or_proj_nm'].str.contains(PATTERN_CITY, flags=re.IGNORECASE, regex = True) ) | 
				( df['Jurisdiction_region_or_General_Contractor'].str.contains(PATTERN_CITY, flags=re.IGNORECASE, regex = True) ) 
			)
		)
		df['zip_cd'][foundZipbyCompany] = region_name
		#add logic to overwire zip codes from outside target region

	return df

def EXCLUSION_LIST_GENERATOR(SIGNATORY_INDUSTRY):
	target = pd.Series(SIGNATORY_INDUSTRY)
	target = target[target.str.contains("!")==True] #https://stackoverflow.com/questions/28679930/how-to-drop-rows-from-pandas-data-frame-that-contains-a-particular-string-in-a-p
	target = target.str.replace('[\!]','', regex=True)
	if len(target) > 0:
		PATTERN_EXCLUDE = '|'.join(target)
	else: 
		PATTERN_EXCLUDE = "999999"
	return PATTERN_EXCLUDE

def infer_wage_penalty(df):

	mean_backwage = df['bw_amt'].mean()
	#mean_backwage = df[df['bw_amt']!=0].mean()

	# lookup term / (1) monetary penalty
	A = ["ACCESS TO PAYROLL", 750] #$750
	B = ["L.C. 1021", 200] #$200 per day (plug one day)
	C = ["L.C. 11942", mean_backwage] #less than minimum wage
	D = ["L.C. 1197", mean_backwage]
	E = ["L.C. 1299", 500]
	F = ["L.C. 1391", 500]
	G = ["L.C. 203", 3600] #improper deduction 30 days of wage (plug $15 x 8 x 30)
	H = ["L.C. 2054", mean_backwage]
	I = ["L.C. 2060", mean_backwage]
	J = ["L.C. 223", mean_backwage] #less than contract (prevailing)
	K = ["L.C. 226(a)", 250] #paycheck itemized plus $250
	K1 = ["LCS 226(a)", 250] #paycheck itemized plus $250
	L = ["L.C. 2267", 150] # Meal periods plug ($150)
	M = ["L.C. 2675(a)", 250]
	N = ["L.C. 3700", 500] # workmans compensation $500 to State Director for work comp fund
	O = ["L.C. 510", 200] # 8 hour workday $50 per pay period plug $200
	P = ["LIQUIDATED DAMAGES", mean_backwage] # equal to lost wage plug mean
	Q = ["MEAL PERIOD PREMIUM WAGES", 75] #1 hour of pay (plug $15 * 5 days)
	R = ["MINIMUM WAGES", 50] #plug $50
	S = ["Misclassification", mean_backwage] #plug mean wage
	T = ["Overtime", mean_backwage] #plus mean wage
	U = ["PAID SICK LEAVE", 200] # $50 per day plug $200
	V = ["PIECE RATE WAGES", 1200] #aka PAGA $100 per pay period for 1 year plug $1,200
	W = ["PRODUCTION BONUS", mean_backwage] # plug mean wage
	X = ["REGULAR WAGES", mean_backwage] # plug mean wage
	Y = ["REPORTING TIME PAY", 1000] # daily wage for 30 days plug $1,000 of (15 * 8 * 30)
	Z = ["REST PERIOD PREMIUM WAGES", 2000] # plug $2,000
	AA = ["VACATION WAGES", 2000] # plug $2,000
	AB = ["SPLIT SHIFT PREMIUM WAGES", 500] # $500
	AC = ["UNLAWFUL DEDUCTIONS", 2000] # plug $2000
	AD = ["UNLAWFUL TIP DEDUCTIONS", 1000] # $1,000
	AE = ["UNREIMBURSED BUSINESS EXPENSES", 2500] #plug $2,500
	AF = ["WAITING TIME PENALTIES", 2500] # plug $2,500
	AG = ["LC 1771", 125] # 5 x $25
	AH = ["L.C. 1771", 125] # 5 x $25
	AI = ["L.C. 1774", 125] # 5 x $25
	AJ = ["LC 1774", 125] # 5 x $25
	AK = ["", mean_backwage] #<blank> plug mean wage
	#generic monetary penalty is 25% of backwages
	
	penalties = [['MONETARY_PENALTY'], A, B, C, D, E, F, G, H, I, J, K, K1, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE, AF, AG, AH, AI, AJ, AK]

	df['assumed_backwage'] = np.where(( (df['bw_amt'] == 0) | (df['bw_amt'] == "") | (df['bw_amt'] == 0) | (df['bw_amt'] == False) ), 1, df['assumed_backwage'] )

	if 'violation' in df.columns:
		df['bw_amt'] = df.apply(
			lambda x: lookuplist(x['violation'], penalties, 1) 
			if ( x['assumed_backwage'] == 1 )
			else x['bw_amt'], axis=1 )
	
	return df

def infer_WHD_prevailing_wage_violation(df):
	#dbra_cl_violtn_cnt, dbra_bw_atp_amt, dbra_ee_atp_cnt
	if "dbra_cl_violtn_cnt" in df.columns:
		df.loc[df["dbra_cl_violtn_cnt"] > 0, "violation_code"] = "DBRA"
	#if "dbra_bw_atp_amt" in df.columns:
		#df.loc[df["dbra_bw_atp_amt"] > 0, "violation_code"] = "DBRA"
	#if "dbra_ee_atp_cnt" in df.columns:
		#df.loc[df["dbra_ee_atp_cnt"] > 0, "violation_code"] = "DBRA"

	return df

def lookuplist(trade, list_x, col):
	trigger = True
	if pd.isna(trade):
		trade = ""
	for x in list_x:
		if x[0].upper() in trade.upper():
			value_out =+ x[col] # += to capture cases with multiple scenarios
			trigger = False
			#break
	if trigger: #if true then no matching violations
		Other = ['Generic', 26.30, 33.49, 50.24, 50.24]
		value_out = Other[col] #if value not found then return 0 <-- maybe this should be like check or add to a lrearning file
		tradenames = open('report_output/new_trade_names.txt', 'a')
		tradenames.write(trade)
		tradenames.write("\n")
		tradenames.close()
	return value_out

def calculate_interest_owed(df):
	if 'findings_start_date' and 'findings_end_date' in df.columns:
		df['findings_start_date'] = pd.to_datetime(df['findings_start_date'], errors = 'coerce')
		df['findings_end_date'] = pd.to_datetime(df['findings_end_date'], errors = 'coerce' ) #crashed here 2/5/2022 "Out of bounds nanosecond timestamp: 816-09-12 00:00:00"
		df['Calculate_start_date'] = df['findings_start_date'].fillna(df['findings_end_date'])
		df['Days_Unpaid'] = pd.to_datetime('today') - df['Calculate_start_date']
		# df['Days_Unpaid'] = np.where(df['Days_Unpaid'] < pd.Timedelta(0,errors='coerce'), (pd.Timedelta(0, errors='coerce')), df['Days_Unpaid'] )

		df['Years_Unpaid'] = df['Days_Unpaid'].dt.days.div(365)
		r = 10
		n = 365
		df['Interest_Accrued'] = (df['wages_owed'] * (((1 + ((r/100.0)/n)) ** (n*df['Years_Unpaid']))) ) - df['wages_owed']
		df['Interest_Accrued'] = df['Interest_Accrued'].fillna(df['Interest_Accrued'])
		df['Interest_Balance_Due'] = np.where(df['Interest_Balance_Due'] == "", df['Interest_Accrued'], df['Interest_Balance_Due'] )
		#df['Interest_Balance_Due'] = np.where(df['Interest_Balance_Due'] == 0, df['Interest_Accrued'], df['Interest_Balance_Due'] ) #<--careful this does not overwite fully paid cases
		df['Interest_Balance_Due'] = np.where(df['Interest_Balance_Due'] == False, df['Interest_Accrued'], df['Interest_Balance_Due'] )
		
		df['interest_owed'] = (df['Interest_Balance_Due'] - df["Interest_Payments_Recd"])
		df['interest_owed'] = np.where(df['interest_owed'] < 0, 0, df['interest_owed'] )
		#total_int_bal = df['interest_owed'].sum()
	return df


def MoveCompanyLiabilityTermsToLiabilityTypeColumn(df):
	
	df['Liabilitytype'] = "" #create new column and fill with str

	liability_terms0 = ['(a California)', '(a Californi)', '(a Californ)']
	pattern_liability0 = '|'.join(liability_terms0)
	if 'legal_nm' and 'trade_nm' in df.columns:
		foundIt0=(df['legal_nm'].str.contains(pattern_liability0, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_liability0, flags=re.IGNORECASE, regex = True) )

		df['Liabilitytype'][foundIt0] = 'California'

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a California', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Californi', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a California', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Californi', '', regex=True, case=False)


		liability_terms1 = ['(a Delaware)']
		pattern_liability1 = '|'.join(liability_terms1)
		foundIt1=(
			df['legal_nm'].str.contains(pattern_liability1, flags=re.IGNORECASE, regex = True) | 
			df['trade_nm'].str.contains(pattern_liability1, flags=re.IGNORECASE, regex = True) 
			)
		df['Liabilitytype'][foundIt1] = 'Delaware'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Delaware', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Delaware', '', regex=True, case=False)


		liability_terms2 = ['(a Nevada)']
		pattern_liability2 = '|'.join(liability_terms2)
		foundIt2=(df['legal_nm'].str.contains(pattern_liability2, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_liability2, flags=re.IGNORECASE, regex = True) )

		df['Liabilitytype'][foundIt2] = 'Delaware'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Nevada', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Nevada', '', regex=True, case=False)


		liability_terms3 = ['(a Nebraska)']
		pattern_liability3 = '|'.join(liability_terms3)
		foundIt3=(df['legal_nm'].str.contains(pattern_liability3, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_liability3, flags=re.IGNORECASE, regex = True) )

		df['Liabilitytype'][foundIt3] = 'Nebraska'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Nebraska', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Nebraska', '', regex=True, case=False)


		liability_terms4 = ['(a foreign)']
		pattern_liability4 = '|'.join(liability_terms4)
		foundIt4=(df['legal_nm'].str.contains(pattern_liability4, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_liability4, flags=re.IGNORECASE, regex = True) )

		df['Liabilitytype'][foundIt4] = 'Foreign'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Foreign', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Foreign', '', regex=True, case=False)


		liability_terms5 = ['(Which Will Be Doing Business In California)', '(Which Will Be Doing Business In California As)']
		pattern_liability5 = '|'.join(liability_terms5)
		foundIt5=(df['legal_nm'].str.contains(pattern_liability5, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_liability5, flags=re.IGNORECASE, regex = True) )

		df['Liabilitytype'][foundIt5] = 'California'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Foreign', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Foreign', '', regex=True, case=False)

	return df

def MoveBusinessTypeToBusinessTypeColumn(df):

	individual_terms = ['an individual', 'individual ']
	pattern_individual = '|'.join(individual_terms)
	if 'legal_nm' and 'trade_nm' in df.columns:
		foundIt=(df['legal_nm'].str.contains(pattern_individual, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_individual, flags=re.IGNORECASE, regex = True) )

		#df['Businesstype'] = foundIt.replace((True,False), ('Individual',df['Businesstype']), regex=True) #fill column 'industry'
		df['Businesstype'][foundIt] = 'Individual'
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('an individual', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('an individual', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('individual ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('individual ', '', regex=True, case=False)
	return df

def MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df):
	company_terms = ['LLC', 'L.L.C.','company', 'Comapany', 'a limited liability company', 'a limited liability', 'Co.', 
	'Limited Liability', 'Limited Liability Comapany', 'Limited Liability Company']
	pattern_company = '|'.join(company_terms)

	if 'legal_nm' and 'trade_nm' in df.columns:
		foundIt=(df['legal_nm'].str.contains(pattern_company, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_company, flags=re.IGNORECASE, regex = True) )

		#df['Businesstype'] = foundIt.replace((True,False), ('Company',df['Businesstype']), regex=True) #fill column 'industry'
		df['Businesstype'][foundIt] = 'Company'

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(r'\bLLC$', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(r'\bLLC$', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(r'\bL.L.C.$', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(r'\bL.L.C.$', '', False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('L.L.C., ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('L.L.C., ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('LLC, ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('LLC, ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('L.L.C. ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('L.L.C. ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('LLC ', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('LLC ', '', False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('company', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('company', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Company', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Company', '', regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('comapany', '', regex=True, case=False) #common typo
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('comapany', '', regex=True, case=False) #common typo
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Comapany', '', regex=True, case=False) #common typo
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Comapany', '', regex=True, case=False) #common typo

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a limited liability', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a limited liability', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a Limited Liability', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a Limited Liability', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('limited liability', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('limited liability', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Limited Liability', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Limited Liability', '', regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace(r'\bCo$', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace(r'\bCo$', '', False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Co. ', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Co. ', '', False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Co ', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Co ', '', False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('CO ', '', False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('CO ', '', False)
	return df

def MovePartnershipBusinessTypeToBusinessTypeColumn(df):
	partnership_terms = ['as partners', 'LLP', 'individually & jointly', 'both individually & as partners','individually and as partners', 
	'both individually and as partners', 'both individually and jointly liable']
	pattern_partner = '|'.join(partnership_terms)

	if 'legal_nm' and 'trade_nm' in df.columns:
		foundIt=(df['legal_nm'].str.contains(pattern_partner, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_partner, flags=re.IGNORECASE, regex = True) )

		#df['Businesstype'] = foundIt.replace((True,False), ('Partnership',df['Businesstype']), regex=True) #fill column 'industry'
		df['Businesstype'][foundIt] = 'Partnership'

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both jointly and severally as employers', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both jointly and severally as employers', '',  regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('jointly and severally as employers', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('jointly and severally as employers', '',  regex=True, case=False)
		
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each individually and as partners', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each individually and as partners', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both individually and as partners', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both individually and as partners', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('individually and as partners', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('individually and as partners', '', regex=True, case=False)

		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each individually and jointly liable', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each individually and jointly liable', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both individually and jointly liable', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both individually and jointly liable', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('individually and jointly liable', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('individually and jointly liable', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each individually and jointly', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each individually and jointly', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both individually and jointly', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both individually and jointly', '', regex=True, case=False)
		
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('individually and jointly', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('individually and jointly', '', regex=True, case=False)
		
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each jointly and severally', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each jointly and severally', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both jointly and severally', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both jointly and severally', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('jointly and severally', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('jointly and severally', '', regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each severally and as joint employers', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each severally and as joint employers', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each severally and as joint employ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each severally and as joint employ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('both severally and as joint employers', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('both severally and as joint employers', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('severally and as joint employers', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('severally and as joint employers', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each severally and as joint', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each severally and as joint', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('severally and as joint', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('severally and as joint', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a general partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a general partnership', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('general partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('general partnership', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a limited partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a limited partnership', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('limited partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('limited partnership', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a partnership', '',  regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('partnership', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('partnership', '',  regex=True, case=False)

		#last ditch cleanup
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('each severally and as', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('each severally and as', '',  regex=True, case=False)

	return df

def MoveCorportationBusinessTypeToBusinessTypeColumn(df):
	corporation_terms = ['inc.', 'corporation', 'corp.', 'corp', 'inc', 'a corporation', 'Incorporated', 'non-profit corporation', 
	'nonprofit', 'non-profit']
	pattern_corp = '|'.join(corporation_terms)
	
	if 'legal_nm' and 'trade_nm' in df.columns:
		foundIt=(df['legal_nm'].str.contains(pattern_corp, flags=re.IGNORECASE, regex = True) | 
		df['trade_nm'].str.contains(pattern_corp,  flags=re.IGNORECASE, regex = True) )

		#df['Businesstype'] = foundIt.replace((True,False), ('Corporation', df['Businesstype']), regex=True) #fill column business type
		df['Businesstype'][foundIt] = 'Corporation'

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('a corporation', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('a corporation', '', regex=True, case=False)
		#df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('corporation', '', regex=True, case=False)
		#df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('corporation', '', regex=True, case=False)

		#df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('CORPORATION', '', regex=True, case=False)

		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Corporation', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Corporation', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Incorporated', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Incorporated', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Corp ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Corp ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Inc ', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Inc ', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Inc.', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Inc.', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('inc', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('inc', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('Corp.', '', regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('Corp.', '', regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('non-profit corporation', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('non-profit corporation', '',  regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('nonprofit', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('nonprofit', '',  regex=True, case=False)
		df['legal_nm'] = df[df['legal_nm'].notnull()]['legal_nm'].str.replace('non-profit', '',  regex=True, case=False)
		df['trade_nm'] = df[df['trade_nm'].notnull()]['trade_nm'].str.replace('non-profit', '',  regex=True, case=False)
	return df


def RemovePunctuationFromCity(df): ##text cleanup: remove double spaces

	if 'cty_nm' in df.columns:
		df['cty_nm'] = df['cty_nm'].str.replace(',','')
		df['cty_nm'] = df['cty_nm'].str.replace('.','')
		df['cty_nm'] = df['cty_nm'].str.replace(';','')
		#df['cty_nm'] = df['cty_nm'].str.replace('-','')
		df['cty_nm'] = df['cty_nm'].str.replace("'",'')

		df['cty_nm'] = df['cty_nm'].str.replace('  ',' ')
		df['cty_nm'] = df['cty_nm'].str.replace('&','')
		df['cty_nm'] = df['cty_nm'].str.strip(';,. ')

		df['cty_nm'] = df['cty_nm'].str.replace('(','')
		df['cty_nm'] = df['cty_nm'].str.replace(')','')
		df['cty_nm'] = df['cty_nm'].str.replace('|','')
		df['cty_nm'] = df['cty_nm'].str.replace('/','')
		df['cty_nm'] = df['cty_nm'].str.replace('*','')
		df['cty_nm'] = df['cty_nm'].str.replace('  ',' ')

	return df

def RemovePunctuationFromAddresses(df): ##text cleanup: remove double spaces

	if 'street_addr' in df.columns:
		df['street_addr'] = df['street_addr'].str.replace(',','')
		df['street_addr'] = df['street_addr'].str.replace('.','')
		df['street_addr'] = df['street_addr'].str.replace(';','')
		df['street_addr'] = df['street_addr'].str.replace('-','')
		df['street_addr'] = df['street_addr'].str.replace("'",'')

		df['street_addr'] = df['street_addr'].str.replace('  ',' ')
		df['street_addr'] = df['street_addr'].str.replace('&','and')
		df['street_addr'] = df['street_addr'].str.strip(';,. ')

		df['street_addr'] = df['street_addr'].str.replace('(','')
		df['street_addr'] = df['street_addr'].str.replace(')','')
		df['street_addr'] = df['street_addr'].str.replace('|','')
		df['street_addr'] = df['street_addr'].str.replace('/','')
		df['street_addr'] = df['street_addr'].str.replace('*','')
		df['street_addr'] = df['street_addr'].str.replace('  ',' ')

	return df

def RemoveDoubleSpacesFromAddresses(df): ##text cleanup: remove double spaces

	if 'street_addr' in df.columns:
		df['street_addr'] = df['street_addr'].str.replace(',,',',')
		df['street_addr'] = df['street_addr'].str.replace('  ',' ')
		df['street_addr'] = df['street_addr'].str.strip()
		df['street_addr'] = df['street_addr'].str.strip(';,. ')
	if 'legal_nm' in df.columns:
		df['legal_nm'] = df['legal_nm'].str.replace('  ',' ')
	return df

def ReplaceAddressAbreviations(df):
	if 'street_addr' in df.columns:
		df['street_addr'] = df['street_addr'].str.replace('  ',' ')
		df['street_addr'] = df['street_addr'].str.strip()
		df['street_addr'] = df['street_addr'].str.strip(';,. ')
		df['street_addr'] = df['street_addr'].astype(str) + ' ' #add a right space to differentiate 'Ave ' from 'Avenue'
		
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CT.', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('WY.', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CTR.', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('AVE.', 'Avenue', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' ST.', ' Street', regex = False) #bugget with EAST
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('DR.', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('RD.', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('LN.', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ.', 'Plaza', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CT,', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('WY,', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CTR,', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('AVE,', 'Avenue', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' ST,', ' Street', regex = False) #bugget with EAST
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('DR,', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('RD,', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('LN,', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ,', 'Plaza', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CT ', 'Court ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('WY ', 'Way ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('CTR ', 'Center ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('AVE ', 'Avenue ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' ST ', ' Street ', regex = False) #bugget with EAST
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('DR ', 'Drive ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('RD ', 'Road ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('LN ', 'Lane ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ ', 'Plaza ', regex = False)


		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bCT$', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bWY$', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bCTR$', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bAVE$', 'Avenue', regex = False)
		#df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bST$', 'Street', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bDR$', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bRD$', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bLN$', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bPLZ$', 'Plaza', regex = False)


		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ct.', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Wy.', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ctr.', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ave.', 'Avenue', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('St.', 'Street', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Dr.', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Rd.', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ln.', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ.', 'Plaza', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ct,', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Wy,', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ctr,', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ave,', 'Avenue', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('St,', 'Street', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Dr,', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Rd,', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ln,', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ,', 'Plaza', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ct ', 'Court ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Wy ', 'Way ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ctr ', 'Center ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ave ', 'Avenue ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('St ', 'Street ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Dr ', 'Drive ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Rd ', 'Road ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ln ', 'Lane ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('PLZ ', 'Plaza ', regex = False)


		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bCt$', 'Court', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bWy$', 'Way', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bCtr$', 'Center', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bAve$', 'Avenue', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bSt$', 'Street', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bDr$', 'Drive', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bRd$', 'Road', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bLn$', 'Lane', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(r'\bPLZ$', 'Plaza', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Boulevard', 'Blvd.', False, re.IGNORECASE)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Expressway', 'Expy.', False, re.IGNORECASE)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Building', 'Bldg.', False, re.IGNORECASE)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('STE.', 'Suite', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ste.', 'Suite', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Ste ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('STE ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('APT.', 'Suite', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Apt.', 'Suite', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Apt ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('APT ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Unit ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('Unit', 'Suite', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('# ', 'Suite ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('#', 'Suite ', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('S. ', 'South ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('W. ', 'West ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('E. ', 'East ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('N. ', 'North ', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' S ', ' South ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' W ', ' West ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' E ', ' East ', regex = False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace(' N ', ' North ', regex = False)

		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('1ST ', 'First ', regex=True, case=False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('2ND ', 'Second ', regex=True, case=False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('3RD ', 'Third ', regex=True, case=False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('4TH ', 'Fourth ', regex=True, case=False)
		df['street_addr'] = df[df['street_addr'].notnull()]['street_addr'].str.replace('5TH ', 'Fifth ', regex=True, case=False)
	return df

def StripPunctuationFromNames(df):
	if 'legal_nm' in df.columns:
		df['legal_nm'] = df['legal_nm'].astype(str) #convert to str to catch floats
		df['legal_nm'] = df['legal_nm'].str.replace('  ',' ')
		df['legal_nm'] = df['legal_nm'].str.replace('&','and')
		df['legal_nm'] = df['legal_nm'].str.strip()
		df['legal_nm'] = df['legal_nm'].str.strip(';,. ')

		df['legal_nm'] = df['legal_nm'].str.replace(';','')
		df['legal_nm'] = df['legal_nm'].str.replace(',','')
		df['legal_nm'] = df['legal_nm'].str.replace('.','')
		df['legal_nm'] = df['legal_nm'].str.replace('-','')
		df['legal_nm'] = df['legal_nm'].str.replace("'",'')
		df['legal_nm'] = df['legal_nm'].str.replace('(','')
		df['legal_nm'] = df['legal_nm'].str.replace(')','')
		df['legal_nm'] = df['legal_nm'].str.replace('|','')
		df['legal_nm'] = df['legal_nm'].str.replace('/','')
		df['legal_nm'] = df['legal_nm'].str.replace('*','')
		df['legal_nm'] = df['legal_nm'].str.replace('  ',' ')

	if 'trade_nm' in df.columns:
		df['trade_nm'] = df['trade_nm'].astype(str) #convert to str to catch floats
		df['trade_nm'] = df['trade_nm'].str.replace('  ',' ')
		df['trade_nm'] = df['trade_nm'].str.replace('&','and')
		df['trade_nm'] = df['trade_nm'].str.strip()
		df['trade_nm'] = df['trade_nm'].str.strip(';,. ')

		df['trade_nm'] = df['trade_nm'].str.replace(';','')
		df['trade_nm'] = df['trade_nm'].str.replace('.','')
		df['trade_nm'] = df['trade_nm'].str.replace(',','')
		df['trade_nm'] = df['trade_nm'].str.replace('-','')
		df['trade_nm'] = df['trade_nm'].str.replace("'",'')
		df['trade_nm'] = df['trade_nm'].str.replace('(','')
		df['trade_nm'] = df['trade_nm'].str.replace(')','')
		df['trade_nm'] = df['trade_nm'].str.replace('|','')
		df['trade_nm'] = df['trade_nm'].str.replace('/','')
		df['trade_nm'] = df['trade_nm'].str.replace('*','')
		df['trade_nm'] = df['trade_nm'].str.replace('  ',' ')

	return df

def RemoveDoubleSpacesFromCompanyName(df):
	if 'legal_nm' in df.columns:
		df['legal_nm'] = df['legal_nm'].str.replace('  ',' ') #remove double spaces
		df['legal_nm'] = df['legal_nm'].str.replace(', , ',', ')
		df['legal_nm'] = df['legal_nm'].str.replace(', , , ',', ')
		df['legal_nm'] = df['legal_nm'].str.strip()
		df['legal_nm'] = df['legal_nm'].str.strip(';,. ')

	if 'trade_nm' in df.columns:
		df['trade_nm'] = df['trade_nm'].str.replace('  ',' ')
		df['trade_nm'] = df['trade_nm'].str.replace(', , ',', ')
		df['trade_nm'] = df['trade_nm'].str.replace(', , , ',', ')
		df['trade_nm'] = df['trade_nm'].str.strip()
		df['trade_nm'] = df['trade_nm'].str.strip(';,. ')
		
		df['legal_nm'] = df['legal_nm'].astype(str) + ' ' #add a right space to differentiate 'Inc ' from 'Incenerator'
		df['trade_nm'] = df['trade_nm'].astype(str) + ' ' #add a right space to differentiate 'Inc ' from 'Incenerator'
	return df


def CleanUpAgency(df, COLUMN):
	if not df.empty and COLUMN in df.columns:
		#DLSE_terms = ['01','04', '05', '06', '07', '08', '09', '10', '11', '12', '13', 
		# '14', '15', '16', '17', '18', '23','32']

		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('01', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('02', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('03', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('04', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('05', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('06', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('07', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('08', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('09', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('10', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('11', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('12', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('13', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('14', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('15', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('16', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('17', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('18', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('19', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('20', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('21', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('22', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('23', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('24', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('25', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('35', 'DLSE', False, re.IGNORECASE)
		df[COLUMN] = df[df[COLUMN].notnull()][COLUMN].str.replace('WC', 'DLSE', False, re.IGNORECASE)

	return df

def CleanNumberColumns(column):

	column = column.replace(to_replace="\$([0-9,\.]+).*", value=r"\1", regex=True)
	# column = str(column) #.astype(str)
	# #column = column.str.strip()
	column = column.replace(' ','')
	# column = column.replace('$','')
	# column = column.replace(',','')
	column = column.replace("???", "-")
	column = column.replace( '[)]','', regex=True )
	column = column.replace( '[(]','-', regex=True )
	#column.loc[column == '-', column] = 0
	column = pd.to_numeric(column, errors='coerce' )
	column = column.astype(float)

	column = column.fillna(0)
	column = column.abs()

	return column

def Cleanup_Number_Columns(df):

	if 'bw_amt' not in df.columns: df['bw_amt'] = 0
	if 'ee_pmt_due' not in df.columns: df['ee_pmt_due'] = 0
	if 'ee_violtd_cnt' not in df.columns: df['ee_violtd_cnt'] = 0
	if 'ee_pmt_recv' not in df.columns: df['ee_pmt_recv'] = 0
	if 'Interest_Balance_Due' not in df.columns: df['Interest_Balance_Due'] = 0
	if 'Interest_Payments_Recd' not in df.columns: df['Interest_Payments_Recd'] = 0
	if 'cmp_assd_cnt' not in df.columns: df['cmp_assd_cnt'] = 0
	if 'violtn_cnt' not in df.columns: df['violtn_cnt'] = 0
	
	df['bw_amt'] = CleanNumberColumns(df['bw_amt'])
	df['ee_pmt_due'] = CleanNumberColumns(df['ee_pmt_due'])
	df['ee_violtd_cnt'] = CleanNumberColumns(df['ee_violtd_cnt'])
	df['ee_pmt_recv'] = CleanNumberColumns(df['ee_pmt_recv'])
	df['Interest_Balance_Due'] = CleanNumberColumns(df['Interest_Balance_Due'])
	df['Interest_Payments_Recd'] = CleanNumberColumns(df['Interest_Payments_Recd'])
	df['cmp_assd_cnt'] = CleanNumberColumns(df['cmp_assd_cnt'])
	
	df['violtn_cnt'] = df['violtn_cnt'].abs()

	return df

def Clean_Summary_Values(DF_OG_VLN):

	DF_OG_VLN['bw_amt'] = CleanNumberColumns(DF_OG_VLN['bw_amt'])
	DF_OG_VLN['ee_pmt_due'] = CleanNumberColumns(DF_OG_VLN['ee_pmt_due'])
	DF_OG_VLN['violtn_cnt'] = CleanNumberColumns(DF_OG_VLN['violtn_cnt'])
	DF_OG_VLN['ee_violtd_cnt'] = CleanNumberColumns(DF_OG_VLN['ee_violtd_cnt'])
	DF_OG_VLN['ee_pmt_recv'] = CleanNumberColumns(DF_OG_VLN['ee_pmt_recv'])

	return DF_OG_VLN

def FormatNumbersHTMLRow (df):
	if not df.empty:
		df['bw_amt']  = df.apply(lambda x: "{0:,.0f}".format(x['bw_amt']), axis=1)
		df['ee_pmt_due']  = df.apply(lambda x: "{0:,.0f}".format(x['ee_pmt_due']), axis=1)
		df['ee_violtd_cnt']  = df.apply(lambda x: "{0:,.0f}".format(x['ee_violtd_cnt']), axis=1)
		df['ee_pmt_recv'] = df.apply(lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)
		df['violtn_cnt'] = df.apply(lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)
		
	return df


#drop duplicated backwage values, keeping the first
def DropDuplicateBackwage(df, FLAG_DUPLICATE):
	if not df.empty and 'case_id' in df.columns:
		df['case_id'] = df['case_id'].str.strip() #remove spaces
		df["ee_pmt_due_x"] = df["ee_pmt_due"]

		df["_DUP_BW"] = df.duplicated(subset=['case_id', 'legal_nm','violation', 'bw_amt'], keep=False)
		df["_DUP_PMT_REC"] = df.duplicated(subset=['case_id', 'legal_nm','violation', 'ee_pmt_recv'], keep=False)

		#violation is ignored due to a specific error in the dataset--however, for generality, this should check that violation are different and then rzero if value is the same
		df["_DUP_PMT_DUE"] = df.duplicated(subset=['case_id', 'legal_nm','ee_pmt_due'], keep=False)
		
		df["_DUP_PMT_DUE_X"] = df.duplicated(subset=['case_id', 'legal_nm','ee_pmt_due'], keep='first')
		df["ee_pmt_due_x"] = np.where(df['_DUP_PMT_DUE_X'] == True, 0, df["ee_pmt_due_x"])

		#drop for report
		if FLAG_DUPLICATE == 0:
			df["ee_pmt_due"] = np.where(df['_DUP_PMT_DUE_X'] == True, 0, df["ee_pmt_due"])
	return df

def DropDuplicateRecords(df, FLAG_DUPLICATE):

	if not df.empty and 'case_id' in df.columns:
		df['case_id'] = df['case_id'].str.strip() #remove spaces

		df = df.drop_duplicates(keep = 'first') #always remove full duplicates and nearly full duplicates
		#assumes that ee_pmt_due will sort same as ee_pmt_recv and that both values are either present or not
		df = df.sort_values('ee_pmt_recv', ascending=False).drop_duplicates(['case_id', 'violation', 'bw_amt'], keep = 'first').sort_index()

		#always label
		#df["_DUP_Person"] = df.duplicated(subset=['Submitter_Email', 'Responsible_Person_Phone'], keep=False)
		df["_Case_X"] = df.duplicated(subset=['case_id', 'violation', 'bw_amt'], keep=False)
		df["_T_NM_X"] = df.duplicated(subset=['case_id', 'trade_nm', 'street_addr', 'violation', 'bw_amt'], keep=False)
		df["_L-NM_X"] = df.duplicated(subset=['case_id', 'legal_nm', 'street_addr', 'violation', 'bw_amt'], keep=False)

		#drop for report
		if FLAG_DUPLICATE == 0:
			df = df.drop_duplicates(subset=['case_id', 'violation', 'bw_amt'], keep = 'first')
			df = df.drop_duplicates(subset=['case_id', 'trade_nm', 'street_addr', 'violation', 'bw_amt'], keep = 'first')
			df = df.drop_duplicates(subset=['case_id', 'legal_nm', 'street_addr', 'violation', 'bw_amt'], keep = 'first')

	return df


#aggregated functions*********************************

def Cleanup_Text_Columns(df):

	df = ReplaceAddressAbreviations(df)
	df = RemoveDoubleSpacesFromAddresses(df) #https://pypi.org/project/pyspellchecker/
	df = RemovePunctuationFromAddresses(df) #once more
	df = RemoveDoubleSpacesFromAddresses(df) #https://pypi.org/project/pyspellchecker/

	df = RemovePunctuationFromCity(df) #once more

	df = StripPunctuationFromNames(df)
	df = RemoveDoubleSpacesFromCompanyName(df)
	df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
	df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
	df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
	df = MoveBusinessTypeToBusinessTypeColumn(df)
	df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
	df = StripPunctuationFromNames(df)

	#run a second time
	df = RemoveDoubleSpacesFromCompanyName(df)
	df = StripPunctuationFromNames(df)
	df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
	df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
	df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
	df = MoveBusinessTypeToBusinessTypeColumn(df)
	df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
	df = StripPunctuationFromNames(df)

	#run a third time
	df = RemoveDoubleSpacesFromCompanyName(df)
	df = StripPunctuationFromNames(df)
	df = MoveCorportationBusinessTypeToBusinessTypeColumn(df)
	df = MovePartnershipBusinessTypeToBusinessTypeColumn(df)
	df = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df)
	df = MoveBusinessTypeToBusinessTypeColumn(df)
	df = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df)
	df = StripPunctuationFromNames(df)

	return df

def Signatory_List_Cleanup(df_signatory):

	df_signatory['legal_nm']= df_signatory['legal_nm'].str.upper()
	df_signatory['street_addr'] = df_signatory['street_addr'].str.upper()
	df_signatory['cty_nm'] = df_signatory['cty_nm'].str.upper()
	df_signatory['st_cd'] = df_signatory['st_cd'].str.upper()

	#deleted all companiy names shorter than 9 letters

	#add missing columns to allow program to go through expected columns
	#df_signatory['legal_nm'] = ""
	#df_signatory['street_addr'] = ""
	#df_signatory['cty_nm'] = ""
	#df_signatory['st_cd'] = ""
	#df_signatory['zip_cd'] = ""

	df_signatory['trade_nm'] = ""
	df_signatory['industry'] = ""
	df_signatory['Prevailing'] = ""
	df_signatory['Signatory'] = ""
	df_signatory['Businesstype'] = ""
	df_signatory['naic_cd'] = ""
	df_signatory['naics_desc.'] = ""

	df_signatory['length'] = df_signatory.legal_nm.str.len()
	company_name_length = 6
	##df_signatory = df_signatory[df_signatory.length > company_name_length] #https://stackoverflow.com/questions/42895061/how-to-remove-a-row-from-pandas-dataframe-based-on-the-length-of-the-column-valu

	df_signatory = StripPunctuationFromNames(df_signatory)
	df_signatory = RemoveDoubleSpacesFromCompanyName(df_signatory)
	df_signatory = MoveCorportationBusinessTypeToBusinessTypeColumn(df_signatory)
	df_signatory = MovePartnershipBusinessTypeToBusinessTypeColumn(df_signatory)
	df_signatory = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(df_signatory)
	df_signatory = MoveBusinessTypeToBusinessTypeColumn(df_signatory)
	df_signatory = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df_signatory)
	df_signatory = StripPunctuationFromNames(df_signatory)

	df_signatory['zip_cd'] = df_signatory['zip_cd'].where( #remove zipcode suffix #https://stackoverflow.com/questions/44776115/remove-four-last-digits-from-string-convert-zip4-to-zip-code/44776170
		df_signatory['zip_cd'].str.len() == 5, 
		df_signatory['zip_cd'].str[:5]
		)

	df_signatory = ReplaceAddressAbreviations(df_signatory)
	df_signatory = RemoveDoubleSpacesFromAddresses(df_signatory) #https://pypi.org/project/pyspellchecker/
	df_signatory = RemovePunctuationFromAddresses(df_signatory) #once more
	df_signatory = RemoveDoubleSpacesFromAddresses(df_signatory) #https://pypi.org/project/pyspellchecker/

	return df_signatory

def Setup_Regular_headers(df_csv): #DSLE, WHD, etc headers
	
	df_csv = df_csv.rename(columns={
		'naics_code_description': 'naics_desc.', 
		'EE_Payments_Received':'ee_pmt_recv',  #causing trouble 11/1/2020
		'Balance_Due_to_Employee(s)': 'ee_pmt_due',  #causing trouble 11/1/2020
		'street_addr_1_txt':'street_addr', 
		'legal_name':'legal_nm', 
		'Jurisdiction_or_Project_Name':'juris_or_proj_nm', 
		'case_violtn_cnt':'violtn_cnt', 
		'bw_atp_amt':'bw_amt',
		'trade':'industry'
		})

	if 'trade_nm' not in df_csv.columns: df_csv['trade_nm'] = ""
	if 'legal_nm' not in df_csv.columns: df_csv['legal_nm'] = ""
	if 'Businesstype' not in df_csv.columns: df_csv['Businesstype'] = ""

	if 'street_addr' not in df_csv.columns: df_csv['street_addr'] = ""
	if 'zip_cd' not in df_csv.columns: df_csv['zip_cd'] = 0

	if 'Prevailing' not in df_csv.columns: df_csv['Prevailing'] = ""
	if 'Signatory' not in df_csv.columns: df_csv['Signatory'] = ""
	
	if 'industry' not in df_csv.columns: df_csv['industry'] = ""
	if 'naic_cd' not in df_csv.columns: df_csv['naic_cd'] = ""
	
	if 'violation' not in df_csv.columns: df_csv['violation'] = ""
	if 'violation_code' not in df_csv.columns: df_csv['violation_code'] = ""
	if 'ee_violtd_cnt' not in df_csv.columns: df_csv['ee_violtd_cnt'] = 0
	if 'violtn_cnt' not in df_csv.columns: df_csv['violtn_cnt'] = 0
	if 'bw_amt' not in df_csv.columns: df_csv['bw_amt'] = 0

	if 'ee_pmt_recv' not in df_csv.columns: df_csv['ee_pmt_recv'] = 0
	if 'ee_pmt_due' not in df_csv.columns: df_csv['ee_pmt_due'] = 0
	if 'Paid' not in df_csv.columns: df_csv['Paid'] = ""

	if 'records' not in df_csv.columns: df_csv['records'] = 0
	
	if 'Note' not in df_csv.columns: df_csv['Note'] = ""
	if 'juris_or_proj_nm' not in df_csv.columns: df_csv['juris_or_proj_nm'] = ""
	if 'Jurisdiction_region_or_General_Contractor' not in df_csv.columns: df_csv['Jurisdiction_region_or_General_Contractor'] = ""
	
	return df_csv

def Define_Column_Types(df_csv):

	#Define column types**********************
	if 'case_id' and 'legal_nm' and 'trade_nm' and 'naic_cd' in df_csv.columns:
		df_csv['naic_cd'] = df_csv['naic_cd'].astype(str)
		df_csv['case_id'] = df_csv['case_id'].astype(str)
		df_csv['legal_nm'] = df_csv['legal_nm'].astype(str)
		df_csv['trade_nm'] = df_csv['trade_nm'].astype(str)
		df_csv['street_addr'] = df_csv['street_addr'].astype(str)
		df_csv['cty_nm'] = df_csv['cty_nm'].str.upper()
		df_csv['st_cd'] = df_csv['st_cd'].astype(str)
		df_csv['zip_cd'] = df_csv['zip_cd'].astype(str)
		
		df_csv['zip_cd'] = df_csv['zip_cd'].where( #remove zip code suffix #https://stackoverflow.com/questions/44776115/remove-four-last-digits-from-string-convert-zip4-to-zip-code/44776170
			df_csv['zip_cd'].str.len() == 5, 
			df_csv['zip_cd'].str[:5]
			)
		
		df_csv['Prevailing'] = pd.to_numeric(df_csv['Prevailing'], errors='coerce')
		df_csv["Signatory"] = pd.to_numeric(df_csv["Signatory"], errors='coerce')
		df_csv['bw_amt'] = pd.to_numeric(df_csv['bw_amt'], errors='coerce')
		df_csv['ee_pmt_recv'] = pd.to_numeric(df_csv['ee_pmt_recv'], errors='coerce')
		df_csv['ee_pmt_due'] = pd.to_numeric(df_csv['ee_pmt_due'], errors='coerce')

		df_csv['legal_nm'] = df_csv['legal_nm'].str.upper()
		df_csv['trade_nm'] = df_csv['trade_nm'].str.upper()
		df_csv['street_addr'] = df_csv['street_addr'].str.upper()
		df_csv['cty_nm'] = df_csv['cty_nm'].str.upper()
		df_csv['st_cd'] = df_csv['st_cd'].str.upper()

	return df_csv


def Signatory_Library():

	hospital_signatories = [['Health_care'], ['El Camino Hospital','El Camino Hospital Los Gatos','El Camino HospitalLos Gatos',
	'VA Palo Alto Health Care System','OConner Hospital','Santa Clara Valley Medical Center','Good Samaritan Hospital',
	'El Camino Hospital Mountain View','El Camino HospitalMountain View','El Camino Hospital Mountain View',
	'Lucile Packard Childrens Hospital','LPC Hospital','Kaiser Permanente San Jose Medical Center','Regional Medical Center of San Jose',
	'Kaiser Permanente Hospital','Kaiser Permanente Santa Clara Medical Center','Kaiser Permanente','Kaiser Permanente Medical Center',
	'Saint Louise Regional Hospital','Saint Louise Hospital','Stanford University Hospital', 'Stanford Hospital']]

	construction_signatories = [['Construction'], ["Granite Construction", "A Ruiz Construction", "Central Fence", r"\bQLM\b", 
	"Otis Elevator ", "United Technologies", "Kiewit Pacific", "West Valley Construction", r"\bFerma\b", "TEICHERT CONSTRUCTION",
	"Alliance Roofing", "Northern Underground Construction", "Albanese", "Vance Brown", "William ONeill Lath and Plastering", 
	"El Camino Paving"]]

	signatories_UCON = [['Construction'], ["Yerba Buena Engineering","WoodruffSawyer","WMA Landscape Construction","William A Guthridge","Whiteside Concrete Construction","Westside Underground Pipe","Westland Contractors",
	"Western Traffic Supply","Western States Tool","Western Stabilization","West Valley Construction","West Coast Sand and Gravel","Wayne E Swisher Cement Contractor","Walter C Smith","Walsh Construction",
	"Waller","W R Forde Associates","W C Maloney","W Bradley Electric","W Contracting","Vulcan Materials","Vulcan Construction and Maintenance","Volvo Construction Equipment","Vintage Paving",
	"Viking Drillers","Viking Construction","Veteran Pipeline Construction","Varela","Vanguard Construction","Valverde Construction",r"\bValentine\b","United Rentals","Underwater Resources","Underground Construction",
	"Trunxai Construction","Troutman Sanders","TriWest Tractor","TriValley Excavating","Trinet Construction","Trench Shoring","Trench Plate Rental","Trench and Traffic Supply","Traffic Management","Tracy Grading and Paving",
	"TPR Traffic Solutions","Total Traffic Control","Tony Skulick","Tom McCready","Thomas Lum","The Hartford","The Guarantee Company of North America","The Construction Zone","TerraCon Constructors","Tennyson Electric",
	"Teichert Waterworks","Teichert Utilities","Teichert Solar","Teichert Pipelines","Teichert","Team Ghilotti","TBC Safety","Talus Construction","Taber Construction","TDW Construction","T and S Construction","Syar Industries",
	"Sweeney Mason","Super Seal and Stripe","Sunbelt Rentals","Sukut Construction","Suarez and Munoz Construction","Sturgeon Electric California","Striping Graphics","Stormwater Specialists","Storm Water Inspection and Maint Svcs", r"\bSWIMS\b",
	r"\bStomper\b","Stoloski and Gonzalez","Stevenson Supply","Stevens Creek Quarry","Steve P Rados","Steelhead Constructors","Stacy and Witbeck","St Francis Electric","SPSG Partners","Sposeto Engineering","SpenCon Construction",
	"Sonsray Machinery","SMTD Law","Smith Denison Construction","Smith Currie and Hancock","SITECH NorCal","Sinclair General Engineering Construction","Silverado Contractors","Silvas Pipeline","Sierra Traffic Markings",
	"Sierra Mountain Construction","Shimmick Construction","Sherry Montoya","Shaw Pipeline","Sharon Alberts","Seyfarth Shaw","Serafix Engineering Contractors","Security Shoring and Steel Plate","Security Paving",
	"Schembri Construction","SANDIS Civil Engineers Surveyors Planners","Sanco Pipelines","S and S Trucking","Ryan Engineering","Rutan and Tucker","Rupert Construction Supply","Royal Electric","Rosie Garcia","Rosendin Electric",
	"Rogers Joseph ODonnell","Robust Network Solutions","Robert Burns Construction","Robert A Bothman Construction","Roadway Construction","Road Machinery","RNR Construction","Rinker MaterialsConcrete Pipe Division","RGW Equipment Sales",
	"RGW Construction","Revel Environmental Manufacturing",r"\bREM\b","Reliable Trucking","Reed and Graham","Redgwick Construction","Rebel Equipment Enterprises","RDOVermeer","RDO Integrated Controls","RCI General Engineering",
	"RC Underground","Rays Electric",r"\bRansome\b","Ranger Pipelines","Ramos Oil","RAM Rick Albert Machinery","Rain for Rent","Rafael De La Cruz",r"\bRM Harris\b","RJ Gordon Construction","RC Fischer",
	"RA Nemetz Construction","R E Maher","RandS Construction Management",r"\bRandB\b","R and W Concrete Contractors","R and R Maher Construction","R and B Equipment",r"\bQLM\b","Proven Management","Preston Pipelines",
	"Prestige Printing and Graphics","Precision Engineering","Precision Drilling","Power One","Power Engineering Construction","Poms Landscaping","PMK Contractors","Platinum Pipeline","PJs Rebar","PIRTEK San Leandro",
	"Petrinovich Pugh","Peterson Trucks","Peterson Cat","Peter Almlie","Performance Equipment","Penhall","Pedro Martinez","Pavement Recycling Systems","Paul V Simpson","Pape Machinery",
	"Pacific International Construction","Pacific Infrastructure Const","Pacific Highway Rentals","Pacific Excavation","Pacific Coast General Engineering","Pacific Coast Drilling","Pacific Boring","PACE Supply",
	"P C and N Construction","P and F Distributors","Outcast Engineering","Org Metrics","OnSite Health and Safety","Oldcastle Precast","Oldcastle Enclosure Solutions","OGrady Paving","Odyssey Environmental Services",
	"Oak Grove Construction","OC Jones and Sons","Northwest Pipe","NorCal Concrete","Nor Cal Pipeline Services","NixonEgli Equipment","Nevada Cement","Neary Landscape","Navajo Pipelines",
	"National Trench Safety","National Casting","Nada Pacific","Mozingo Construction","Mountain F Enterprises","Mountain Cascade","Moss Adams","Moreno Trenching","Mobile Barriers MBT1","MK Pipelines",
	"MJG Constructors","Mitchell Engineering","Mission Constructors","Mission Clay Products","MinervaGraniterock","Minerva Construction","Mike Brown Electric","Midstate Barrier",r"\bMichels\b","McSherry and Hudson",
	"MCK Services","McInerney and Dillon PC","McGuire and Hester","Martin General Engineering","Martin Brothers Construction","Marques Pipeline","Marinship Development Interest","Marina Landscape","Malcolm International",
	"Main Street Underground","Maggiora and Ghilotti","MF Maher","Hernandez Engineering","M Squared Construction","M and M Foundation and Drilling","Luminart Concrete","Lorang Brothers Construction","Long Electric",
	"Lone Star Landscape","Liffey Electric","Liberty Contractors","Leonidou and Rosin Professional","Lehigh Hanson","LeClairRyan","Last and Faoro","Las Vegas Paving","Landavazo Bros","Labor Services","Knife River Construction",
	"Kerex Engineering","Kelly Lynch","KDW Construction","Karen Wonnenberg","KJ Woods Construction",r"\bJS Cole\b","Joseph J Albanese","Jon Moreno","Johnston, Gremaux and Rossi","John S Shelton","Joe Sostaric",
	"Joe Gannon","JMB Construction","JLM Management Consultants","Jimni Rentals","Jifco","Jensen Precast","Jensen Landscape Contractor","Jeff Peel","JDB and Sons Construction","JCC","James J Viso Engineering","JAM Services",
	"JM Turner Engineering","JJR Construction","J Mack Enterprises","J Flores Construction","J D Partners Concrete","J and M","IronPlanet","Interstate Grading and Paving","Interstate Concrete Pumping",
	"Integro Insurance Brokers","Innovate Concrete","Inner City Demolition","Industrial Plant Reclamation","Independent Structures","ICONIX Waterworks",r"\bHoseley\b","Horizon Construction","Hess Construction",
	"Harty Pipelines","Harris Blade Rental","Half Moon Bay Grading and Paving","HandE Equipment Services","Guy F Atkinson Construction","GSL Construction","Griffin Soil Group","Graniterock","Granite Construction",
	"Gordon N Ball","Goodfellow Bros","Gonsalves and Santucci", "The Conco Companies","Golden Gate Constructors","Golden Bay Construction","Goebel Construction","Gilbertson Draglines","Ghilotti Construction","Ghilotti Bros",
	"GECMS/McGuire and Hester JV","Garney Pacific","Gallagher and Burk","G Peterson Consulting Group","Fox Loomis","Forterra","Ford Construction","Fontenoy Engineering","Florez Paving","Flatiron West","Fisher Phillips",
	"Fine Line Sawing and Drilling","Fermin Sierra Construction",r"\bFerma\b","Ferguson Welding Service","Ferguson Waterworks","Farwest Safety","Evans Brothers","Esquivel Grading and Paving","Enterprise Fleet Management",
	"Eighteen Trucking","Economy Trucking","Eagle Rock Industries","EE Gilbert Construction","Dynamic Office and Accounting Solutions","Dutch Contracting","Duran Construction Group","Duran and Venables","Druml Group",
	"Drill Tech Drilling and Shoring","Doyles Work","Downey Brand","Dorfman Construction","DMZ Transit","DMZ Builders","DLine Constructors","Dixon Marine Services","Ditch Witch West","Disney Construction",
	"DirtMarket","DHE Concrete Equipment","DeSilva Gates Construction","Demo Masters","Dees Burke Engineering Constructors","Debbie Ferrari","De Haro Ramirez Group","DDM Underground","D'Arcy and Harty Construction",
	"DW Young Construction","DP Nicoli","DA Wood Construction","D and D Pipelines","Cushman and Wakefield","Cratus Inc","County Asphalt","Corrpro Companies","Corix Water Products","Core and Main LP","Cooper Engineering",
	"Contractor Compliance","Construction Testing Services","ConQuest Contractors","CondonJohnson and Associates","Concrete Demo Works","ConcoWest","Compass Engineering Contractors","Command Alkon","Columbia Electric",
	"CMD Construction Market Data","CMC Construction","Clipper International Equipment","Champion Contractors","Central Striping Service","Central Concrete Supply","Centerline Striping","Carpenter Rigging",r"\bCarone\b",
	r"\bCampanella\b","CalSierra Pipe","California Trenchless","California Portland Cement","California Engineering Contractors","Cal State Constructors","Cal Safety","CF Archibald Paving","CandN Reinforcing","Burnham Brown",
	"Bugler Construction","Bruce Yoder","Bruce Carone Grading and Paving","Brosamer and Wall","BrightView Landscape Development","Bridgeway Civil Constructors","Brianne Conroy","Brian Neary","Brendan Coyne",r"\bBolton\b",r"\bBob Heal\b",
	"BlueLine Rental","Blue Iron Foundations and Shoring","Blaisdell Construction","Bill Crotinger","Bertco","Berkeley Cement","Bentancourt Bros Construction","Beliveau Engineering Contractors","Bear Electrical Solutions",
	"Bayside Stripe and Seal","Bay Pacific Pipeline","Bay Line Cutting and Coring","Bay Cities Paving and Grading","Bay Area Traffic Solutions","Bay Area Concretes","Bay Area Barricade Service","Bay Area Backhoes",
	"Bauman Landscape and Construction","Badger Daylighting","B and C Asphalt Grinding","Azul Works","AWSI","AVAR Construction","Atlas Peak Construction","Atkinson","Argonaut Constructors","Argent Materials",
	"Arcadia Graphix and Signs","Appian Engineering","Apex Rents","APB General Engineering","Aon Construction Services Group","Anvil Builders",r"\bAnrak\b","Andreini Brothers",r"\bAndreini\b","Andes Construction",
	"AMPCO North","American Pavement Systems","Alex Moody","AJW Construction","Advanced Stormwater Protection","Advanced Drainage Systems","Adrian Martin","A and B Construction"]]

	signatories_CEA = [['Construction'], ["Alcal Specialty Contracting","Alten Construction",r"\bOveraa\b","Cahill Contractors","Clark Construction","Clark Pacific","Dolan Concrete Construction","Dome Construction","DPR Construction",
	"Gonsalves and Stronck Construction","Hathaway Dinwiddie Construction","Howard Verrinder","Obayashi","Lathrop Construction","McCarthy Building","Nibbi Bros Associates","Peck and Hiller","Roebbelen Contracting",
	"Roy Van Pelt","Rudolph and Sletten","SJ Amoroso Construction","Skanska","Suffolk Construction","Swinerton Builders","Thompson Builders","Webcor Builders","XL Construction","Rosendin Electric","Boss Electric",
	"Cupertino Electric",'Beltramo Electric','The Best Electrical','CH Reynolds Electric','Cal Coast Telecom','Comtel Systems Technology','Cupertino Electric','CSI Electrical Contractors',
	'Delgado Electric','Elcor Electric','Friel Energy Solutions','ICS Integrated Comm Systems','Intrepid Electronic Systems','MDE Electric','MidState Electric','Pacific Ridge Electric',
	'Pfeiffer Electric','Radiant Electric','Ray Scheidts Electric','Redwood Electric Group','Rosendin Electric','Sanpri Electric','Sasco Electric','Selectric Services','San Jose Signal Electric',
	'Silver Creek Electric','Splicing Terminating and Testing','Sprig Electric','TDN Electric','TL Electric',r'\bTherma\b','Thermal Mechanical','Don Wade Electric', 'ABCO Mechanical Contractors',
	'ACCO Engineered Systems','Air Conditioning Solutions','Air Systems Service and Construction','Air Systems','Airco Mechanical','Allied Heating and AC','Alpine Mechanial Service','Amores Plumbing',
	'Anderson Rowe and Buckley','Applied Process Cooling','Arc Perfect Solutions','Axis Mechanicals','Ayoob and Peery','Ayoob Mechanical','Bacon Plumbing','Bay City Mechanical','Bayline Mechancial',
	'Bell Products','Bellanti Plumbing','Booth Frank','Brady Air Conditioning','Broadway Mechanical Contractors','Brothers Energy','Cal Air','Cal Pacific Plumbing Systems',r'\bCARRIER\b',
	'City Mechanical','CNS Mechanical','Cold Room Solutions','Comfort Dynamics','Commerical Refrigeration Specialist','Cool Breeze Refrigeration','Critchfield Mechanical','Daikin Applied','Daniel Larratt Plumbing',
	'Desert Mechanical','Done Rite Plumbing','Dowdle Andrew and Sons',r'\bDPW\b','Egan Plumbing','Emcor Services', 'Mesa Energy','Envise','Estes Refrigeration','Green Again Landscaping and Concrete','Hickey W L Sons',
	'Johnson Controls M54','KDS Plumbing','KEP Plumbing','Key Refrigeration','Kinectics Mechanical Services','KMC Plumbing','KOH Mechanical Contractors',r'\bKruse L J\b','Larratt Bros Plumbing',
	'Lawson Mechanical Contractors',r'\bLescure\b','LiquiDyn','Marelich Mechanical','Masterson Enterprises','Matrix HG','McPhails Propane Installation',r'\bMcPhails\b',r'\bMitchell E\b','Monterey Mechanical',
	'MSR Mechanical','Murray Plumbing and Heating','OC McDonald','OBrien Mechanical','OMNITemp Refrigeration','Pacific Coast Trane','PanPacific Mechanical','Peterson Mechanical',
	r'\bPMI\b','POMI Mechanical','Pribuss Engineering','Quest Mechanical','RG Plumbing','Redstone Plumbing','Refrigeration Solutions','Reichel', 'C R Engineering','Rigney Plumbing',
	'Rountree Plumbing and Heating','S and R Mechanical','Schram Construction','Southland Industries','Spencer F W and Sons','Temper Insulation','Therma','Thermal Mechanical','United Mechanical',
	'Valente A and Sons','Westates Mechanical','Western Allied Mechanical','White Water Plumbing','Blues roofing']]

	SIGNATORIES = [['All_SIGNATORIES'], hospital_signatories, signatories_CEA, signatories_UCON,construction_signatories]

	return SIGNATORIES

def ALL_NAICS_LIBRARY():
	# ^ notation means the term must prefix the string
	# (...) captures substring like A(pple) https://unbounded.systems/blog/3-kinds-of-parentheses-are-you-a-regex-master/
	#\b matches word boundaries between word and non-word https://www.regular-expressions.info/wordboundaries.html
	#r' always write pattern strings with the 'r' just as a habit https://developers.google.com/edu/python/regular-expressions
	#. matches any single character except newline '\n' https://developers.google.com/edu/python/regular-expressions
	#* matches preceding char repeatedly, with .* adds any number of additional chars
	#! customized by infer industry to exclude term, for example '(!Market)'

	#NAICS_INDUSTRIES 
	#agriculture 
	agriculture_terms = ['Agriculture', r'Agricult.*', r'Forest.*', "^(111)", "^(1112)", "^(113)", "^(114)", "^(115)", 'Fishing', 'Hunting$',
	r'farm.*', 'orchard', 'vineyard', 'mushroom', 'garlic', 'nursery', 'cattle', r'\branch.*', 'dairy', 
	r'horse.*', 'equine', 'timber', 'logging', r'harvest.*', 'pumpkin', 'livestock', 'Guglielmo', 'Garrod Farms', 'Tilton Ranch',
	'Del Fresh Produce', 'Chiala Farm', 'Nature Quality', 'Christopher Ranch', 'Syngenta', 'Sakata Seeds', 'Headstart Nursery', 
	'Boething Treeland', 'B & T Farms', 'B and T Farms', 'Mission Ranches', 'Kirigin Cellars', 'Uesugi Farms', 'Matt Bissell Forestry', 
	'Big Oak Ranch', 'NURSERIES', 'HORTICULTURE', r'SANDRIDGE.*PARTNERS', '(!Market)', '(!Store)'] #
	#mining 
	mining_terms = ['Mining', 'Mining', "^(211)", "^(212)", "^(213)", 'Quarrying', r'\bOil\b', r'\bGas\b', 'PETROLEUM', 'Extraction', 
	'quarry', r'\bmine\b'] 
	#utilities 
	utilities_terms = ['Utilities', r'utilit.*', 'Electric', 'Power', 'Generation', 'Hydroelectric', 'Nuclear', 'Geothermal', 'Solar', 
	r'\bWind\b', 'Biomass', 'Natural Gas', 'Sewer', 'Water supply', "^(221)", '(!MANPOWER)'] 
	#construction 
	construction_terms = ['Construction', '^(23)', r'plumb.*', r'construct.*', 'MANPOWER','build','floor', r'landscap.*',  
	'mechanical', 'elevator', 'plaster', 'frame', 'concrete', 'roof', 'glass', 'tile', r'dry.*wall', 'painting', 'painter', 'remodel', 'cabinet', 
	'carpet', 'drafting', 'carpentry', 'AIR SYSTEMS', r'sheet.*metal', 'pipe', 'HOMES', 'asbestos', r'custom.*home', 'maintenance', 'window', r'tree.*trim', 'iron', 'heating', 
	r'air.*condition', 'instal', 'insulation', 'MOUNTAIN AIR', 'contract', 'brick', 'paving', 'sprinkler', 'improvement', 'renovat', 'energy', 
	'mason', 'marble', 'cooling', '161000', '162000', "^(23)", '230000', "^(3323)", "^(3334)", '333415', '337110', '337212', '339950', 
	'423320', '532410', '541350', '561730', '561790', '562910', '623200', r'tree.*service', 'Excavating', 'Restoration', 'MOULDING',
	'electric', 'labor', 'ASPHALT','CEMENT', 'FENCING', r'HOME.*REPAIR', r'WOOD.*WORKING', 'ROOTER', r'\bHVAC\b', '^(54132)', r"(!Farm\b)",
	r"(!.*CARE.HOME.*)",
	r"(!RETIREMENT)", r"(!.*GROUP.HOME.*)", r"(!.*RESIDENTIAL.*CARE.*)",
	r"(!CATERING)", r"(!AUTO.*BODY)", r"(!FOOD.*SERVICE)", r"(!CLEANING)", r"(!ELDERLY)"] #'PLUMBING',
	
	#manufacturing 
	manufacturing_terms = ['Manufacturing', r'Manufactur.*', 'machine', 'milling', 'millwork', 'Product', 'Preserving', 'Slaughter', 'Dairy', 
	'Packaging', 'Beverage', 'Textile', 'Apparel', 'Printing', 'Petroleum', 'WELDING', 'Pesticide', 'Forging', 'Industrial', 'Metalworking', 
	'Machinery', 'Semiconductor', 'Electromedical', 'CIRCUITS', 'Appliance', 'ARMATURE', 'Component', 'APPLE', 'Aerospace', 'Furniture', 
	'^(311)', '^(312)', '^(313)', '^(314)', '^(315)', '^(316)', '^(317)', '^(318)', '^(319)', '^(320)', '^(321)', '^(322)', '^(323)', 
	'^(324)', '^(325)', '^(326)', '^(327)', '^(328)', '^(329)', '^(330)', '^(331)', '^(332)', '^(333)', '^(334)', '^(335)', '^(336)', 
	'^(337)', '^(338)', '^(339)', "feed", 'hay', r'HEWLETT.PACKARD', 'PLATING', 'panaderia', 'MACHINING', 'PRECISION', 'FABRICATION',
	'WINERY','INDUSTRIES', 'TELEDEX', 'ASSEMBLY', 'ALLOYS', 'ESP SAFETY', 'WELDERS', r'METAL.*FINISH'] 
	#trade 
	trade_terms = ['Trade', '^(423)', '^(424)', '^(425)', 'trading', 'trader', 'Wholesale', 'Broker', 'merchant', 'lumber', r'material.*', 
	'plywood', r'millwork.*', r'home.depo','lowes', 'DISTRIBUTION', 'DISTRIBUTOR'] 
	#retail
	#retail trade
	retail_terms = ['Retail','^(44)','^(45)', "(store)", 'SPORTSWEAR', "DOLLAR STORE", 'Paints', 'PLATOS CLOSET', 'PAPAYA', 'SWAROVSKI', 
	r'VAPE.*', 'Victoria Secret', 'SAVERS', 'PETCO', r'QUICK.STOP', 'KOHLS', 'KOHLS', r'FOREVER.21', 'CASK N FLASK', 'Target', r'Smoke.*', 
	r'Guitar.*', 'Ross Dress for Less', 'SUPPLIES', r'Sherwin.Williams', r'Quik.Stop', 'Mattress', 'Bevmo', r'Jewl.*', 'Dollar Tree', 
	'7Eleven', 'ampm', 'music', 'audio', 'EBAY', 'HERBAL', 'tinting', "water", "(LIQUOR)", '(liqour)', 'mercado', 'palenteria', "quik stop", 
	r'flower.*', 'bike shop', r'SUPERMARKET.', 'CARNICERIA', 'CHEVRON', "MARKET", 'produce', '711', '7eleven', 'seveneleven', 'gasoline', 
	r'gas.*station', 'convenience', 'safeway', 'COLLECTIBLE', 'PETSMART', 'PHARMACY', 'FASHION', 'FLORIST', 'HARDWOOD', 'ELECTRONICS',
	'WALMART', 'JEWELE', 'JEWELR', 'LUXURY', 'IMPORT', r'TOYS.*R.*US', 'OUTLET', 'TROPHIES','CIGARETTES', 'OXYGEN', r'RITE.*AID','SUPPLY',
	'QUIK AND SAVE', 'VALERO', r'RUSSELLS.*FURNTIURE', 'CLOTHING', 'COSMETICS', 'GROCERY', 'BRIDAL', 'CIGARS', 'BAZAR'] 
	#transportation 
	gig_terms = ['Gig', "Uber", "Lyft", "DoorDash", "Instacart", "Postmates", "TaskRabbit", "Wonolo", "Tradehounds", "Handy", "Amazon Flex", 
	"Bellhops", "Care.com", "Caviar", "Closet Collective", "Crowdflower", "Dolly", "Etsy", "Fancy hands", "Favor", "Feastly", "Fiverr", 
	"Freelancer", "Gigwalk", "Grubhub", "HelloTech", "HopskipDrive", "Hubstaff Talent", "Juno", "Moonlighting", "onefinestay", "openairplane", 
	"peopleperhour", "prefer", "rentah", "roadie", "rover", "shipt", "snagajob", "spare5", "sparehire", "spothero", "takl", "taskeasy", 
	"turo", "upwork", "VRBO", "Vacation Rentals by Owner", "Wingz", "airbnb", "yourmechanic", "zeel"]
	transportation_terms = ['Transportation', '^(48)', '^(49)', r'Transport.*', 'TRANSPORATION', 'railroad', 'air transportation', 'trucking', 'freight', 
	'transit', 'taxi', 'limousine', 'charter', 'pipeline', 'MOVING', 'air traffic', 'packing', 'postal service', 'courier', 'delivery', 'warehouse', 
	'storage', 'TOWING', 'MOVERS', 'DRIVERS', 'DELIVERIES', 'AMAZON', 'SHUTTLE', 'VAN LINES', 'ROADWAYS', 'YELLOW CAB', 'HAULING','PACKAGE',
	'FEDEX', 'ROYAL COACH', 'AMBULANCE', r'RENT.*CAR', r'car.*rent', r'\bCAB\b'] + gig_terms
	#information 
	information_terms = ['Information', 'Information','^(51)', 'LOGIC', 'TECHNOLO', 'newspaper', 'publisher', 'software', 'video', 'recording', 
	'radio', 'broadcasting', 'programming', 'wireless', 'MOBILE', 'SYSTEMS', 'COMMUNICATIONS', 'ATANDT', 'telecommunication', 'NETWORK', 
	'SILICON', 'satellite', 'data', 'MOBILITY', 'NETFLIX','processing', 'hosting', 'library', 'archive', 'internet', 'PUBLISH', r'\bNEWS\b',
	'TRONIC', 'ZAGACE', 'MAGAZINE', 'TELECOM', 'CELLULAR', 'INTEGRATED'] 
	#finance 
	finance_terms = ['Finance', 'Financ','^(52)', "^(429)", "^(522)", "^(521)", 'capital', r'asset.*management', 'banking', r"Wells.Fargo", 
	r"(Chase.Bank)", r"(JP.Morgan)", 'CITIBANK', r'\bBank\b', r"Credit.*union", r"Credit.*card", 'insurance', r'saving.*',
	'securities', r'^MED.*PRO$', 'investment', r'stock.*broker', r'DISCOUNT.*TAX', r'\btaxes\b','adjusting', r'CREDIT.*SOLUTION', 'brokerage', 'fund', 
	'MORTGAGE', 'PAYPAL', 'welfare', 'LENDING', 'HOLDINGS', 'REFINANCING', r'ASSETS.*GROUP', 'ESTATE OF'] 
	#real estate 
	real_estate_terms = ['Real_estate', r'real.*estate', '^(53)', 'lease', 'residential', r'commercial.*property', r'property.*management', 
	'appraiser', 'rental', 'renting', r'invest.*property', 'PROPERTIES', r'income.*property', 'PROPERTY', 'APARTMENTS', r'\bland\b',
	'PROMOTE ROI', 'SUMMER HILL TERRACE'] 
	#professional 
	professional_terms = ['Professional', 'Professional','^(54)', '^(5413)', '^(5419)', 'legal', 'lawyer', 'accounting', r'book.*keeping', 'payroll', 
	'architect', 'architecture','engineering', 'drafting', 'inspection', 'surveying', 'laboratories', 'design', 'graphic', 'interior', 
	'consulting', 'marketing', 'PROMOTIONS', 'environmental', 'CIVIL ENGINEER', 'ASSOCIATES', 'EVENT PRO', 'DOCUMENT', 'IMAGING', 'LAW OFFICE', 'research', 'VIRTUAL', 'APPRAISAL', 'development', 'advertising', r'public.*relations', 'media', 'scientific', 
	'photography', 'science', 'net optics', 'COMPUTER', 'LABS', 'interpreter', 'DIAGNOSTICS', 'TRUSTEE', 'PHOTO', 'STUDIOS', 'ACCOUNTAN',
	'N2N SECURE', r'LAW.*GROUP', 'INTRANET', 'PRINTERS', r'HOME.*OWNER.*ASSOC', r'\bHOA\b', 'CIRRUS SOLUTIONS', r'BLUE.*CHIP .*TEK', 'AUTOCHLOR SYSTEM',
	r'TECH.*SHOP', 'ANALYTICAL', 'LOCAL UNION', 'LAW FIRM', 'COUNSELING', 'GEOTECHNICAL', 'CONSERVATOR', r'BAIL.*BOND', 'FAMILY TRUST', 'EMBROIDERY']
	#management 
	management_terms = ['Management', '^(55)', '^(56)', 'Management', 'ENTERPRISES', 'LOGISTICS', 'SLINGSHOT CONNECTIONS'] 
	#administrative 
	janitorial_terms = ['Janitorial', r'Janitor.*', r'month.*clean', r'rental.*clean', r'week.*clean', r'condo.*clean', 
	r'clean.*vacanc', r'airbnb.*clean', r'move.*in.*clean', r'resident.*clean', r'post.*construct.*clean', r'apartment.*clean', 
	r'profession.*clean', r'move.*out.*clean', r'clean.*lady', 'green clean', r'commercial.*clean', r'home.*clean', r'clean.*company', 
	r'deep.*clean', r'maid.*', r'spring.*clean', r'maid.*service', r'window.*clean', r'house.*clean', r'clean.*service.*', r'house.*keep', 
	r'carpet.*clean','561720', '5617', 'MAINTENENCE'] 
	waste_terms = ['Waste', 'Waste','^(5621)', '^(5621)', '^(5629)', 'WRECKERS', 'RECYCLING', r'solid.*waste', 'hazardous', 'remediation', 'landfill', 
	'incinerator', 'septic', 'DISMANTLER', 'DISPOSAL']
	administrative_terms = ['Administrative', r'Administrat.*','^(56)']
	#education 
	educational_terms = ['Educational', r'educat.*','^(61)', 'school', 'university', 'college', 'training', 'instruction', 'lecture',  
	'learning', r'tutor.*', 'CULINARY', '(!MEDICINE)'] 
	#healthcare 
	residential_carehome_terms = ["Residential_carehome", r"resident.*care.*home", r"assist.*living", r"board.*care.*home" ,
	r"senior.*care.*home", r"resident.*home.*care", r"nursing.*home", r"memory.*care", r"home.*senior", r"care.*senior",
	r"senior.*care", r"sunrise.*manor", r"elderly.*care.*facility", r"care.*center", r"senior.*care", r"elderly.*care",
	r"garden.*villa", r"home.*elderly", "residence", "manor", r'\bvilla\b', "haven", r"resident.*care", r"home.*care", r'ADULT.*DAY.*PROGRAM',
	r"board.*and.*care", r"residential.*facility", "^(6241)", "^(5170)", "^(6232)", "^(6239)", "^(6233)", "^(6216)", "^(6242)", 
	'CLOVERLEAF CARE'] # removed 11/16/2020 per discussion w/ Felwina "care", 
	carehome_terms = ['Carehome', r'care.*home', r'.*care.*home.*', r'\bRCFE\b', r'\bChild\b', 'RETIREMENT', 'Convalescence', r'Adult.*Resident.*Care', 
	r'Group.*Home', r'Crisis.*Nurseries', r'Short.*Term.*Residential', 'Therapeutic', 'Crisis', r'child.*care', 'preschool', r'\byouth\b', 
	'adolescent', 'transitional', 'lodge', 'maternity', 'cottage', 'recovery', 'early', 'Montessori', 'kindercare', 
	r'develop.*disabl.*', r'disablit.*', r'special.*need', 'academy', r'\bYMCA\b', r"\bresiden.*", r'DAY.*CARE', r'DAY.*CENTER',
	'crisis', 'Elwyn', 'treatment', r'total.care', r'skilled.*nursing', r'house.*home.*health', r'adult.*care.*facility', 
	'RCFAs', 'RCFEs', '^(RCFA)', '^(RCFE)', '^(RFCE)', '^(SNF)$'] #10/30/2020 removed 'learning', 'home', 'lane', 'camp', 'ranch', 'nursery', 'Family', 
	healthcare_terms = ['Health_care', '^(62)', r'Health.*care', r'\bDMD\b', r'\bMD\b', r'\bDDS\b', 'AMBULATORY', 'GENETICS', 'HOSPITAL',
	'DENTAL', 'MEDICINE', 'medical', 'health', 'doctor', 'SURGERY', 'KAISER PERMANENTE', 'OPTOMETRI', 'MEDICAL CENTER', 'PHARMA', 'ANKLE',
	'BEHAVIORAL', 'PEDIATRICS', 'ALLERGY', 'ASTHMA', 'DENTIST', 'CHIROPRACTIC', 'ACUPUNCTURE', 'REHABILITATION', 'OPTOMETRY', 
	'LIFES CONNECTIONS', 'PROSTHETICS', 'ORTHOSYNETICS', 'CANCER', r'SUBACUTE.*CARE', r'URGENT.*CARE', 'ACCUPUNCTURE ', '(!EDUCATION)'] 
	#entertainment 
	#Art, Entertainment, and Recreation
	entertainment_terms = ['Entertainment', '^(71)', 'performing', 'Billiard', 'theater', 'dance', 'musical', 'artist', r'sport.*', 'team', 
	'racetrack', 'athlete', 'entertainer', 'writer', 'museum', 'historical', 'zoo', 'botanical', 'BADMINTON', 'nature park', 'amusement', 'gambling', 
	'golf', 'casino', 'marina', 'bowling', r'PERFORMING ART.', r'SOCCER.*LEAGUE', 'SOCCER', 'LEAGUE', 'BASEBALL', 'football', 'ATHLETIC',
	r'MARTIAL.*ART', 'HOOKAH', 'GREAT AMERICA', '(!bar)', '(!SPORTSWEAR)', '(!Transport)'] 
	fitness_terms = ['Fitness',r'Fitness.*', '^(71394)', r'gym.*', 'YOGA', r'JENNY CRAIG.', r'crossfit.*', r'physical.*', r'fit.*']
	#accommodation 
	accommodation_terms = ['Accommodation', '^(72)', r'Accommodat.*', 'hotel', 'motel', 'LODGING', r'BOARD.*HOUSE', r'\binn\b', 'HILTON',
	r'EXTENDED.*STAY']
	#Food Retail
	fast_food_terms = ['Fast_food', r'fast*.food', '722513', '722514','722515', '^(7222)', '^(72233)', '311520', '311920',
	r'Limit.*Service', r'sweet.*', 'Bagel', 'Buffalo', r"\bCHA\b", r"CHA\b", r"\bstreet food\b", "AandW", "Arby", "Auntie Anne", 
	"Burger King", "Carls Jr", r'\bcafe\b', 'Chipotle', r"Chuck.E.Cheese", "Churchs Chicken", "Cinnabon", "Dairy Queen", 
	"Dominos Pizza", "Domino", "Dunkin", "Five Guy", "Hardee", "Jack in the Box", "Jollibee", "KFC", "Little Caesar", "Long John Silver", 
	r"Mc.*Donald", "Panda Express", 'CUPCAKE', "Papa John", "The Pizza Company", "Pizza Hut", "Popeye", "Quizno", "Starbuck", 'Subway', 
	'Little Caesars', 'Round Table', 'Taco Bell', 'In N Out', "TCBY", "Tim Horton", "TKK Fried", r"Wendy.*s", r"Wing.stop", 'Blaze Pizza', 
	"WingStreet", r"Baskin.*Robbin", 'FALAFEL', "ChickfilA", "Cold Stone Creamery", "Del Taco", 'coffee', "El Pollo Loco", "Fosters Freeze", 
	"Green Burrito", "InNOut", r'CARL.*JR', "Jamba Juice", "Krispy Kreme", "LandL Hawaiian Barbecue", "Papa Murphy", "Burger", 'Candies', "Popeyes", 
	'The Habit', 'Mountain Mike', 'Jenis Splendid', 'ice cream', 'Krispy Krunchy', 'chicken', 'sonic drive in', "Sonic DriveIn", 'Yogurt', 
	'( tea )', 'tea house', 'teahouse', r'CATERING TRUCK.', 'Espresso', "Peets", 'Starbucks', 'Wetzels', 'Pretzel', 
	'Whataburger', 'White Castle', "Wienerschnitzel", 'Jersey Mike', "Churchs Chicken", r"Donut.*", 'Chick Fil A', 'Panera Bread',  
	r"Togo.*", r"HOT.*DOG", r'shake.*shack', 'poke house', 'dog haus', 'Dippin Dots', 'Tea Leaf', 'baja fresh', 'kirks steakburgers', 'the melt', 
	'Auntie Anne', 'Tutti Frutti', 'Juice It Up', 'Winchell', 'Sugarfina', 'Menchie', 'Yum Yum', 'Jimmy John', 'Chocolate', 'sandwich', 
	'Ikes', 'Charleys', 'Louisiana Fried', 'Cinnabon', 'Dutch Bros', 'Pieology', 'Ritas Italian Ice', 'Shakeys', 'Me N Eds', 'MeNEds', 
	'Islands Restaurants', 'Pressed Juicery', 'Bakers Drive Thru', r'Drive.*Thru', r'Drive.*in','Robeks', '85 Degrees C Bakery', 'Jimboys', 
	'Vitality Bowls', 'Mrs Fields Gifts', r'Fire.*house.*sub', 'Blue Bottle', 'Philz', 'Boba', 'Wahoos', 'Surf Citysqueeze', 
	'Fish Taco', 'Fatburger', 'Rallys Drive In', 'Creamistry', 'Sharetea', 'Tapioca Express', 'Sbarro', 'Original Tommys', 
	'Port Ofsubs', r'Deli.*cafe', 'Marthas Pancake Bar', 'Raising Canes', 'Chronic Tacos', 'Earthbar', 'Quiznos', 'Eriks Delicafe', 
	'Ben and Jerrys', 'Which Wich', 'Mendocino Farms', 'Juan Pollo', 'The Pizza Press', 'Joe The Juice', 'Wings N Things', 'Sweet Factory', 
	'Zpizza', 'Epic Wings', 'Smashburger', 'Johnny Rockets', 'Boba Loca', 'Bonchon', 'Jollibee', 'Project Juice', 'The Counter', 
	'Kreation Organic', 'Board Brew', 'Honeymee', 'Daphnes Mediterranean', 'Blenders In The Grass', 'Bambu Desserts', 'Itsugar', 'Patxis', 
	'Ghirardelli', 'Pollo Campero', 'The Baked Bear', 'Tcby', 'Wayback', 'Haagen Dazs', 'Somisomisoftserve', 'Boba Guys', 'Rocket Fizzsoda', 
	'Beard Papas', 'Marcos Pizza', 'Qwench Juice', 'Bluestone Lane', 'Pretzelmaker', 'Nestle Cafe', 'Sunlife Organics', 'Hungry Howies', 
	'Caffe Bene', 'Insomnia Cookies', 'Kung Fu Tea', 'Green Crush', 'The Melt', 'Freshens', 'Umami', 'Handels Homemade', 'Pizzarev', 
	'Drnk Coffee Tea', 'Groundwork Coffee', r'Earl.*sandwich', r'Pizza.*studio', 'Mooyah', 'Cinnaholic', 'Bowl Of Heaven', 'Temple Coffee', 
	'Tropicalsmoothie', 'Popbar', 'Le Macaron', 'Brusters Ice Cream', 'Capriottis Sandwich', 'Chatime', 'Kitchen United', 'Tom N Toms', 
	'The Human Bean', 'Lofty Coffee', 'La Colombe', 'Freddys Frozen', 'Matcha Cafe Maiko', 'Planetsmoothie', 'Banzai Bowls', 'Intelligentsia', 
	'Sweetfrog Premium', 'Sightglass Coffee', 'Steak Nshake', 'Great Steak', 'Teuscher Chocolates', 'Van Leeuwen Artisan', 'Godiva', 
	'The Dolly Llama', 'Manhattan Bagel', 'Hopdoddy', 'Blimpie', 'Yifang Taiwan', 'Godfathers Pizza', 'Grater Grilled', 'Red Mango', 
	'Duffs Cakemix', 'Wahlburgers', 'Duck Donuts', 'Lady M Confections', 'Clean Juice', 'Scooters Coffee', 'Stumptown Coffee', 
	'Big Apple Bagels', 'Sub Zero Nitrogen', 'Gloria Jeans Coffees', 'Frost Gelato', 'Peachwave', 'Neuhaus', 'Ritual Coffee', 
	'Great American Cookies', 'Black Rock Coffee', 'Caliburger', 'Bahama Bucks', 'Top Round Roast', 'Gloria Jeans Gourmet', 
	'Daves Hot Chicken', '800 Degrees Woodfired', 'Steak Escapesandwich', 'Brooklyn Water Bagels', 'Forty Carrots', 'American Deli', 
	r'\bdeli.*', 'Artichoke Basilles', 'Cicis', 'Cookies By Design', 'Carvel', 'Extreme Pita', 'Lindt', 'Your Pie', 'Heidis Brooklyn', 
	'Tasti Dlite', 'Rapid Fired Pizza', 'Bostons Restaurant', 'Caffe Vita', 'Checkers Drive In', 'Peter Piper Pizza', 'Pie Five Pizza', 
	'Freshberry', 'Fairgrounds Craft Coffee', 'Potbellysandwich Works', 'Foxs Pizza Den', 'Grimaldis Pizzeria', 'Daylight Donuts', 'WRAPS',
	'Ample Hills Creamery', r'Noodles.*Co.*','Marbleslab Creamery', 'Pret A Manger', "(!restaurant)", "(!deliv)", "(!DELIGHT)", "(!DELICIOUS)"] #"Togos",
	restaurant_terms = ['Restaurant', 'Restaurant', '^(7225)$', '^(7223)$', '^(72231)', '^(72232)', '^(7224)', '722511', '311811', 
	'pastrami', 'Pizza Chicago', 'MIMOSAS', 'BUN BO HUE', 'Mediterranean', r'KABOB.', 'Curry', 'Bread', 'Mandarin', 'Gourmet', 'taqueria', 
	r"\bPho\b", 'noodle', 'ramen', 'tofu', r"^\bcom\b", 'creamery', 'pizza', 'chinese', 'italian', 'indian', 'vietnames', 'korean', 
	'sandwich', 'pancake', 'TOOTSIE', 'breakfast', 'lunch', 'dinner', 'diner', 'bistro', 'china', 'cheesecake', 'bakery',r'sport.*bar',  
	r'\bbar\b', r'\bBBQ\b','thai', 'grille', 'grill', r'taco.*', 'chef', '(sushi)', 'japanese', 'kitchen', 'steak', r'fish.*', 'a la carte', 
	'vietnam', 'shiki wok', 'pasta', r'sea.*food', 'cuisine', 'brewhouse', r'cater.*', 'lounge', r'FOOD.*SERVICE','buffet', 'barbecue', 'burrito', 
	'greek', 'mexican', 'food', 'eggroll', 'PORRIDGE', 'PIZZERIA', 'brew', 'dining', 'Lobster', 'DELICIOUS', 'produce', r'markett\b', 'bowl', 'club', 
	'beverage', 'tavern', "TERYAKI", "TERIYAKI",  "El Patron", "Boston Market", "Cocos Bakery", 'Loyal Order of Moose', 'IHOP', r'house.*pancake', 
	'152 Post', 'Dennys', 'Chalateca', 'power pot', 'wing box', 'wingbox', "Pizza", 'BARBEQUE', 'BAGUETTE', 'COOKIES', r'DENNY.*S', 'EATERY', 'CUP AND SAUCER',
	r'LATINIGHTS', 'OMELETTE', 'BAKERIES', 'HOT WOK', r'ALE.*HOUSE', 'OLIVE GARDEN', 'CHOWDER', r'BAKE.*SHOP', 'BAKER', 'HOUSE OF CHU', 'BARTENDERS',
	'TAQUEROS', 'ROTISSERIE', '(!SPORTS CLUB)', '(!ATHLETIC CLUB)', '(!nail lounge)', '(!SOCCER)', "(!Technology)", '(!REMODEL)', '(!HOOKAH)', r'(!\bstore\b)', 
	'(!LIQUOR.)', '(!722513)', '(!722514)','(!722515)', '^(!7222)', '^(!72233)', '(!311520)', '(!311920)'] #'pho', 
	#service 
	service_terms = ['Service', '^(8112)', '^(8113)', '^(8114)', '^(8122)', '^(8123)','^(8129)','^(8132)','^(8133)','^(8134)','^(8139)','^(8141)',
	r'car.*audio', 'STAFFING', 'PERSONNEL', 'Postal', 'PLACEMENT', 'SECURITY', r'OFFICE.*WORK', 'SECURE', 'PROTECTION', 'GARDENER.', 'GARDENING',
	r'Alteration.*', r'Tailor.*', r'Cleaner.*', r'Dry.*clean', 'Linen', r'Laund.*', 'CANINE', 'PEST PRO', 'PATROL', 'INVESTIGATIONS', 'TERMITE',
	'PEST CONTROL', 'TEKLICON', r'OFFICE.*SOLUTION', 'EZHOME', 'GROOMING', 'PET CLINIC', 'TREE SURGEON',
	'(!FOOD)', '(!CONSTRUCTION)', '(!JANITORIAL)', '(!EDUCAT)']
	automotive_terms = ['Automotive', '^(8111)', r'Automot.*', r'car.*care', r'\bwheel.', r'\btire.', r'\btire\b', r'auto.*shop', r'auto.*parts', r'Jiffy.lube', 
	r'General Auto.*', r'Auto.*Repair', r'AUTO.*SALE', 'TRANSMISSION', r'Auto.*zone', 'MOTORS', r'AUTO.*SERVICE', r'smog.*check', 'smog', 'AVIATION', r'Exhaust*.System', r'Trans.*Repair', 
	r'Mech.*Elect', r'Body.*Paint', r'Body.*Shop', r'auto.*body', 'COLLISION', r'Glass.*Replacement', r'Oil.*Change', r'Lub.*Shop.', r'GOOD*.YEAR',
	r'CAR.*WASH', r'AUTO.*GROUP','VOLKSWAGEN', r'AUTO.*CARE', 'midas', r'AUTO.*WORK', r'AUTO.*GROUP', 'MOTORCYCLE', 'CHRYSLER', r'\bDODGE\b',
	'TOYOTA', 'NISSAN', r'AUTO.*SHOWROOM', r'MOTOR.*GROUP', r'CAR.*STEREO', r'AUTO.*DETAIL', 'MERCEDES BENZ', r'AUTO.*CENTER', 'honda',
	r'AUTO.*MART', 'PREMIER XPRESS', r'PAINT.*BODY', '(!Financial)']
	personal_care_terms = ['Personal_care', r'personal.*care', '^(8121)', r"nail.*spa", r'AESTHETIC.*', r'Hair.*', r'Hair.*Cut', r'NAIL.*SALON', 
	r'HAIR.*SALON', r'NAIL.*LOUNGE', r'hair.*dresser', r'BEAUTY.*SALON', r'spa\b', 'BEAUTY', 'LIFE SPA', r'SPA.*SALON', r'SALON.*SPA',
	'MASSAGE', 'barber', 'WELLNESS', 'salon', 'nail', 'beauty', 'BOUTIQUE', 'diet', 'SUNTAN', 'tanning', 'EYEBROW', 'REJUVENATION', r'GREAT.*CUTS']
	religion_terms = ['Religion', 'Religion','^(8131)', 'catholic', 'protestant', 'jewish', 'buddhist', 'hindu', 'muslim', 'islam', 
	'church', 'mosque', 'temple', 'synagogue', 'parish', '(!SCHOOL)','(!college)','(!university)','(!academy)']
	#public administration 
	public_administration_terms = ['Public_servant', r'Public.*admin','^(92)', r'fire depart.*', r'police.*', 'US VETERANS', 
	'UNITED STATES VETERANS', 'PUBLIC ENTITY', 'CITY OF', 'County of', 'State of', 'DEPARTMENT OF', 'SANTA CLARA COUNTY',
	r'OHLONE.*TRIBE'] 
	other_terms = ['Undefined', 'Undefined', 'No Result', 'Other Type of Facility'] #catch all unlabeled items

	professional_terms_rollup = ['Professional'] + service_terms + automotive_terms
	restaurant_terms_rollup = ['Restaurant'] + restaurant_terms + fast_food_terms
	carehome_terms_rollup = ['Carehome'] + residential_carehome_terms + carehome_terms

	service_terms_rollup = ['Service'] + service_terms + automotive_terms + personal_care_terms + religion_terms 
	healthcare_terms_rollup = ['Health_care'] + healthcare_terms + residential_carehome_terms + carehome_terms 
	accommodation_terms_rollup = ['Accommodation'] + accommodation_terms + restaurant_terms + fast_food_terms
	entertainment_terms_rollup = ['Entertainment'] + entertainment_terms + fitness_terms
	administrative_terms_rollup = ['Administrative'] + administrative_terms + waste_terms + janitorial_terms

	ALL_NAICS_INDUSTRIES = [['All NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms, 
	trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms, 
	management_terms, administrative_terms_rollup, educational_terms, healthcare_terms_rollup, entertainment_terms_rollup, 
	accommodation_terms_rollup, service_terms_rollup, public_administration_terms, other_terms] 

	WTC_NAICS_INDUSTRIES = [['WTC NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms, 
	trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms, 
	management_terms, administrative_terms, waste_terms, janitorial_terms, educational_terms, healthcare_terms, 
	residential_carehome_terms, carehome_terms, entertainment_terms, fitness_terms, accommodation_terms, restaurant_terms, 
	fast_food_terms, service_terms, automotive_terms, personal_care_terms, religion_terms, public_administration_terms, other_terms]

	#TEST_INDUSTRIES = [['test_industries'],['janitorial', 'monthly cleaning','rental cleaning'],['carehome', 'RCFE', 'Child'] + carehome_terms, restaurant_terms, fast_food_terms]

	#SPECIFIC_NAICS_INDUSTRY = [['SPECIFIC'], carehome_terms_rollup, fast_food_terms, construction_terms, personal_care_terms, gig_terms, janitorial_terms]
	
	#SPECIFIC_NAICS_INDUSTRY = [['Carehome'], carehome_terms_rollup]
	#SPECIFIC_NAICS_INDUSTRY = [['Fast_food'], fast_food_terms]
	SPECIFIC_NAICS_INDUSTRY = [['Construction'], construction_terms]
	#SPECIFIC_NAICS_INDUSTRY = [['Personal_care'], personal_care_terms]
	#SPECIFIC_NAICS_INDUSTRY = [['Gig'], gig_terms]
	#SPECIFIC_NAICS_INDUSTRY = [['Janitorial'], janitorial_terms]
	#SPECIFIC_NAICS_INDUSTRY = [['Retail'], retail_terms]

	SIX_INDUSTRIES_REPORT = [['Six-'], carehome_terms_rollup, fast_food_terms, construction_terms, personal_care_terms, 
	gig_terms, janitorial_terms, other_terms]

	return ALL_NAICS_INDUSTRIES
	#return SIX_INDUSTRIES_REPORT
	#return WTC_NAICS_INDUSTRIES
	#return SPECIFIC_NAICS_INDUSTRY


def Read_Violation_Data(TEST, TEST_CASES, federal_data, state_data):
	from os.path import exists
	
	if exists("dlse_judgements/unified_no_WHD_20190629.csv"):
		read_file0 = "dlse_judgements/unified_no_WHD_20190629.csv" #mixed SJOE, SCCBTC, DLSE
	if exists("dlse_judgements/whd_whisard_02052022.csv"):
		read_file1 = "dlse_judgements/whd_whisard_02052022.csv" #US DOL WHD website
	if exists("dlse_judgements/ordered_HQ20009_HQ_08132019.csv"):
		read_file2 = "dlse_judgements/ordered_HQ20009_HQ_08132019.csv" #CA DIR DSLE PRA

	url1 = "https://enfxfr.dol.gov/data_catalog/WHD/whd_whisard_20220713.csv.zip"
	#url2 = "https://www.researchgate.net/profile/Forest-Peterson/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019/data/61de6b974e4aff4a643603ae/HQ20009-HQ-2nd-Production-8132019.csv"
	#url2 = https://drive.google.com/file/d/1TRaixcwTg08bEyPSchyHntkkktG2cuc-/view?usp=sharing
	url2 = "https://stanford.edu/~granite/HQ20009-HQ2ndProduction8.13.2019.csv"

	if TEST == 2:
		federal_data = 1
		state_data = 1

	if TEST == 1: # read test file

		if exists("dlse_judgements/unified_test.csv"):
			read_file_test = "dlse_judgements/unified_test.csv"
			df_csv = pd.read_csv(read_file_test, encoding = "ISO-8859-1", low_memory=False, thousands=',', dtype={'zip_cd': 'str'} )
			df_csv = Setup_Regular_headers(df_csv)

	else : #TEST == 2 (use 1000) or TEST == 0 (use 1000000000) and then limited to n==TEST_CASES rows 
		#DF0 = pd.read_csv(read_file0, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
		#DF1 = pd.read_csv(read_file1, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
		#DF2 = pd.read_csv(read_file2, encoding = "ISO-8859-1", low_memory=False, thousands=',', nrows=TEST_CASES, dtype={'zip_cd': 'str'} )
		
		DF1 = read_from_url(url1)
		DF1.to_csv('report_output/_TEST_DOL_WHD_.csv')
		DF2 = read_from_url(url2)
		DF2.to_csv('report_output/_TEST_DIR_DLSE_.csv')
		
		#df_csv_0 = Setup_Regular_headers(DF0)

		df_csv_1 = Setup_Regular_headers(DF1)
		df_csv_1['juris_or_proj_nm'] = 'WHD' #Jurisdiction_or_Project_Name
		
		df_csv_2 = Setup_Regular_headers(DF2)
		df_csv_2['juris_or_proj_nm'] = 'DLSE' #Jurisdiction_or_Project_Name
		
		if federal_data == 1 and state_data == 0: df_csv = df_csv_1
		elif federal_data == 0 and state_data == 1: df_csv = df_csv_2
		#else: df_csv = pd.concat([df_csv_1, df_csv_2], ignore_index=True) #, axis=1, sort=False) # fills non overlapping columns with NAN
		#else: df_csv = pd.concat([df_csv_0, df_csv_1, df_csv_2], ignore_index=True)
		else: df_csv = pd.concat([df_csv_1, df_csv_2], ignore_index=True)

		#df_csv = df_csv.replace(r'\\n\\n',' ', regex=True) 
		#df_csv = df_csv.replace(r'\\r\\n',' ', regex=True) 
		#df_csv = df_csv.replace(r'\\r',' ', regex=True) 
		#df_csv = df_csv.replace('\n', ' ') #.replace('\r', '') #replace line returns

	return df_csv

def read_from_url(url):
	#from urllib.request import urlopen
	import urllib
	from urllib.request import Request, urlopen
	
	import io

	class AppURLopener(urllib.request.FancyURLopener):
		version = "Mozilla/102.0"
	
	opener = AppURLopener()
	resp = opener.open(url)

	#req = Request(url)
	#resp = urlopen(resp)

	x = resp.read()
	
	buf = io.BytesIO(x)
	if url[-3:] == 'zip':
		df_csv = pd.read_csv(buf, compression='zip', low_memory=False, thousands=',', encoding = "ISO-8859-1", sep=',', error_bad_lines=False)
	else:
		df_csv = pd.read_csv(buf, low_memory=False, thousands=',', encoding = "ISO-8859-1", sep=',', error_bad_lines=False)

	return df_csv

def Title_Block(TEST, DF_OG_VLN, DF_OG_ALL, target_city, TARGET_INDUSTRY, prevailing_wage_report, federal_data, state_data, textFile):
	textFile.write(f"<h1>DRAFT REPORT: Wage Theft in the Jurisdiction of {target_city} for {TARGET_INDUSTRY[0][0]} Industry</h1> \n") 
	if prevailing_wage_report == 1: textFile.write(f"<h2 align=center>***PREVAILING WAGE REPORT***</h2> \n") 
	if federal_data == 1 and state_data == 0: textFile.write(f"<h2 align=center>***FEDERAL DOL WHD DATA ONLY***</h2> \n") #2/5/2022
	if federal_data == 0 and state_data == 1: textFile.write(f"<h2 align=center>***CA STATE DLSE DATA ONLY***</h2> \n")
	textFile.write("\n")

	#all data summary block
	if TEST != 3:
		textFile.write("<p>These data are a combination of the Department of Labor Wage and Hour Division cases (not all result in judgments), the Division of Labor Standards Enforcement judgments, "
		"and the San Jose Office of Enforcement cases. The WHD data were obtained from the DOL, the DLSE data were obtained through a Section 6250 CA Public Records Act request (up to Aug 1, 2019 and does not include purged cases which are those settled and then purged typically after three years), "
		"and the City provided the SJOE data.</p>") 

	textFile.write("\n")

	textFile.write("<p>The dataset in this report is pulled from a larger dataset that for all regions and sources contains ")
	textFile.write(str.format('{0:,.0f}',DF_OG_ALL['case_id'].size ) )
	textFile.write(" cases")

	if not DF_OG_VLN['violtn_cnt'].sum()==0:
		textFile.write(", ")
		textFile.write(str.format('{0:,.0f}',DF_OG_VLN['violtn_cnt'].sum() ) )
		textFile.write( " violations")

	if not DF_OG_ALL['ee_violtd_cnt'].sum()==0:
		textFile.write(", ")
		textFile.write(str.format('{0:,.0f}',DF_OG_ALL['ee_violtd_cnt'].sum() ) ) 
		textFile.write(" employees")

	if not DF_OG_VLN['bw_amt'].sum()==0:
		textFile.write(", and  $ ")
		textFile.write(str.format('{0:,.0f}',DF_OG_VLN['bw_amt'].sum() ) ) 
		textFile.write(" in backwages")

	# if not DF_OG_VLN['ee_pmt_due'].sum()==0:
	# 	textFile.write(", and  $ ")
	# 	textFile.write(str.format('{0:,.0f}',DF_OG_VLN['ee_pmt_due'].sum() ) ) 
	# 	textFile.write(" in unpaid backwages")

	test_sum = DF_OG_VLN['ee_pmt_recv'].sum() #<--i have no idea, it works fin above but here I had to do this 11/3/2020
	if not test_sum==0:
		textFile.write(", and  $ ")
		textFile.write(str.format('{0:,.0f}',test_sum ) ) 
		textFile.write(" in restituted backwages")
	
	textFile.write(".")

	textFile.write( " This is approximately a ")
	textFile.write(str.format('{0:,.0f}',(DF_OG_VLN['bw_amt'].sum()/22000000000)*100 ) ) 
	textFile.write( "-percent sample of an estimated actual $22B annually in wage theft that occurs nationally (see https://blog.tsheets.com/2018/news/cost-of-wage-theft).</p>")

	textFile.write("\n")
	
	if TEST != 3:
		textFile.write("<p>The Federal WHD data goes back to 2000, the State DLSE data goes back to 2000, and the City SJOE data goes back to 2011.</p>")

	'''
	textFile.write( "<p>Dataset date range: ")
	DF_MIN_OG = "<undefined>"
	if not df.empty:
		DF_MIN_OG = min(pd.to_datetime(DF_OG_ALL['findings_start_date'].dropna() ) )
		textFile.write( DF_MIN_OG.strftime("%m/%d/%Y") )
	textFile.write(" to ")
	DF_MAX_OG = "<undefined>"
	if not df.empty:
		DF_MAX_OG = max(pd.to_datetime(DF_OG_ALL['findings_start_date'].dropna() ) )
		textFile.write( DF_MAX_OG.strftime("%m/%d/%Y") )
	textFile.write("</p>")
	'''

	textFile.write("<p> These data are internally incomplete, and do not include private lawsuits, stop notices, and complaints to the awarding agency, contractor, employment department, licensing board, and district attorney. ")
	textFile.write("Therefore, the following is a sample given the above data constraints and the reluctance by populations to file wage and hour claims.</p>")

	textFile.write("\n")

	textFile.write("<p>Note that categorizations are based on both documented data and intelligent inferences, therefore, there are errors. ")
	textFile.write("For the fields used to prepare this report, please see https://docs.google.com/spreadsheets/d/19EPT9QlUgemOZBiGMrtwutbR8XyKwnrEhB5rZpZqM98/edit?usp=sharing . ") 
	textFile.write("And for the industry categories, which are given shortened names here, please see https://www.naics.com/search/ . ")
	textFile.write("To see a visualization of the data by zip code and industry, please see (last updated Feb 2020) https://public.tableau.com/profile/forest.peterson#!/vizhome/Santa_Clara_County_Wage_Theft/SantaClaraCounty . </p>")

	textFile.write("\n")
	textFile.write("\n")

def City_Summary_Block_4_Zipcode_and_Industry(df, df_max_check, TARGET_INDUSTRY, SUMMARY_SIG, filename):

	result = '''
	<html>
	<head>
	<style>

		h2 {
			text-align: center;
			font-family: Helvetica, Arial, sans-serif;
		}
		
	</style>
	</head>
	<body>
	'''
	
	##zip code = loop through
	df = df.reset_index(level=0, drop=True) #drop city category

	#df = df.groupby(level=0)
	# df = df.agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
	# 	"bw_amt":'sum',
	# 	"violtn_cnt":'sum',
	# 	"ee_violtd_cnt":'sum',
	# 	"ee_pmt_due": 'sum',
	# 	"records": 'sum',
	# 	}).reset_index().sort_values(["zip_cd"], ascending=True)
	zipcode = ""
	for zipcode, df_zipcode in df.groupby(level=0): #.groupby(level=0):
		
		### print theft level in $ and employees
		test_num1 = pd.to_numeric(df_zipcode['bw_amt'].sum(), errors='coerce')
		test_num2 = pd.to_numeric(df_zipcode['ee_violtd_cnt'].sum(), errors='coerce')
		
		if test_num1 < 3000:
			#result +="<p> has no backwage data.</p>""
			dummy = "" #just does nothing
		else:
			result +="<p>"
			result +="In the "
			result += zipcode
			result +=" zip code, "
			result +=(str.format('{0:,.0f}', test_num2 ) )
			if math.isclose(test_num2 , 1.0, rel_tol=0.05, abs_tol=0.0):
				result +=" worker suffered wage theft totaling $ "
			else:
				result +=" workers suffered wage theft totaling $ "

			result +=(str.format('{0:,.0f}', test_num1 ) )
			result +=" "

			### print the industry with highest theft
			
			### check df_max_check for industry with highest theft in this zip code
			df_zipcode = df_zipcode.reset_index(level=0, drop=True) #drop zip code category
			df_zipcode = df_zipcode.groupby(level=0)

			df_zipcode = df_zipcode.agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
			"bw_amt":'sum',
			"violtn_cnt":'sum',
			"ee_violtd_cnt":'sum',
			"ee_pmt_recv": 'sum',
			"records": 'sum',
			}).reset_index().sort_values(["bw_amt"], ascending=False)

			### check df_max_check that industry in the top three for the County
			industry_list = len(TARGET_INDUSTRY)
			if industry_list > 2:
				result += ". The "
				result += df_zipcode.iloc[0,0]
				result += " industry is one of the largest sources of theft in this zip code"
				#if math.isclose(df_zipcode.iloc[0,1], df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
					#result += " (this zip code has one of the highest levels of theft in this specific industry across the County)"
					# result += str.format('{0:,.0f}', df_zipcode.iloc[0,1])
					# result += "|"
					# result += str.format('{0:,.0f}', df_max_check['bw_amt'].max() ) 

			### note if this is the top 3 zip code for the county --> df_max_check
			#if test_num1 == df_max_check['bw_amt'].max():
			if math.isclose(test_num1, df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
				result += " (this zip code has one of the highest overall levels of theft in the County)."
				# result += str.format('{0:,.0f}', test_num1)
				# result += "|"
				# result += str.format('{0:,.0f}',df_max_check['bw_amt'].max() ) 
			else:
				result += ". "
			result +="</p>"
			result +=("\n")

	result +=("\n")
	result +=("\n")

	result +=  '''
		</body>
		</html>
		'''
	#with open(filename, mode='a') as f:
	with open(filename, mode='a', encoding="utf-8") as f: 
		f.write(result)

def City_Summary_Block(city_cases, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, cty_nm, SUMMARY_SIG, filename):

	result = '''
		<html>
		<head>
		<style>

			h2 {
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
			}
			
		</style>
		</head>
		<body>
		'''
	result += '\n'
	result += '<h2>'
	result += cty_nm
	result += ' CITY SUMMARY</h2>\n'

	# if not df.empty: #commented out 10/26/2020 to remove crash on findings_start_date
	# 	DF_MIN = min(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
	# 	DF_MAX = max(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
	# 	result += ( DF_MIN.strftime("%m/%d/%Y") )
	# 	result += (" to ")
	# 	result += ( DF_MAX.strftime("%m/%d/%Y") )
	# 	result += ("</p> \n")
	# else:
	# 	result += ( "<p>Actual date range: <undefined></p> \n")
	
	result +="<p>"
	test_num1 = pd.to_numeric(df['bw_amt'].sum(), errors='coerce')
	# if test_num1 > 3000:
	# 	result +="Wage theft is a concern in the City of "
	# 	result += cty_nm.title()
	# 	result +=". "

	if city_cases < 1:
		do_nothing = ""
		# result +="No wage theft cases were found in the City of "
		# result += cty_nm.title()
		# result +=". "
	elif math.isclose(city_cases, 1.0, rel_tol=0.05, abs_tol=0.0):
		result +="There is at least one wage theft case"
		if test_num1 <= 3000:
			result += " in the City of "
			result += cty_nm.title()
		result +=", "
	else:
		result +="There are "
		result +=(str.format('{0:,.0f}', city_cases ) )
		result +=" wage theft cases"
		if test_num1 <= 3000:
			result += " in the City of "
			result += cty_nm.title()
		result +=", "

	#total theft $
	#test_num1 = pd.to_numeric(df['bw_amt'].sum(), errors='coerce')
	if test_num1 < 1 and city_cases < 1:
		do_nothing = ""
		#result +=" and, there is no backwage data. "
	elif test_num1 < 1 and city_cases >= 1:
		result +=" however, the backwage data is missing. "
	elif test_num1 > 3000:
		result +=" resulting in a total of $ "
		result +=(str.format('{0:,.0f}', test_num1 ) )
		result +=" in stolen wages. "
	else:
		result +=" resulting in stolen wages. "

	#total unpaid theft $
	test_num0 = pd.to_numeric(df['ee_pmt_recv'].sum(), errors='coerce')
	if test_num0 < 1:
		do_nothing = ""
		#result +="Of that, an unknown amount is still due to the workers of this city. "
	else:
		result +=("Of that, $ ")
		result +=(str.format('{0:,.0f}',test_num0 ) )
		result +=" has been returned to the workers of this city. "


	#total number of violations
	test_num2 = pd.to_numeric(df['ee_violtd_cnt'].sum(), errors='coerce' )
	if test_num2 < 1:
		do_nothing = ""
		#result +="Therefore, there is no case evidence of workers affected by stolen wages. "
	else:
		test_num3 = pd.to_numeric(df['violtn_cnt'].sum(), errors='coerce' )
		if math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
			result +="The theft comprises at least one discrete wage-and-hour violation "
		else:
			result +="The theft comprises "
			result +=(str.format('{0:,.0f}', test_num3 ) )
			result +=" discrete wage-and-hour violations "

	if test_num2 < 1:
		do_nothing = ""
	elif math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
		result +="affecting at least one worker: "
	else:
		result +="affecting "
		result +=(str.format('{0:,.0f}', test_num2 ) )
		result +=" workers: "

	# xx companies have multiple violations
	if len(unique_legalname.index) < 1:
		do_nothing = ""
		#result +="No employer was found with more than one case. "
	if math.isclose(len(unique_legalname.index), 1.0, rel_tol=0.05, abs_tol=0.0):
		result +="At least one employer has multiple cases. "
	else:
		result +=(str.format('{0:,.0f}', len(unique_legalname.index) ) )
		result +=" employers have multiple cases recorded against them. "

	# xx companies cited by multiple agencies
	if len(agency_df.index) < 1:
		#result +="No employer was found with cases from multiple agencies. "
		do_nothing = ""
	elif math.isclose(len(agency_df.index), 1.0, rel_tol=0.05, abs_tol=0.0):
		result +="at least one employer has cases from multiple agencies. "
	else:
		result +=(str.format('{0:,.0f}', len(agency_df.index) ) )
		result +=" employers have cases from multiple agencies. "

	#employer with top theft
	if test_num1 > 3000:
		# df = df.droplevel('legal_nm').copy()
		# df = df.reset_index()
		# df = df.groupby(['legal_nm']).agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
		# 	"bw_amt":'sum',
		# 	"violtn_cnt":'sum',
		# 	"ee_violtd_cnt":'sum',
		# 	"ee_pmt_recv": 'sum',
		# 	"records": 'sum',
		# 	}).reset_index().sort_values(["bw_amt"], ascending=True)
	
		temp_row = unique_legalname.nlargest(3, 'backwage_owed').reset_index(drop=True)
		temp_value_1 = temp_row['backwage_owed'].iloc[0]
		if temp_row.size > 0 and temp_value_1 > 0: 
			#indexNamesArr = temp_row.index.values[0] #https://thispointer.com/python-pandas-how-to-get-column-and-row-names-in-dataframe/
			#result += indexNamesArr.astype(str)
			result += temp_row['legal_nm'].iloc[0]
			result += " is the employer with the highest recorded theft in this city, "
			result += "it has unpaid wage theft claims of $ "
			result +=(str.format('{0:,.0f}', temp_value_1 ) )
			result += ". "
		
		result +="</p>"

		result +=("\n")
		result +=("\n")

	#close
	result +=  '''
		</body>
		</html>
		'''
	#with open(filename, mode='a') as f:
	with open(filename, mode='a', encoding="utf-8") as f: 
		f.write(result)

def Regional_All_Industry_Summary_Block(out_counts, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, OPEN_CASES, textFile):
	textFile.write("<h2>Summary for All Industries</h2> \n")

	if not df.empty:
		textFile.write(f"<p>Actual date range: ")
		DF_MIN = min(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce'  ) )
		DF_MAX = max(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce'  ) )
		textFile.write( DF_MIN.strftime("%m/%Y") )
		textFile.write(" to ")
		textFile.write( DF_MAX.strftime("%m/%Y") )
		textFile.write("</p> \n")
	else:
		textFile.write( "<p>Actual date range: <undefined></p> \n")

	if OPEN_CASES == 1:
		textFile.write("<p>This report has cases removed that are documented as closed or the amount repaid matches the backwages owed.</p> \n")

	if not len(out_counts.index)==0:
		textFile.write("<p>Wage theft cases: ")
		textFile.write(str.format('{0:,.0f}',len(out_counts.index ) ) )
		textFile.write("</p> \n")

	if not out_counts['wages_owed'].sum()==0:
		textFile.write("<p>Total wage theft:  $ ")
		textFile.write(str.format('{0:,.0f}',out_counts['wages_owed'].sum() ) ) #bw_amt
		if not df.empty:
			if total_ee_violtd == 0:
				textFile.write(" <i> Note: Backwages per employee violated is not calculated in this report</i></p> \n")
			else:
				textFile.write(" <i>(gaps estimated as $" )
				textFile.write(str.format('{0:,.0f}',total_bw_atp//total_ee_violtd) )
				textFile.write(" in backwages per employee violated--plus interest and penalties)</i> \n")
		textFile.write("</p>")

	if not out_counts['backwage_owed'].sum()==0:
		textFile.write("<p>Including monetary penalties and accrued interest, the amount owed is:  $ ")
		textFile.write(str.format('{0:,.0f}',out_counts['backwage_owed'].sum() ) ) #bw_amt
		textFile.write("</p>")

	# if not out_counts['ee_pmt_due'].sum()==0:
	# 	textFile.write("<p>Total unpaid wage theft:  $ ")
	# 	textFile.write(str.format('{0:,.0f}',out_counts['ee_pmt_due'].sum() ) )
	# 	textFile.write("</p>")

	# text_sum_bug_fix = out_counts['ee_pmt_recv'].sum()
	# if not text_sum_bug_fix==0:
	# 	textFile.write("<p>Total paid wage theft:  $ ")
	# 	textFile.write(str.format('{0:,.0f}',text_sum_bug_fix ) )
	# 	textFile.write("</p>")

	if not out_counts['ee_violtd_cnt'].sum()==0:
		textFile.write("<p>Total employees: ")
		textFile.write(str.format('{0:,.0f}',out_counts['ee_violtd_cnt'].sum() ) )
		textFile.write("</p> \n")

	if not out_counts['violtn_cnt'].sum()==0:
		textFile.write("<p>Total violations: ")
		textFile.write(str.format('{0:,.0f}',out_counts['violtn_cnt'].sum() ) )
		if not df.empty:
			if total_ee_violtd == 0:
				textFile.write(" <i> Note: Violations per employee is not calculated in this report</i></p> \n")
			else:
				textFile.write(" <i>(gaps estimated as " )
				textFile.write(str.format('{0:,.2g}',total_case_violtn//total_ee_violtd) )
				textFile.write(" violation(s) per employee violated)</i>")
		textFile.write("</p>")

	textFile.write("\n")
	textFile.write("\n")

	textFile.write("<p>Companies that are involved in multiple cases: ")
	textFile.write(str.format('{0:,.0f}', len(unique_legalname.index) ) ) #here
	textFile.write("</p> \n")

	if not len(agency_df.index)==0:
		textFile.write("<p>Companies that are cited by multiple agencies: ")
		textFile.write(str.format('{0:,.0f}', len(agency_df.index) ) ) #here
		textFile.write("</p> \n")

	textFile.write("\n")
	textFile.write("\n")

def Industry_Summary_Block(out_counts, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, OPEN_CASES, textFile):
	textFile.write("<h2>Summary for Reported Industry</h2> \n")

	if not df.empty:
		#textFile.write(f"<p>Date range: {YEAR_START} to {YEAR_END}</p>")

		#commented out due to out of bounds date that need an error check to fix debug 10/26/2020 Fixed 2/5/2022
		DF_MIN = min(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
		DF_MAX = max(pd.to_datetime(df['findings_start_date'].dropna(), errors = 'coerce' ) )
		
		textFile.write(f"<p>Actual date range: ")
		#textFile.write( DF_MIN.strftime("%m/%d/%Y") )
		textFile.write( DF_MIN.strftime("%m/%Y") )
		textFile.write(" to ")
		#textFile.write( DF_MAX.strftime("%m/%d/%Y") )
		textFile.write( DF_MAX.strftime("%m/%Y") )
		textFile.write("</p> \n")
	else:
		textFile.write( "<p>Actual date range: <undefined></p> \n")

	if OPEN_CASES == 1:
		textFile.write("<p>This report has cases removed that are documented as closed or the amount repaid matches the backwages owed.</p> \n")

	if not len(out_counts.index)==0:
		textFile.write("<p>Wage theft cases: ")
		textFile.write(str.format('{0:,.0f}',len(out_counts.index ) ) )
		textFile.write("</p> \n")

	if not out_counts['bw_amt'].sum()==0:
		textFile.write("<p>Total wage theft:  $ ")
		textFile.write(str.format('{0:,.0f}',out_counts['bw_amt'].sum() ) )
		if not df.empty:
			if total_ee_violtd == 0:
				textFile.write(" <i> Note: Backwages per employee violated is not calulated in this report</i></p> \n")
			else:
				textFile.write(" <i>(gaps estimated as $" )
				textFile.write(str.format('{0:,.0f}',total_bw_atp//total_ee_violtd) )
				textFile.write(" in backwages per employee violated)</i> \n")
		textFile.write("</p>")

	# if not out_counts['ee_pmt_due'].sum()==0:
	# 	textFile.write("<p>Total unpaid wage theft:  $ ")
	# 	textFile.write(str.format('{0:,.0f}',out_counts['ee_pmt_due'].sum() ) )
	# 	textFile.write("</p>")

	# text_sum_bug_fix = out_counts['ee_pmt_recv'].sum()
	# if not text_sum_bug_fix==0:
	# 	textFile.write("<p>Total paid wage theft:  $ ")
	# 	textFile.write(str.format('{0:,.0f}',text_sum_bug_fix ) )
	# 	textFile.write("</p>")

	if not out_counts['ee_violtd_cnt'].sum()==0:
		textFile.write("<p>Total employees: ")
		textFile.write(str.format('{0:,.0f}',out_counts['ee_violtd_cnt'].sum() ) )
		textFile.write("</p> \n")

	if not out_counts['violtn_cnt'].sum()==0:
		textFile.write("<p>Total violations: ")
		textFile.write(str.format('{0:,.0f}',out_counts['violtn_cnt'].sum() ) )
		if not df.empty:
			if total_ee_violtd == 0:
				textFile.write(" <i> Note: Violations per employee is not calculated in this report</i></p> \n")
			else:
				textFile.write(" <i>(gaps estimated as " )
				textFile.write(str.format('{0:,.2g}',total_case_violtn//total_ee_violtd) )
				textFile.write(" violation(s) per employee violated)</i>")
		textFile.write("</p>")

	textFile.write("\n")
	textFile.write("\n")

	textFile.write("<p>Companies that are involved in multiple cases: ")
	textFile.write(str.format('{0:,.0f}',len(unique_legalname.index) ) ) #here
	textFile.write("</p> \n")

	if not len(agency_df.index)==0:
		textFile.write("<p>Companies that are cited by multiple agencies: ")
		textFile.write(str.format('{0:,.0f}',len(agency_df.index) ) ) #here
		textFile.write("</p> \n")

	textFile.write("\n")
	textFile.write("\n")

def Notes_Block(textFile, default_zipcode):

	textFile.write("<p>Notes: ")
	textFile.write(f"(1) In the following tables and city summaries, the zip {default_zipcode} represents data that is missing the zip code field. ")
	textFile.write("(2) There are unlabeled industries, many of these are actually construction, care homes, restaurants, etc. just there is not an ability to label them as such--a label of 'other' could lead one to indicate that they are not these industries and therefore the category of 'uncategorized.' ")
	textFile.write("(3) Values may deviate by 10% within the report for camparable subcategories: this is due to labeling and relabeling of industry that may overwrite a previous industry label (for example Nail Hamburger could be labeled service or food). ")
	textFile.write("</p>")

	textFile.write("\n")
	textFile.write("\n")

def Methods_Block(textFile):
	textFile.write("<p>")
	textFile.write("Methods: ")
	textFile.write("</p>")

	textFile.write("<p>")
	textFile.write("backwage_owed:")
	textFile.write("</p>")
	textFile.write("<ul>")
	textFile.write("<li>the sum of wages owed, monetary penalty, and interest</li>")
	textFile.write("<li>df['backwage_owed'] = df['wages_owed'] + df['cmp_assd_cnt'] + df['interest_owed']</li>")
	textFile.write("</ul>")

	textFile.write("<p>")
	textFile.write("wages_owed: ")
	textFile.write("</p>")
	textFile.write("<ul>")
	textFile.write("<li>unpaid backwages less payment recieved by employee</li>")
	textFile.write("<li>df['wages_owed'] = df['bw_amt'] - df['ee_pmt_recv']</li>")
	textFile.write("</ul>")

	textFile.write("<p>")
	textFile.write("interest_owed: ")
	textFile.write("</p>")
	textFile.write("<ul>")
	textFile.write("<li>interestd due less interest payments recieved</li>")
	textFile.write("<li>df['interest_owed'] = df['Interest_Balance_Due'] - df['Interest_Payments_Recd])</li>")
	textFile.write("</ul>")

	textFile.write("<p>")
	textFile.write("Interest_Balance_Due: ")
	textFile.write("</p>")
	textFile.write("<ul>")
	textFile.write("<li>where interest balance due is missing, then infer an interest balance based on a calaculated compounded interest of the backwages owed</li>")
	textFile.write("<li>df['Interest_Balance_Due'] = np.where(df['Interest_Balance_Due'] == "", df['Interest_Accrued'], df['Interest_Balance_Due']</li>" )
	textFile.write("<li>df['Interest_Accrued'] = (df['wages_owed'] * (((1 + ((r/100.0)/n)) ** (n*df['Years_Unpaid']))) ) - df['wages_owed']</li>" )
	textFile.write("</ul>")
	
	# textFile.write("bw_amt: ")
	# textFile.write("<li> </li>")
	# textFile.write("violtn_cnt: ")
	# textFile.write("<li> </li>")
	# textFile.write("ee_violtd_cnt: ")
	# textFile.write("<li> </li>")
	# textFile.write("ee_pmt_recv: ")
	# textFile.write("<li> </li>")
	# textFile.write("records: ")
	# textFile.write("<li> </li>")
	textFile.write("</p>")

	textFile.write("\n")
	textFile.write("\n")

def Signatory_to_Nonsignatory_Block(df1, df2, filename):
	## construction 
	## medical

	# ratio_construction = df1.query(
	# 	"signatory_industry == 'construction_signatories' and signatory_industry == 'signatories_UCON' and signatory_industry == 'signatories_CEA' "
	# 	)['bw_amt'].sum() / df1['bw_amt'].sum()

	ratio_construction_ = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
	#ratio_construction_ucon = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
	#ratio_construction_cea = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
	construction_industry_backwage = df1.loc[df1['industry'] == 'Construction','backwage_owed'].sum() 
	ratio_construction = (ratio_construction_) / construction_industry_backwage

	ratio_hospital_ = df1.loc[df1['signatory_industry'] == 'Health_care','backwage_owed'].sum() 
	carehome_industry_backwage = df1.loc[df1['industry'] == 'Carehome','backwage_owed'].sum() 
	healthcare_industry_backwage = df1.loc[df1['industry'] == 'Health_care','backwage_owed'].sum()
	residential_carehome_industry_backwage = df1.loc[df1['industry'] == 'Residential_carehome','backwage_owed'].sum()
	ratio_hospital = ratio_hospital_ / (carehome_industry_backwage + healthcare_industry_backwage + residential_carehome_industry_backwage) 
	
	filename.write("<p>")
	filename.write("Of $ ")
	filename.write(str.format('{0:,.0f}', df1['backwage_owed'].sum() ) )
	filename.write(" in all industry back wages owed (inc. monetary penalty and interest), ")
	filename.write(" union signatory employers represent: </p>")
	filename.write("<p>")
	filename.write(str.format('{0:,.0f}', ratio_construction * 100 ) )
	filename.write(" percent of total documented theft in the construction industry with $ ")
	filename.write(str.format('{0:,.0f}', (ratio_construction_) ) )
	filename.write(" of $ ")
	filename.write(str.format('{0:,.0f}', construction_industry_backwage ) )
	filename.write(" in construction specific backwages. ")
	filename.write("</p>")
	filename.write("<p>")
	filename.write(str.format('{0:,.0f}', ratio_hospital * 100 ) )
	filename.write(" percent of total documented theft in the healthcare industry with $ ")
	filename.write(str.format('{0:,.0f}', ratio_hospital_ ) )
	filename.write(" of $ ")
	filename.write(str.format('{0:,.0f}', (carehome_industry_backwage + healthcare_industry_backwage + residential_carehome_industry_backwage) ) )
	filename.write(" in healthcare specific backwages. ")
	filename.write("</p>")
	filename.write("<p> Due to the situation of the union worker as fairly paid, educated in labor rights, and represented both in bargaining and enforcement of that bargained for agreement--as well as that these are two heavily unionized industries and that much of the non-union data is lost as undefined industry--then these percentages are likely overly represented as union workers would know how and when to bring a wage and hour case. As such, it is fair to conclude that there effectively is no concernable degree of wage theft in the unionized workforce that requires outside enforcement. </p>")

def Footer_Block(TEST, textFile):
	textFile.write("<p>Prepared by the Stanford University Center for Integrated Facility Engineering (CIFE) in collaboration with the Santa Clara County Wage Theft Coalition. These data have not been audited and, therefore, are only intended as an indication of wage theft.</p> \n")
	
	textFile.write("<p> Report generated ")
	textFile.write(pd.to_datetime('today').strftime("%m/%d/%Y") )
	textFile.write("</p>")

#write_to_html_file(new_df_3, header_HTML_EMP3, "", file_path('py_output/A4W_Summary_by_Emp_for_all2.html') )
def write_to_html_file(df, header_HTML, title, filename): #https://stackoverflow.com/questions/47704441/applying-styling-to-pandas-dataframe-saved-to-html-file

	result = '''
		<html>
		<head>
		<style>

			h2 {
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
			}
			table { 
				margin-left: auto;
				margin-right: auto;
			}
			table, th, td {
				border: 1px solid black;
				border-collapse: collapse;
			}
			th, td {
				padding: 5px;
				text-align: center;
				font-family: Helvetica, Arial, sans-serif;
				font-size: 90%;
			}
			table tbody tr:hover {
				background-color: #dddddd;
			}
			.wide {
				width: 90%; 
			}

		</style>
		</head>
		<body>
		'''
	result += '<h2> %s </h2>\n' % title
	if type(df) == pd.io.formats.style.Styler:
		result += df.render()
	else:
		result += df.to_html(classes='wide', columns = header_HTML, escape=False)
	result +=  '''
		</body>
		</html>
		'''
	#with open(filename, mode='a') as f:
	with open(filename, mode='a', encoding="utf-8") as f: #https://stackoverflow.com/questions/27092833/unicodeencodeerror-charmap-codec-cant-encode-characters
		f.write(result)

def debug_fileSetup_def(bug_filename):

	bug_filename.write("<!DOCTYPE html>")
	bug_filename.write("\n")
	bug_filename.write("<html><body>")
	bug_filename.write("\n")

	bug_filename.write("<h1>Here 1</h1>")
	bug_filename.write("\n")

def file_path(relative_path):
	dir = os.path.dirname(os.path.abspath(__file__))
	split_path = relative_path.split("/")
	new_path = os.path.join(dir, *split_path)
	return new_path


if __name__ == '__main__':
 	main()
