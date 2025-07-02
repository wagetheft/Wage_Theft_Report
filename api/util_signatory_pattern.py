import re
import pandas as pd
import numpy as np

from api.util_group import GroupByX

from api.wagetheft_clean_value_utils import (
    StripPunctuationFromNames,
    RemoveDoubleSpacesFromCompanyName,
    MoveCorportationBusinessTypeToBusinessTypeColumn,
    MovePartnershipBusinessTypeToBusinessTypeColumn,
    MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn,
    MoveBusinessTypeToBusinessTypeColumn,
    MoveCompanyLiabilityTermsToLiabilityTypeColumn,
    ReplaceAddressAbreviations,
    RemoveDoubleSpacesFromAddresses,
    RemovePunctuationFromAddresses,
)


def prevailing_wage_blacklist(out_target):

    out_prevailing_target = pd.DataFrame()
    out_signatory_target = pd.DataFrame()
    if 'Prevailing' in out_target.columns or "Signatory" in out_target.columns:
        unique_legalname_sig = GroupByX(out_target, 'legal_nm')
        #unique_legalname_sig  = unique_legalname_sig[~unique_legalname_sig.index.duplicated()]

        if 'Prevailing' in out_target.columns:
            out_prevailing_target = unique_legalname_sig.loc[unique_legalname_sig['Prevailing'] == 1]

        if "Signatory" in out_target.columns:
            out_signatory_target = unique_legalname_sig.loc[unique_legalname_sig["Signatory"] == 1]

    return out_prevailing_target, out_signatory_target


def SIG_EXCLUSION_LIST_GENERATOR(SIGNATORY_INDUSTRY):
        target = pd.Series(SIGNATORY_INDUSTRY)
        # https://stackoverflow.com/questions/28679930/how-to-drop-rows-from-pandas-data-frame-that-contains-a-particular-string-in-a-p
        target = target[target.str.contains("!") == True]
        target = target.str.replace('[\!]', '', regex=True)
        if len(target) > 0:
            PATTERN_EXCLUDE = '|'.join(target)
        else:
            PATTERN_EXCLUDE = "999999"
        return PATTERN_EXCLUDE


def infer_signatory_cases(df, SIGNATORY_INDUSTRY):
    if "Signatory" not in df.columns:
        df["Signatory"] = 0
    if 'signatory_industry' not in df.columns:
        df['signatory_industry'] = ""
    df = InferSignatoriesFromNameAndFlag(df, SIGNATORY_INDUSTRY)
    df = InferSignatoryIndustryAndLabel(df, SIGNATORY_INDUSTRY)
    # unused df_temp_address = InferSignatoriesFromAddressAndFlag(df, signatory_address_list)
    # unused df = InferSignatoriesFromNameAndAddressFlag(df, signatory_list, signatory_address_list)
    # unused df = df.append(df_temp_address)
    return df


def Signatory_List_Cleanup(df_signatory):

    df_signatory['legal_nm'] = df_signatory['legal_nm'].str.upper()
    df_signatory['street_addr'] = df_signatory['street_addr'].str.upper()
    df_signatory['cty_nm'] = df_signatory['cty_nm'].str.upper()
    df_signatory['st_cd'] = df_signatory['st_cd'].str.upper()

    # deleted all companiy names shorter than 9 letters

    # add missing columns to allow program to go through expected columns
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
    # df_signatory = df_signatory[df_signatory.length > company_name_length] #https://stackoverflow.com/questions/42895061/how-to-remove-a-row-from-pandas-dataframe-based-on-the-length-of-the-column-valu

    df_signatory = StripPunctuationFromNames(df_signatory)
    df_signatory = RemoveDoubleSpacesFromCompanyName(df_signatory)
    df_signatory = MoveCorportationBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MovePartnershipBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MoveLimitedLiabilityBusinessTypeToBusinessTypeColumn(
        df_signatory)
    df_signatory = MoveBusinessTypeToBusinessTypeColumn(df_signatory)
    df_signatory = MoveCompanyLiabilityTermsToLiabilityTypeColumn(df_signatory)
    df_signatory = StripPunctuationFromNames(df_signatory)

    df_signatory['zip_cd'] = df_signatory['zip_cd'].where(  # remove zipcode suffix #https://stackoverflow.com/questions/44776115/remove-four-last-digits-from-string-convert-zip4-to-zip-code/44776170
        df_signatory['zip_cd'].str.len() == 5,
        df_signatory['zip_cd'].str[:5]
    )

    df_signatory = ReplaceAddressAbreviations(df_signatory)
    df_signatory = RemoveDoubleSpacesFromAddresses(
        df_signatory)  # https://pypi.org/project/pyspellchecker/
    df_signatory = RemovePunctuationFromAddresses(df_signatory)  # once more
    df_signatory = RemoveDoubleSpacesFromAddresses(
        df_signatory)  # https://pypi.org/project/pyspellchecker/

    return df_signatory


def Signatory_Library():

    hospital_signatories = [['Health_care'], ['El Camino Hospital', 'El Camino Hospital Los Gatos', 'El Camino HospitalLos Gatos',
                                              'VA Palo Alto Health Care System', 'OConner Hospital', 'Santa Clara Valley Medical Center', 'Good Samaritan Hospital',
                                              'El Camino Hospital Mountain View', 'El Camino HospitalMountain View', 'El Camino Hospital Mountain View',
                                              'Lucile Packard Childrens Hospital', 'LPC Hospital', 'Kaiser Permanente San Jose Medical Center', 'Regional Medical Center of San Jose',
                                              'Kaiser Permanente Hospital', 'Kaiser Permanente Santa Clara Medical Center', 'Kaiser Permanente', 'Kaiser Permanente Medical Center',
                                              'Saint Louise Regional Hospital', 'Saint Louise Hospital', 'Stanford University Hospital', 'Stanford Hospital']]

    construction_signatories = [['Construction'], ["Granite Construction", "A Ruiz Construction", "Central Fence", r"\bQLM\b",
                                                   "Otis Elevator ", "United Technologies", "Kiewit Pacific", "West Valley Construction", r"\bFerma\b", "TEICHERT CONSTRUCTION",
                                                   "Alliance Roofing", "Northern Underground Construction", "Albanese", "Vance Brown", "William ONeill Lath and Plastering",
                                                   "El Camino Paving"]]

    signatories_UCON = [['Construction'], ["Yerba Buena Engineering", "WoodruffSawyer", "WMA Landscape Construction", "William A Guthridge", "Whiteside Concrete Construction", "Westside Underground Pipe", "Westland Contractors",
                                           "Western Traffic Supply", "Western States Tool", "Western Stabilization", "West Valley Construction", "West Coast Sand and Gravel", "Wayne E Swisher Cement Contractor", "Walter C Smith", "Walsh Construction",
                                           "Waller", "W R Forde Associates", "W C Maloney", "W Bradley Electric", "W Contracting", "Vulcan Materials", "Vulcan Construction and Maintenance", "Volvo Construction Equipment", "Vintage Paving",
                                           "Viking Drillers", "Viking Construction", "Veteran Pipeline Construction", "Varela", "Vanguard Construction", "Valverde Construction", r"\bValentine\b", "United Rentals", "Underwater Resources", "Underground Construction",
                                           "Trunxai Construction", "Troutman Sanders", "TriWest Tractor", "TriValley Excavating", "Trinet Construction", "Trench Shoring", "Trench Plate Rental", "Trench and Traffic Supply", "Traffic Management", "Tracy Grading and Paving",
                                           "TPR Traffic Solutions", "Total Traffic Control", "Tony Skulick", "Tom McCready", "Thomas Lum", "The Hartford", "The Guarantee Company of North America", "The Construction Zone", "TerraCon Constructors", "Tennyson Electric",
                                           "Teichert Waterworks", "Teichert Utilities", "Teichert Solar", "Teichert Pipelines", "Teichert", "Team Ghilotti", "TBC Safety", "Talus Construction", "Taber Construction", "TDW Construction", "T and S Construction", "Syar Industries",
                                           "Sweeney Mason", "Super Seal and Stripe", "Sunbelt Rentals", "Sukut Construction", "Suarez and Munoz Construction", "Sturgeon Electric California", "Striping Graphics", "Stormwater Specialists", "Storm Water Inspection and Maint Svcs", r"\bSWIMS\b",
                                           r"\bStomper\b", "Stoloski and Gonzalez", "Stevenson Supply", "Stevens Creek Quarry", "Steve P Rados", "Steelhead Constructors", "Stacy and Witbeck", "St Francis Electric", "SPSG Partners", "Sposeto Engineering", "SpenCon Construction",
                                           "Sonsray Machinery", "SMTD Law", "Smith Denison Construction", "Smith Currie and Hancock", "SITECH NorCal", "Sinclair General Engineering Construction", "Silverado Contractors", "Silvas Pipeline", "Sierra Traffic Markings",
                                           "Sierra Mountain Construction", "Shimmick Construction", "Sherry Montoya", "Shaw Pipeline", "Sharon Alberts", "Seyfarth Shaw", "Serafix Engineering Contractors", "Security Shoring and Steel Plate", "Security Paving",
                                           "Schembri Construction", "SANDIS Civil Engineers Surveyors Planners", "Sanco Pipelines", "S and S Trucking", "Ryan Engineering", "Rutan and Tucker", "Rupert Construction Supply", "Royal Electric", "Rosie Garcia", "Rosendin Electric",
                                           "Rogers Joseph ODonnell", "Robust Network Solutions", "Robert Burns Construction", "Robert A Bothman Construction", "Roadway Construction", "Road Machinery", "RNR Construction", "Rinker MaterialsConcrete Pipe Division", "RGW Equipment Sales",
                                           "RGW Construction", "Revel Environmental Manufacturing", r"\bREM\b", "Reliable Trucking", "Reed and Graham", "Redgwick Construction", "Rebel Equipment Enterprises", "RDOVermeer", "RDO Integrated Controls", "RCI General Engineering",
                                           "RC Underground", "Rays Electric", r"\bRansome\b", "Ranger Pipelines", "Ramos Oil", "RAM Rick Albert Machinery", "Rain for Rent", "Rafael De La Cruz", r"\bRM Harris\b", "RJ Gordon Construction", "RC Fischer",
                                           "RA Nemetz Construction", "R E Maher", "RandS Construction Management", r"\bRandB\b", "R and W Concrete Contractors", "R and R Maher Construction", "R and B Equipment", r"\bQLM\b", "Proven Management", "Preston Pipelines",
                                           "Prestige Printing and Graphics", "Precision Engineering", "Precision Drilling", "Power One", "Power Engineering Construction", "Poms Landscaping", "PMK Contractors", "Platinum Pipeline", "PJs Rebar", "PIRTEK San Leandro",
                                           "Petrinovich Pugh", "Peterson Trucks", "Peterson Cat", "Peter Almlie", "Performance Equipment", "Penhall", "Pedro Martinez", "Pavement Recycling Systems", "Paul V Simpson", "Pape Machinery",
                                           "Pacific International Construction", "Pacific Infrastructure Const", "Pacific Highway Rentals", "Pacific Excavation", "Pacific Coast General Engineering", "Pacific Coast Drilling", "Pacific Boring", "PACE Supply",
                                           "P C and N Construction", "P and F Distributors", "Outcast Engineering", "Org Metrics", "OnSite Health and Safety", "Oldcastle Precast", "Oldcastle Enclosure Solutions", "OGrady Paving", "Odyssey Environmental Services",
                                           "Oak Grove Construction", "OC Jones and Sons", "Northwest Pipe", "NorCal Concrete", "Nor Cal Pipeline Services", "NixonEgli Equipment", "Nevada Cement", "Neary Landscape", "Navajo Pipelines",
                                           "National Trench Safety", "National Casting", "Nada Pacific", "Mozingo Construction", "Mountain F Enterprises", "Mountain Cascade", "Moss Adams", "Moreno Trenching", "Mobile Barriers MBT1", "MK Pipelines",
                                           "MJG Constructors", "Mitchell Engineering", "Mission Constructors", "Mission Clay Products", "MinervaGraniterock", "Minerva Construction", "Mike Brown Electric", "Midstate Barrier", r"\bMichels\b", "McSherry and Hudson",
                                           "MCK Services", "McInerney and Dillon PC", "McGuire and Hester", "Martin General Engineering", "Martin Brothers Construction", "Marques Pipeline", "Marinship Development Interest", "Marina Landscape", "Malcolm International",
                                           "Main Street Underground", "Maggiora and Ghilotti", "MF Maher", "Hernandez Engineering", "M Squared Construction", "M and M Foundation and Drilling", "Luminart Concrete", "Lorang Brothers Construction", "Long Electric",
                                           "Lone Star Landscape", "Liffey Electric", "Liberty Contractors", "Leonidou and Rosin Professional", "Lehigh Hanson", "LeClairRyan", "Last and Faoro", "Las Vegas Paving", "Landavazo Bros", "Labor Services", "Knife River Construction",
                                           "Kerex Engineering", "Kelly Lynch", "KDW Construction", "Karen Wonnenberg", "KJ Woods Construction", r"\bJS Cole\b", "Joseph J Albanese", "Jon Moreno", "Johnston, Gremaux and Rossi", "John S Shelton", "Joe Sostaric",
                                           "Joe Gannon", "JMB Construction", "JLM Management Consultants", "Jimni Rentals", "Jifco", "Jensen Precast", "Jensen Landscape Contractor", "Jeff Peel", "JDB and Sons Construction", "JCC", "James J Viso Engineering", "JAM Services",
                                           "JM Turner Engineering", "JJR Construction", "J Mack Enterprises", "J Flores Construction", "J D Partners Concrete", "J and M", "IronPlanet", "Interstate Grading and Paving", "Interstate Concrete Pumping",
                                           "Integro Insurance Brokers", "Innovate Concrete", "Inner City Demolition", "Industrial Plant Reclamation", "Independent Structures", "ICONIX Waterworks", r"\bHoseley\b", "Horizon Construction", "Hess Construction",
                                           "Harty Pipelines", "Harris Blade Rental", "Half Moon Bay Grading and Paving", "HandE Equipment Services", "Guy F Atkinson Construction", "GSL Construction", "Griffin Soil Group", "Graniterock", "Granite Construction",
                                           "Gordon N Ball", "Goodfellow Bros", "Gonsalves and Santucci", "The Conco Companies", "Golden Gate Constructors", "Golden Bay Construction", "Goebel Construction", "Gilbertson Draglines", "Ghilotti Construction", "Ghilotti Bros",
                                           "GECMS/McGuire and Hester JV", "Garney Pacific", "Gallagher and Burk", "G Peterson Consulting Group", "Fox Loomis", "Forterra", "Ford Construction", "Fontenoy Engineering", "Florez Paving", "Flatiron West", "Fisher Phillips",
                                           "Fine Line Sawing and Drilling", "Fermin Sierra Construction", r"\bFerma\b", "Ferguson Welding Service", "Ferguson Waterworks", "Farwest Safety", "Evans Brothers", "Esquivel Grading and Paving", "Enterprise Fleet Management",
                                           "Eighteen Trucking", "Economy Trucking", "Eagle Rock Industries", "EE Gilbert Construction", "Dynamic Office and Accounting Solutions", "Dutch Contracting", "Duran Construction Group", "Duran and Venables", "Druml Group",
                                           "Drill Tech Drilling and Shoring", "Doyles Work", "Downey Brand", "Dorfman Construction", "DMZ Transit", "DMZ Builders", "DLine Constructors", "Dixon Marine Services", "Ditch Witch West", "Disney Construction",
                                           "DirtMarket", "DHE Concrete Equipment", "DeSilva Gates Construction", "Demo Masters", "Dees Burke Engineering Constructors", "Debbie Ferrari", "De Haro Ramirez Group", "DDM Underground", "D'Arcy and Harty Construction",
                                           "DW Young Construction", "DP Nicoli", "DA Wood Construction", "D and D Pipelines", "Cushman and Wakefield", "Cratus Inc", "County Asphalt", "Corrpro Companies", "Corix Water Products", "Core and Main LP", "Cooper Engineering",
                                           "Contractor Compliance", "Construction Testing Services", "ConQuest Contractors", "CondonJohnson and Associates", "Concrete Demo Works", "ConcoWest", "Compass Engineering Contractors", "Command Alkon", "Columbia Electric",
                                           "CMD Construction Market Data", "CMC Construction", "Clipper International Equipment", "Champion Contractors", "Central Striping Service", "Central Concrete Supply", "Centerline Striping", "Carpenter Rigging", r"\bCarone\b",
                                           r"\bCampanella\b", "CalSierra Pipe", "California Trenchless", "California Portland Cement", "California Engineering Contractors", "Cal State Constructors", "Cal Safety", "CF Archibald Paving", "CandN Reinforcing", "Burnham Brown",
                                           "Bugler Construction", "Bruce Yoder", "Bruce Carone Grading and Paving", "Brosamer and Wall", "BrightView Landscape Development", "Bridgeway Civil Constructors", "Brianne Conroy", "Brian Neary", "Brendan Coyne", r"\bBolton\b", r"\bBob Heal\b",
                                           "BlueLine Rental", "Blue Iron Foundations and Shoring", "Blaisdell Construction", "Bill Crotinger", "Bertco", "Berkeley Cement", "Bentancourt Bros Construction", "Beliveau Engineering Contractors", "Bear Electrical Solutions",
                                           "Bayside Stripe and Seal", "Bay Pacific Pipeline", "Bay Line Cutting and Coring", "Bay Cities Paving and Grading", "Bay Area Traffic Solutions", "Bay Area Concretes", "Bay Area Barricade Service", "Bay Area Backhoes",
                                           "Bauman Landscape and Construction", "Badger Daylighting", "B and C Asphalt Grinding", "Azul Works", "AWSI", "AVAR Construction", "Atlas Peak Construction", "Atkinson", "Argonaut Constructors", "Argent Materials",
                                           "Arcadia Graphix and Signs", "Appian Engineering", "Apex Rents", "APB General Engineering", "Aon Construction Services Group", "Anvil Builders", r"\bAnrak\b", "Andreini Brothers", r"\bAndreini\b", "Andes Construction",
                                           "AMPCO North", "American Pavement Systems", "Alex Moody", "AJW Construction", "Advanced Stormwater Protection", "Advanced Drainage Systems", "Adrian Martin", "A and B Construction"]]

    signatories_CEA = [['Construction'], ["Alcal Specialty Contracting", "Alten Construction", r"\bOveraa\b", "Cahill Contractors", "Clark Construction", "Clark Pacific", "Dolan Concrete Construction", "Dome Construction", "DPR Construction",
                                          "Gonsalves and Stronck Construction", "Hathaway Dinwiddie Construction", "Howard Verrinder", "Obayashi", "Lathrop Construction", "McCarthy Building", "Nibbi Bros Associates", "Peck and Hiller", "Roebbelen Contracting",
                                          "Roy Van Pelt", "Rudolph and Sletten", "SJ Amoroso Construction", "Skanska", "Suffolk Construction", "Swinerton Builders", "Thompson Builders", "Webcor Builders", "XL Construction", "Rosendin Electric", "Boss Electric",
                                          "Cupertino Electric", 'Beltramo Electric', 'The Best Electrical', 'CH Reynolds Electric', 'Cal Coast Telecom', 'Comtel Systems Technology', 'Cupertino Electric', 'CSI Electrical Contractors',
                                          'Delgado Electric', 'Elcor Electric', 'Friel Energy Solutions', 'ICS Integrated Comm Systems', 'Intrepid Electronic Systems', 'MDE Electric', 'MidState Electric', 'Pacific Ridge Electric',
                                          'Pfeiffer Electric', 'Radiant Electric', 'Ray Scheidts Electric', 'Redwood Electric Group', 'Rosendin Electric', 'Sanpri Electric', 'Sasco Electric', 'Selectric Services', 'San Jose Signal Electric',
                                          'Silver Creek Electric', 'Splicing Terminating and Testing', 'Sprig Electric', 'TDN Electric', 'TL Electric', r'\bTherma\b', 'Thermal Mechanical', 'Don Wade Electric', 'ABCO Mechanical Contractors',
                                          'ACCO Engineered Systems', 'Air Conditioning Solutions', 'Air Systems Service and Construction', 'Air Systems', 'Airco Mechanical', 'Allied Heating and AC', 'Alpine Mechanial Service', 'Amores Plumbing',
                                          'Anderson Rowe and Buckley', 'Applied Process Cooling', 'Arc Perfect Solutions', 'Axis Mechanicals', 'Ayoob and Peery', 'Ayoob Mechanical', 'Bacon Plumbing', 'Bay City Mechanical', 'Bayline Mechancial',
                                          'Bell Products', 'Bellanti Plumbing', 'Booth Frank', 'Brady Air Conditioning', 'Broadway Mechanical Contractors', 'Brothers Energy', 'Cal Air', 'Cal Pacific Plumbing Systems', r'\bCARRIER\b',
                                          'City Mechanical', 'CNS Mechanical', 'Cold Room Solutions', 'Comfort Dynamics', 'Commerical Refrigeration Specialist', 'Cool Breeze Refrigeration', 'Critchfield Mechanical', 'Daikin Applied', 'Daniel Larratt Plumbing',
                                          'Desert Mechanical', 'Done Rite Plumbing', 'Dowdle Andrew and Sons', r'\bDPW\b', 'Egan Plumbing', 'Emcor Services', 'Mesa Energy', 'Envise', 'Estes Refrigeration', 'Green Again Landscaping and Concrete', 'Hickey W L Sons',
                                          'Johnson Controls M54', 'KDS Plumbing', 'KEP Plumbing', 'Key Refrigeration', 'Kinectics Mechanical Services', 'KMC Plumbing', 'KOH Mechanical Contractors', r'\bKruse L J\b', 'Larratt Bros Plumbing',
                                          'Lawson Mechanical Contractors', r'\bLescure\b', 'LiquiDyn', 'Marelich Mechanical', 'Masterson Enterprises', 'Matrix HG', 'McPhails Propane Installation', r'\bMcPhails\b', r'\bMitchell E\b', 'Monterey Mechanical',
                                          'MSR Mechanical', 'Murray Plumbing and Heating', 'OC McDonald', 'OBrien Mechanical', 'OMNITemp Refrigeration', 'Pacific Coast Trane', 'PanPacific Mechanical', 'Peterson Mechanical',
                                          r'\bPMI\b', 'POMI Mechanical', 'Pribuss Engineering', 'Quest Mechanical', 'RG Plumbing', 'Redstone Plumbing', 'Refrigeration Solutions', 'Reichel', 'C R Engineering', 'Rigney Plumbing',
                                          'Rountree Plumbing and Heating', 'S and R Mechanical', 'Schram Construction', 'Southland Industries', 'Spencer F W and Sons', 'Temper Insulation', 'Therma', 'Thermal Mechanical', 'United Mechanical',
                                          'Valente A and Sons', 'Westates Mechanical', 'Western Allied Mechanical', 'White Water Plumbing', 'Blues roofing']]

    SIGNATORIES = [['All_SIGNATORIES'], hospital_signatories,
                   signatories_CEA, signatories_UCON, construction_signatories]

    return SIGNATORIES


def InferSignatoriesFromNameAndAddressFlag(df, signatory_name_list, signatory_address_list):

    if "Signatory" not in df.columns:
        df["Signatory"] = 0

    pattern_signatory_name = '|'.join(signatory_name_list)
    pattern_signatory_add = '|'.join(signatory_address_list)

    foundIt_sig = (
        ((df['legal_nm'].str.contains(pattern_signatory_name, na=False, flags=re.IGNORECASE, regex=True)) &
            (df['street_addr'].str.contains(
                pattern_signatory_add, na=False, flags=re.IGNORECASE, regex=True))
         ) |
        ((df['trade_nm'].str.contains(pattern_signatory_name, na=False, flags=re.IGNORECASE, regex=True)) &
         (df['street_addr'].str.contains(
             pattern_signatory_add, na=False, flags=re.IGNORECASE, regex=True))
         )
    )
    df.loc[foundIt_sig,"Signatory"] = 1

    return df


def InferSignatoriesFromAddressAndFlag(df, signatory_address_list):

    if "Signatory" not in df.columns:
        df["Signatory"] = 0

    pattern_signatory = '|'.join(signatory_address_list)
    foundIt_sig = (
        (df['street_addr'].str.contains(
            pattern_signatory, na=False, flags=re.IGNORECASE, regex=True))
        #(df['street_addr'].str.match(pattern_signatory, na=False, flags=re.IGNORECASE) )
    )
    df.loc[foundIt_sig, "Signatory"] = 1

    return df


def InferSignatoriesFromNameAndFlag(df, SIGNATORY_INDUSTRY):

    if 'legal_nm' not in df.columns:
        df['legal_nm'] = ""

    if 'trade_nm' not in df.columns:
        df['trade_nm'] = ""
    
    if "Signatory" not in df.columns:
        df["Signatory"] = 0

    df['legal_nm'] = df['legal_nm'].astype(str)
    df['trade_nm'] = df['trade_nm'].astype(str)

    for x in range(1, len(SIGNATORY_INDUSTRY)):
        PATTERN_EXCLUDE = SIG_EXCLUSION_LIST_GENERATOR(
            SIGNATORY_INDUSTRY[x][1])
        
        PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

        foundIt_sig = (
            (
                df['legal_nm'].str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                ~df['legal_nm'].str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)) |
            (
                df['trade_nm'].str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                ~df['trade_nm'].str.contains(PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True))
        )
        df.loc[foundIt_sig, "Signatory"] = 1

    return df


def infer_prevailing_wage_cases(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals):
    df = InferPrevailingWageAndColumnFlag(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals)
    return df


def InferPrevailingWageAndColumnFlag(df, prevailing_wage_terms, prevailing_wage_labor_code, prevailing_wage_politicals):

    if 'Prevailing' not in df.columns:
        df['Prevailing'] = '0'
    else:
        df['Prevailing'] = df.Prevailing.fillna("0")
    
    if "Reason For Closing" not in df.columns:
        df["Reason For Closing"] = ""
    if 'Closure Disposition - Other Reason' not in df.columns:
        df['Closure Disposition - Other Reason'] = ""
    if 'violation_code' not in df.columns:
        df['violation_code'] = ""
    if 'violation' not in df.columns:
        df['violation'] = ""
    if 'Note' not in df.columns:
        df['Note'] = ""
    

    prevailing_wage_pattern = '|'.join(prevailing_wage_terms)
    found_prevailing_0 = (
        ((df['Reason For Closing'].astype(str).str.contains(prevailing_wage_pattern, case = False))) |
        ((df['Closure Disposition - Other Reason'].astype(str).str.contains(prevailing_wage_pattern, case = False)))
    )

    prevailing_wage_labor_code_pattern = '|'.join(prevailing_wage_labor_code)
    found_prevailing_1 = (
        ((df['violation_code'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) |
        ((df['violation'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) |
        ((df['Note'].astype(str).str.contains(prevailing_wage_labor_code_pattern, case = False))) 
    )

    prevailing_wage_political_pattern = '|'.join(prevailing_wage_politicals)
    found_prevailing_2 = (
        ((df['legal_nm'].astype(str).str.contains(prevailing_wage_political_pattern, case = False)))
    )

    df.loc[((found_prevailing_0 | found_prevailing_1 | found_prevailing_2) & 
        ((df['industry'] == "Construction") | (df['industry'] == 'Utilities') )), 
        'Prevailing'] = '1'

    #specific to DOL WHD data
    if "dbra_cl_violtn_cnt" in df.columns:
        df.loc[df["dbra_cl_violtn_cnt"] > 0, "violation_code"] = "DBRA"
        df.loc[df["dbra_cl_violtn_cnt"] > 0, "Prevailing"] = "1"

    return df


def InferSignatoryIndustryAndLabel(df, SIGNATORY_INDUSTRY):
    if 'signatory_industry' not in df.columns:
        df['signatory_industry'] = ""

    if not df.empty and 'legal_nm' and 'trade_nm' in df.columns:  # filter out industry rows
        for x in range(1, len(SIGNATORY_INDUSTRY)):

            PATTERN_EXCLUDE = SIG_EXCLUSION_LIST_GENERATOR(
                SIGNATORY_INDUSTRY[x][1])
            PATTERN_IND = '|'.join(SIGNATORY_INDUSTRY[x][1])

            foundIt_ind1 = (
                (
                    df['legal_nm'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['legal_nm'].astype(str).str.contains(
                        PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
                ) |
                (
                    df['trade_nm'].astype(str).str.contains(PATTERN_IND, na=False, flags=re.IGNORECASE, regex=True) &
                    ~df['trade_nm'].astype(str).str.contains(
                        PATTERN_EXCLUDE, na=False, flags=re.IGNORECASE, regex=True)
                ) |
                (df['Signatory'] == 1) &
                (df['industry'] == SIGNATORY_INDUSTRY[x][0][0])
            )
            df.loc[foundIt_ind1, 'signatory_industry'] = SIGNATORY_INDUSTRY[x][0][0]

        # if all fails, assign 'other' so it gets counted
        df['signatory_industry'] = df['signatory_industry'].replace(
            r'^\s*$', 'Undefined', regex=True)
        df['signatory_industry'] = df['signatory_industry'].fillna('Undefined')
        df['signatory_industry'] = df['signatory_industry'].replace(
            np.nan, 'Undefined')

    return df


def filter_function_organization(df, TARGET_ORGANIZATIONS):


    if (TARGET_ORGANIZATIONS[1] != ""): 
        df = Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS)  #<--- BUGGY HERE 1/12/2023 2x records -- hacky fix w/ dup removal


    return df


def Filter_for_Target_Organization(df, TARGET_ORGANIZATIONS):
    organization_list = ''.join(TARGET_ORGANIZATIONS[1]).split('|')

    df_temp_0 = df.loc[df['legal_nm'].astype(str).str.contains(
        '|'.join(organization_list), case=False, na=False) ] #na=False https://stackoverflow.com/questions/66536221/getting-cannot-mask-with-non-boolean-array-containing-na-nan-values-but-the
    df_temp_1 = df.loc[df['trade_nm'].astype(str).str.contains(
        '|'.join(organization_list), case=False, na=False) ]

    df_temp = pd.concat([df_temp_0, df_temp_1], ignore_index=True)
    df_temp = df_temp.drop_duplicates()

    return df_temp