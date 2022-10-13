# ^ notation means the term must prefix the string
# (...) captures substring like A(pple) https://unbounded.systems/blog/3-kinds-of-parentheses-are-you-a-regex-master/
# \b matches word boundaries between word and non-word https://www.regular-expressions.info/wordboundaries.html
# r' always write pattern strings with the 'r' just as a habit https://developers.google.com/edu/python/regular-expressions
# . matches any single character except newline '\n' https://developers.google.com/edu/python/regular-expressions
# * matches preceding char repeatedly, with .* adds any number of additional chars
#! customized by infer industry to exclude term, for example '(!Market)'

agriculture_terms = ['Agriculture', r'Agricult.*', r'Forest.*', "^(111)", "^(1112)", "^(113)", "^(114)", "^(115)", 'Fishing', 'Hunting$',
                     r'farm.*', 'orchard', 'vineyard', 'mushroom', 'garlic', 'nursery', 'cattle', r'\branch.*', 'dairy',
                     r'horse.*', 'equine', 'timber', 'logging', r'harvest.*', 'pumpkin', 'livestock', 'Guglielmo', 'Garrod Farms', 'Tilton Ranch',
                     'Del Fresh Produce', 'Chiala Farm', 'Nature Quality', 'Christopher Ranch', 'Syngenta', 'Sakata Seeds', 'Headstart Nursery',
                     'Boething Treeland', 'B & T Farms', 'B and T Farms', 'Mission Ranches', 'Kirigin Cellars', 'Uesugi Farms', 'Matt Bissell Forestry',
                     'Big Oak Ranch', 'NURSERIES', 'HORTICULTURE', r'SANDRIDGE.*PARTNERS', '(!Market)', '(!Store)']

mining_terms = ['Mining', 'Mining', "^(211)", "^(212)", "^(213)", 'Quarrying', r'\bOil\b', r'\bGas\b', 'PETROLEUM', 'Extraction',
                'quarry', r'\bmine\b']

utilities_terms = ['Utilities', r'utilit.*', 'Electric', 'Power', 'Generation', 'Hydroelectric', 'Nuclear', 'Geothermal', 'Solar',
                   r'\bWind\b', 'Biomass', 'Natural Gas', 'Sewer', 'Water supply', "^(221)", '(!MANPOWER)']

construction_terms = ['Construction', '^(23)', r'plumb.*', r'construct.*', 'MANPOWER', 'build', 'floor', r'landscap.*',
                      'mechanical', 'elevator', 'plaster', 'frame', 'concrete', 'roof', 'glass', 'tile', r'dry.*wall', 'painting', 'painter', 'remodel', 'cabinet',
                      'carpet', 'drafting', 'carpentry', 'AIR SYSTEMS', r'sheet.*metal', 'pipe', 'HOMES', 'asbestos', r'custom.*home', 'maintenance', 'window', r'tree.*trim', 'iron', 'heating',
                      r'air.*condition', 'instal', 'insulation', 'MOUNTAIN AIR', 'contract', 'brick', 'paving', 'sprinkler', 'improvement', 'renovat', 'energy',
                      'mason', 'marble', 'cooling', '161000', '162000', "^(23)", '230000', "^(3323)", "^(3334)", '333415', '337110', '337212', '339950',
                      '423320', '532410', '541350', '561730', '561790', '562910', '623200', r'tree.*service', 'Excavating', 'Restoration', 'MOULDING',
                      'electric', 'labor', 'ASPHALT', 'CEMENT', 'FENCING', r'HOME.*REPAIR', r'WOOD.*WORKING', 'ROOTER', r'\bHVAC\b', '^(54132)', r"(!Farm\b)",
                      r"(!.*CARE.HOME.*)",
                      r"(!RETIREMENT)", r"(!.*GROUP.HOME.*)", r"(!.*RESIDENTIAL.*CARE.*)",
                      r"(!CATERING)", r"(!AUTO.*BODY)", r"(!FOOD.*SERVICE)", r"(!CLEANING)", r"(!ELDERLY)"]  # 'PLUMBING',

manufacturing_terms = ['Manufacturing', r'Manufactur.*', 'machine', 'milling', 'millwork', 'Product', 'Preserving', 'Slaughter', 'Dairy',
                       'Packaging', 'Beverage', 'Textile', 'Apparel', 'Printing', 'Petroleum', 'WELDING', 'Pesticide', 'Forging', 'Industrial', 'Metalworking',
                       'Machinery', 'Semiconductor', 'Electromedical', 'CIRCUITS', 'Appliance', 'ARMATURE', 'Component', 'APPLE', 'Aerospace', 'Furniture',
                       '^(311)', '^(312)', '^(313)', '^(314)', '^(315)', '^(316)', '^(317)', '^(318)', '^(319)', '^(320)', '^(321)', '^(322)', '^(323)',
                       '^(324)', '^(325)', '^(326)', '^(327)', '^(328)', '^(329)', '^(330)', '^(331)', '^(332)', '^(333)', '^(334)', '^(335)', '^(336)',
                       '^(337)', '^(338)', '^(339)', "feed", 'hay', r'HEWLETT.PACKARD', 'PLATING', 'panaderia', 'MACHINING', 'PRECISION', 'FABRICATION',
                       'WINERY', 'INDUSTRIES', 'TELEDEX', 'ASSEMBLY', 'ALLOYS', 'ESP SAFETY', 'WELDERS', r'METAL.*FINISH', '^(print)', r'PRINT.*']

trade_terms = ['Trade', '^(423)', '^(424)', '^(425)', 'trading', 'trader', 'Wholesale', 'Broker', 'merchant', 'lumber', r'material.*',
               'plywood', r'millwork.*', r'home.depo', 'lowes', 'DISTRIBUTION', 'DISTRIBUTOR']

retail_terms = ['Retail', '^(44)', '^(45)', "(store)", 'SPORTSWEAR', "DOLLAR STORE", 'Paints', 'PLATOS CLOSET', 'PAPAYA', 'SWAROVSKI',
                r'VAPE.*', 'Victoria Secret', 'SAVERS', 'PETCO', r'QUICK.STOP', 'KOHLS', 'KOHLS', r'FOREVER.21', 'CASK N FLASK', 'Target', r'Smoke.*',
                r'Guitar.*', 'Ross Dress for Less', 'SUPPLIES', r'Sherwin.Williams', r'Quik.Stop', 'Mattress', 'Bevmo', r'Jewl.*', 'Dollar Tree',
                '7Eleven', 'ampm', 'music', 'audio', 'EBAY', 'HERBAL', 'tinting', "water", "(LIQUOR)", '(liqour)', 'mercado', 'palenteria', "quik stop",
                r'flower.*', 'bike shop', r'SUPERMARKET.', 'CARNICERIA', 'CHEVRON', "MARKET", 'produce', '711', '7eleven', 'seveneleven', 'gasoline',
                r'gas.*station', 'convenience', 'safeway', 'COLLECTIBLE', 'PETSMART', 'PHARMACY', 'FASHION', 'FLORIST', 'HARDWOOD', 'ELECTRONICS',
                'WALMART', 'JEWELE', 'JEWELR', 'LUXURY', 'IMPORT', r'TOYS.*R.*US', 'OUTLET', 'TROPHIES', 'CIGARETTES', 'OXYGEN', r'RITE.*AID', 'SUPPLY',
                'QUIK AND SAVE', 'VALERO', r'RUSSELLS.*FURNTIURE', 'CLOTHING', 'COSMETICS', 'GROCERY', 'BRIDAL', 'CIGARS', 'BAZAR']
# transportation
gig_terms = ['Gig', "Uber", "Lyft", "DoorDash", "Instacart", "Postmates", "TaskRabbit", "Wonolo", "Tradehounds", "Handy", "Amazon Flex",
             "Bellhops", "Care.com", "Caviar", "Closet Collective", "Crowdflower", "Dolly", "Etsy", "Fancy hands", "Favor", "Feastly", "Fiverr",
             "Freelancer", "Gigwalk", "Grubhub", "HelloTech", "HopskipDrive", "Hubstaff Talent", "Juno", "Moonlighting", "onefinestay", "openairplane",
             "peopleperhour", "prefer", "rentah", "roadie", "rover", "shipt", "snagajob", "spare5", "sparehire", "spothero", "takl", "taskeasy",
             "turo", "upwork", "VRBO", "Vacation Rentals by Owner", "Wingz", "airbnb", "yourmechanic", "zeel"]
transportation_terms = ['Transportation', '^(48)', '^(49)', r'Transport.*', 'TRANSPORATION', 'railroad', 'air transportation', 'trucking', 'freight',
                        'transit', 'taxi', 'limousine', 'charter', 'pipeline', 'MOVING', 'air traffic', 'packing', 'postal service', 'courier', 'delivery', 'warehouse',
                        'storage', 'TOWING', 'MOVERS', 'DRIVERS', 'DELIVERIES', 'AMAZON', 'SHUTTLE', 'VAN LINES', 'ROADWAYS', 'YELLOW CAB', 'HAULING', 'PACKAGE',
                        'FEDEX', 'ROYAL COACH', 'AMBULANCE', r'RENT.*CAR', r'car.*rent', r'\bCAB\b'] + gig_terms

information_terms = ['Information', 'Information', '^(51)', 'LOGIC', 'TECHNOLO', 'newspaper', 'publisher', 'software', 'video', 'recording',
                     'radio', 'broadcasting', 'programming', 'wireless', 'MOBILE', 'SYSTEMS', 'COMMUNICATIONS', 'ATANDT', 'telecommunication', 'NETWORK',
                     'SILICON', 'satellite', 'data', 'MOBILITY', 'NETFLIX', 'processing', 'hosting', 'library', 'archive', 'internet', 'PUBLISH', r'\bNEWS\b',
                     'TRONIC', 'ZAGACE', 'MAGAZINE', 'TELECOM', 'CELLULAR', 'INTEGRATED']

finance_terms = ['Finance', 'Financ', '^(52)', "^(429)", "^(522)", "^(521)", 'capital', r'asset.*management', 'banking', r"Wells.Fargo",
                 r"(Chase.Bank)", r"(JP.Morgan)", 'CITIBANK', r'\bBank\b', r"Credit.*union", r"Credit.*card", 'insurance', r'saving.*',
                 'securities', r'^MED.*PRO$', 'investment', r'stock.*broker', r'DISCOUNT.*TAX', r'\btaxes\b', 'adjusting', r'CREDIT.*SOLUTION', 'brokerage', 'fund',
                 'MORTGAGE', 'PAYPAL', 'welfare', 'LENDING', 'HOLDINGS', 'REFINANCING', r'ASSETS.*GROUP', 'ESTATE OF']

real_estate_terms = ['Real_estate', r'real.*estate', '^(53)', 'lease', 'residential', r'commercial.*property', r'property.*management',
                     'appraiser', 'rental', 'renting', r'invest.*property', 'PROPERTIES', r'income.*property', 'PROPERTY', 'APARTMENTS', r'\bland\b',
                     'PROMOTE ROI', 'SUMMER HILL TERRACE']

professional_terms = ['Professional', 'Professional', '^(54)', '^(5413)', '^(5419)', 'legal', 'lawyer', 'accounting', r'book.*keeping', 'payroll',
                      'architect', 'architecture', 'engineering', 'drafting', 'inspection', 'surveying', 'laboratories', 'design', 'graphic', 'interior',
                      'consulting', 'marketing', 'PROMOTIONS', 'environmental', 'CIVIL ENGINEER', 'ASSOCIATES', 'EVENT PRO', 'DOCUMENT', 'IMAGING', 'LAW OFFICE', 'research', 'VIRTUAL', 'APPRAISAL', 'development', 'advertising', r'public.*relations', 'media', 'scientific',
                      'photography', 'science', 'net optics', 'COMPUTER', 'LABS', 'interpreter', 'DIAGNOSTICS', 'TRUSTEE', 'PHOTO', 'STUDIOS', 'ACCOUNTAN',
                      'N2N SECURE', r'LAW.*GROUP', 'INTRANET', 'PRINTERS', r'HOME.*OWNER.*ASSOC', r'\bHOA\b', 'CIRRUS SOLUTIONS', r'BLUE.*CHIP .*TEK', 'AUTOCHLOR SYSTEM',
                      r'TECH.*SHOP', 'ANALYTICAL', 'LOCAL UNION', 'LAW FIRM', 'COUNSELING', 'GEOTECHNICAL', 'CONSERVATOR', r'BAIL.*BOND', 'FAMILY TRUST', 'EMBROIDERY']

management_terms = ['Management', '^(55)', '^(56)', 'Management',
                    'ENTERPRISES', 'LOGISTICS', 'SLINGSHOT CONNECTIONS']

janitorial_terms = ['Janitorial', r'Janitor.*', r'month.*clean', r'rental.*clean', r'week.*clean', r'condo.*clean',
                    r'clean.*vacanc', r'airbnb.*clean', r'move.*in.*clean', r'resident.*clean', r'post.*construct.*clean', r'apartment.*clean',
                    r'profession.*clean', r'move.*out.*clean', r'clean.*lady', 'green clean', r'commercial.*clean', r'home.*clean', r'clean.*company',
                    r'deep.*clean', r'maid.*', r'spring.*clean', r'maid.*service', r'window.*clean', r'house.*clean', r'clean.*service.*', r'house.*keep',
                    r'carpet.*clean', '561720', '5617', 'MAINTENENCE']

waste_terms = ['Waste', 'Waste', '^(5621)', '^(5621)', '^(5629)', 'WRECKERS', 'RECYCLING', r'solid.*waste', 'hazardous', 'remediation', 'landfill',
               'incinerator', 'septic', 'DISMANTLER', 'DISPOSAL']

administrative_terms = ['Administrative', r'Administrat.*', '^(56)']

educational_terms = ['Educational', r'educat.*', '^(61)', 'school', 'university', 'college', 'training', 'instruction', 'lecture',
                     'learning', r'tutor.*', 'CULINARY', '(!MEDICINE)']

residential_carehome_terms = ["Residential_carehome", r"resident.*care.*home", r"assist.*living", r"board.*care.*home",
                              r"senior.*care.*home", r"resident.*home.*care", r"nursing.*home", r"memory.*care", r"home.*senior", r"care.*senior",
                              r"senior.*care", r"sunrise.*manor", r"elderly.*care.*facility", r"care.*center", r"senior.*care", r"elderly.*care",
                              r"garden.*villa", r"home.*elderly", "residence", "manor", r'\bvilla\b', "haven", r"resident.*care", r"home.*care", r'ADULT.*DAY.*PROGRAM',
                              r"board.*and.*care", r"residential.*facility", "^(6241)", "^(5170)", "^(6232)", "^(6239)", "^(6233)", "^(6216)", "^(6242)",
                              'CLOVERLEAF CARE']

carehome_terms = ['Carehome', r'care.*home', r'.*care.*home.*', r'\bRCFE\b', r'\bChild\b', 'RETIREMENT', 'Convalescence', r'Adult.*Resident.*Care',
                  r'Group.*Home', r'Crisis.*Nurseries', r'Short.*Term.*Residential', 'Therapeutic', 'Crisis', r'child.*care', 'preschool', r'\byouth\b',
                  'adolescent', 'transitional', 'lodge', 'maternity', 'cottage', 'recovery', 'early', 'Montessori', 'kindercare',
                  r'develop.*disabl.*', r'disablit.*', r'special.*need', 'academy', r'\bYMCA\b', r"\bresiden.*", r'DAY.*CARE', r'DAY.*CENTER',
                  'crisis', 'Elwyn', 'treatment', r'total.care', r'skilled.*nursing', r'house.*home.*health', r'adult.*care.*facility',
                  'RCFAs', 'RCFEs', '^(RCFA)', '^(RCFE)', '^(RFCE)', '^(SNF)$']

healthcare_terms = ['Health_care', '^(62)', r'Health.*care', r'\bDMD\b', r'\bMD\b', r'\bDDS\b', 'AMBULATORY', 'GENETICS', 'HOSPITAL',
                    'DENTAL', 'MEDICINE', 'medical', 'health', 'doctor', 'SURGERY', 'KAISER PERMANENTE', 'OPTOMETRI', 'MEDICAL CENTER', 'PHARMA', 'ANKLE',
                    'BEHAVIORAL', 'PEDIATRICS', 'ALLERGY', 'ASTHMA', 'DENTIST', 'CHIROPRACTIC', 'ACUPUNCTURE', 'REHABILITATION', 'OPTOMETRY',
                    'LIFES CONNECTIONS', 'PROSTHETICS', 'ORTHOSYNETICS', 'CANCER', r'SUBACUTE.*CARE', r'URGENT.*CARE', 'ACCUPUNCTURE ', '(!EDUCATION)']

entertainment_terms = ['Entertainment', '^(71)', 'performing', 'Billiard', 'theater', 'dance', 'musical', 'artist', r'sport.*', 'team',
                       'racetrack', 'athlete', 'entertainer', 'writer', 'museum', 'historical', 'zoo', 'botanical', 'BADMINTON', 'nature park', 'amusement', 'gambling',
                       'golf', 'casino', 'marina', 'bowling', r'PERFORMING ART.', r'SOCCER.*LEAGUE', 'SOCCER', 'LEAGUE', 'BASEBALL', 'football', 'ATHLETIC',
                       r'MARTIAL.*ART', 'HOOKAH', 'GREAT AMERICA', '(!bar)', '(!SPORTSWEAR)', '(!Transport)']

fitness_terms = ['Fitness', r'Fitness.*',
                 '^(71394)', r'gym.*', 'YOGA', r'JENNY CRAIG.', r'crossfit.*', r'physical.*', r'fit.*']

accommodation_terms = ['Accommodation', '^(72)', r'Accommodat.*', 'hotel', 'motel', 'LODGING', r'BOARD.*HOUSE', r'\binn\b', 'HILTON',
                       r'EXTENDED.*STAY']

fast_food_terms = ['Fast_food', r'fast*.food', '722513', '722514', '722515', '^(7222)', '^(72233)', '311520', '311920',
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
                   'Islands Restaurants', 'Pressed Juicery', 'Bakers Drive Thru', r'Drive.*Thru', r'Drive.*in', 'Robeks', '85 Degrees C Bakery', 'Jimboys',
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
                   'Ample Hills Creamery', r'Noodles.*Co.*', 'Marbleslab Creamery', 'Pret A Manger', "(!restaurant)", "(!deliv)", "(!DELIGHT)", "(!DELICIOUS)"]

restaurant_terms = ['Restaurant', 'Restaurant', '^(7225)$', '^(7223)$', '^(72231)', '^(72232)', '^(7224)', '722511', '311811',
                    'pastrami', 'Pizza Chicago', 'MIMOSAS', 'BUN BO HUE', 'Mediterranean', r'KABOB.', 'Curry', 'Bread', 'Mandarin', 'Gourmet', 'taqueria',
                    r"\bPho\b", 'noodle', 'ramen', 'tofu', r"^\bcom\b", 'creamery', 'pizza', 'chinese', 'italian', 'indian', 'vietnames', 'korean',
                    'sandwich', 'pancake', 'TOOTSIE', 'breakfast', 'lunch', 'dinner', 'diner', 'bistro', 'china', 'cheesecake', 'bakery', r'sport.*bar',
                    r'\bbar\b', r'\bBBQ\b', 'thai', 'grille', 'grill', r'taco.*', 'chef', '(sushi)', 'japanese', 'kitchen', 'steak', r'fish.*', 'a la carte',
                    'vietnam', 'shiki wok', 'pasta', r'sea.*food', 'cuisine', 'brewhouse', r'cater.*', 'lounge', r'FOOD.*SERVICE', 'buffet', 'barbecue', 'burrito',
                    'greek', 'mexican', 'food', 'eggroll', 'PORRIDGE', 'PIZZERIA', 'brew', 'dining', 'Lobster', 'DELICIOUS', 'produce', r'markett\b', 'bowl', 'club',
                    'beverage', 'tavern', "TERYAKI", "TERIYAKI",  "El Patron", "Boston Market", "Cocos Bakery", 'Loyal Order of Moose', 'IHOP', r'house.*pancake',
                    '152 Post', 'Dennys', 'Chalateca', 'power pot', 'wing box', 'wingbox', "Pizza", 'BARBEQUE', 'BAGUETTE', 'COOKIES', r'DENNY.*S', 'EATERY', 'CUP AND SAUCER',
                    r'LATINIGHTS', 'OMELETTE', 'BAKERIES', 'HOT WOK', r'ALE.*HOUSE', 'OLIVE GARDEN', 'CHOWDER', r'BAKE.*SHOP', 'BAKER', 'HOUSE OF CHU', 'BARTENDERS',
                    'TAQUEROS', 'ROTISSERIE', '(!SPORTS CLUB)', '(!ATHLETIC CLUB)', '(!nail lounge)', '(!SOCCER)', "(!Technology)", '(!REMODEL)', '(!HOOKAH)', r'(!\bstore\b)',
                    '(!LIQUOR.)', '(!722513)', '(!722514)', '(!722515)', '^(!7222)', '^(!72233)', '(!311520)', '(!311920)']  # 'pho',

service_terms = ['Service', '^(8112)', '^(8113)', '^(8114)', '^(8122)', '^(8123)', '^(8129)', '^(8132)', '^(8133)', '^(8134)', '^(8139)', '^(8141)',
                 r'car.*audio', 'STAFFING', 'PERSONNEL', 'Postal', 'PLACEMENT', 'SECURITY', r'OFFICE.*WORK', 'SECURE', 'PROTECTION', 'GARDENER.', 'GARDENING',
                 r'Alteration.*', r'Tailor.*', r'Cleaner.*', r'Dry.*clean', 'Linen', r'Laund.*', 'CANINE', 'PEST PRO', 'PATROL', 'INVESTIGATIONS', 'TERMITE',
                 'PEST CONTROL', 'TEKLICON', r'OFFICE.*SOLUTION', 'EZHOME', 'GROOMING', 'PET CLINIC', 'TREE SURGEON',
                 '(!FOOD)', '(!CONSTRUCTION)', '(!JANITORIAL)', '(!EDUCAT)']

automotive_terms = ['Automotive', '^(8111)', r'Automot.*', r'car.*care', r'\bwheel.', r'\btire.', r'\btire\b', r'auto.*shop', r'auto.*parts', r'Jiffy.lube',
                    r'General Auto.*', r'Auto.*Repair', r'AUTO.*SALE', 'TRANSMISSION', r'Auto.*zone', 'MOTORS', r'AUTO.*SERVICE', r'smog.*check', 'smog', 'AVIATION', r'Exhaust*.System', r'Trans.*Repair',
                    r'Mech.*Elect', r'Body.*Paint', r'Body.*Shop', r'auto.*body', 'COLLISION', r'Glass.*Replacement', r'Oil.*Change', r'Lub.*Shop.', r'GOOD*.YEAR',
                    r'CAR.*WASH', r'AUTO.*GROUP', 'VOLKSWAGEN', r'AUTO.*CARE', 'midas', r'AUTO.*WORK', r'AUTO.*GROUP', 'MOTORCYCLE', 'CHRYSLER', r'\bDODGE\b',
                    'TOYOTA', 'NISSAN', r'AUTO.*SHOWROOM', r'MOTOR.*GROUP', r'CAR.*STEREO', r'AUTO.*DETAIL', 'MERCEDES BENZ', r'AUTO.*CENTER', 'honda',
                    r'AUTO.*MART', 'PREMIER XPRESS', r'PAINT.*BODY', '(!Financial)']

personal_care_terms = ['Personal_care', r'personal.*care', '^(8121)', r"nail.*spa", r'AESTHETIC.*', r'Hair.*', r'Hair.*Cut', r'NAIL.*SALON',
                       r'HAIR.*SALON', r'NAIL.*LOUNGE', r'hair.*dresser', r'BEAUTY.*SALON', r'spa\b', 'BEAUTY', 'LIFE SPA', r'SPA.*SALON', r'SALON.*SPA',
                       'MASSAGE', 'barber', 'WELLNESS', 'salon', 'nail', 'beauty', 'BOUTIQUE', 'diet', 'SUNTAN', 'tanning', 'EYEBROW', 'REJUVENATION', r'GREAT.*CUTS']

religion_terms = ['Religion', 'Religion', '^(8131)', 'catholic', 'protestant', 'jewish', 'buddhist', 'hindu', 'muslim', 'islam',
                  'church', 'mosque', 'temple', 'synagogue', 'parish', '(!SCHOOL)', '(!college)', '(!university)', '(!academy)']

public_administration_terms = ['Public_servant', r'Public.*admin', '^(92)', r'fire depart.*', r'police.*', 'US VETERANS',
                               'UNITED STATES VETERANS', 'PUBLIC ENTITY', 'CITY OF', 'County of', 'State of', 'DEPARTMENT OF', 'SANTA CLARA COUNTY',
                               r'OHLONE.*TRIBE']

other_terms = ['Undefined', 'Undefined', 'No Result',
               'Other Type of Facility']  # catch all unlabeled items

professional_terms_rollup = ['Professional'] + service_terms + automotive_terms
restaurant_terms_rollup = ['Restaurant'] + restaurant_terms + fast_food_terms
carehome_terms_rollup = ['Carehome'] + \
    residential_carehome_terms + carehome_terms

service_terms_rollup = ['Service'] + service_terms + \
    automotive_terms + personal_care_terms + religion_terms
healthcare_terms_rollup = [
    'Health_care'] + healthcare_terms + residential_carehome_terms + carehome_terms
accommodation_terms_rollup = ['Accommodation'] + \
    accommodation_terms + restaurant_terms + fast_food_terms
entertainment_terms_rollup = ['Entertainment'] + \
    entertainment_terms + fitness_terms
administrative_terms_rollup = ['Administrative'] + \
    administrative_terms + waste_terms + janitorial_terms

ALL_NAICS_INDUSTRIES = [['All NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms,
                        trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms,
                        management_terms, administrative_terms_rollup, educational_terms, healthcare_terms_rollup, entertainment_terms_rollup,
                        accommodation_terms_rollup, service_terms_rollup, public_administration_terms, other_terms]

WTC_NAICS_INDUSTRIES = [['WTC NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms,
                        trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms,
                        management_terms, administrative_terms, waste_terms, janitorial_terms, educational_terms, healthcare_terms,
                        residential_carehome_terms, carehome_terms, entertainment_terms, fitness_terms, accommodation_terms, restaurant_terms,
                        fast_food_terms, service_terms, automotive_terms, personal_care_terms, religion_terms, public_administration_terms, other_terms]


industriesDict = {
    "Agriculture": [["Agriculture"], agriculture_terms],
    "Mining": [["Mining"], mining_terms],
    "Utilities": [["Utilities"], utilities_terms],
    "Construction": [["Construction"], construction_terms],
    "Manufacturing": [["Manufacturing"], manufacturing_terms],
    "Trade": [["Trade"], trade_terms],
    "Retail": [["Retail"], retail_terms],
    "Gig": [["Gig"], gig_terms],
    "Transportation": [["Transportation"], transportation_terms],
    "Information": [["Information"], information_terms],
    "Finance": [["Finance"], finance_terms],
    "Real Estate": [["Real_estate"], real_estate_terms],
    "Professional": [["Professional"], professional_terms],
    "Management": [["Management"], management_terms],
    "Janitorial": [["Janitorial"], janitorial_terms],
    "Waste": [["Waste"], waste_terms],
    "Administrative": [["Administrative"], administrative_terms],
    "Educational": [["Educational"], educational_terms],
    "Residential Carehome": [["Residential_carehome"], residential_carehome_terms],
    "Carehome": [["Carehome"], carehome_terms],
    "HealthCare": [["Health_care"], healthcare_terms],
    "Entertainment": [["Entertainment"], entertainment_terms],
    "Fitness": [["Fitness"], fitness_terms],
    "Accommodation": [["Accommodation"], accommodation_terms],
    "Fast Food": [["Fast_food"], fast_food_terms],
    "Restaurant": [["Restaurant"], restaurant_terms],
    "Service": [["Service"], service_terms],
    "Automotive": [["Automotive"], automotive_terms],
    "Personal Care": [["Personal_care"], personal_care_terms],
    "Religion": [["Religion"], religion_terms],
    "Public Servant": [["Public_servant"], public_administration_terms],
    ""
    "Other": [["Undefined"], other_terms],
    "All NAICS": [['All NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms,
                  trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms,
                  management_terms, administrative_terms_rollup, educational_terms, healthcare_terms_rollup, entertainment_terms_rollup,
                  accommodation_terms_rollup, service_terms_rollup, public_administration_terms, other_terms],
    "WTC NAICS": [['WTC NAICS'], agriculture_terms, mining_terms, utilities_terms, construction_terms, manufacturing_terms,
                  trade_terms, retail_terms, transportation_terms, information_terms, finance_terms, real_estate_terms, professional_terms,
                  management_terms, administrative_terms, waste_terms, janitorial_terms, educational_terms, healthcare_terms,
                  residential_carehome_terms, carehome_terms, entertainment_terms, fitness_terms, accommodation_terms, restaurant_terms,
                  fast_food_terms, service_terms, automotive_terms, personal_care_terms, religion_terms, public_administration_terms, other_terms]
}


def getIndustryNames() -> list:
    return list(industriesDict.keys())
