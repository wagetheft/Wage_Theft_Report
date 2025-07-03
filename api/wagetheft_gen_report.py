

import platform
if platform.system() == 'Windows' or platform.system() =='Darwin':
    from wagetheft_print_utils import (
        Footer_Block,
        Notes_Block,
        Methods_Block,
        Sources_Block,
        Title_Block,
        Industry_Summary_Block,
        Proportion_Summary_Block,

        write_style_html,
        
        print_table_html_by_industry_and_city,
        print_table_html_by_industry_and_zipcode,
        print_table_html_Text_Summary,
        print_top_viol_tables_html,
    )

    from util_group import (
        GroupByMultpleCases,
        GroupByMultpleAgency,
        GroupByX,
    )

    from wagetheft_clean_value_utils import (
        Clean_Summary_Values,
        DropDuplicateRecords,
    )

    from util_zipcode import (
        Filter_for_Zipcode,
    )
else:
    from api.wagetheft_print_utils import (
        Footer_Block,
        Notes_Block,
        Methods_Block,
        Sources_Block,
        Title_Block,
        Industry_Summary_Block,
        Proportion_Summary_Block,

        write_style_html,
        
        print_table_html_by_industry_and_city,
        print_table_html_by_industry_and_zipcode,
        print_table_html_Text_Summary,
        print_top_viol_tables_html,
    )

    from api.util_group import (
        GroupByMultpleCases,
        GroupByMultpleAgency,
        GroupByX,
    )

    from api.wagetheft_clean_value_utils import (
        Clean_Summary_Values,
        DropDuplicateRecords,
    )

    from api.util_zipcode import (
        Filter_for_Zipcode,
    )


def compile_theft_report(
        out_target,
        out_target_organization,
        target_dict,
        print_dict,
        sum_dict,
        debug,
        temp_file_name = 'wagetheft_report.html',
        signatories_report = False,
        ):
    
    all_unique_legalname = GroupByMultpleCases(out_target, 'legal_nm')
    all_unique_legalname = all_unique_legalname.sort_values(
        by=['records'], ascending=False) #this is never used
    
    all_agency_df = GroupByMultpleAgency(out_target)
    all_agency_df = all_agency_df.sort_values(by=['records'], ascending=False)  #this is never used
    
    # group repeat offenders************************************
    
    out_counts = out_target_organization.copy()  # hold for case counts

    unique_legalname = GroupByX(out_target_organization, 'legal_nm')
    unique_legalname_all = GroupByX(out_target, 'legal_nm')

    unique_address = GroupByMultpleCases(out_target, 'street_addr')
    unique_legalname2 = GroupByMultpleCases(out_target, 'legal_nm')
    unique_tradename = GroupByMultpleCases(out_target, 'trade_nm')
    unique_agency = GroupByMultpleCases(out_target, 'juris_or_proj_nm')
    unique_owner = GroupByMultpleCases(
        out_target, 'Jurisdiction_region_or_General_Contractor')
    agency_df = GroupByMultpleAgency(out_target)
    agency_df_organization = GroupByMultpleAgency(out_target_organization)

    out_target_all = unique_legalname_all.copy()

    
    # sort and format************************************

    # sort for table
    out_sort_ee_violtd = out_target_all.sort_values(
        by=['ee_violtd_cnt'], ascending=False)
    out_sort_bw_amt = out_target_all.sort_values(by=['bw_amt'], ascending=False)
    out_sort_repeat_violtd = out_target_all.sort_values(
        by=['records'], ascending=False)
        

    unique_address = unique_address.sort_values(
        by=['records'], ascending=False)
    unique_legalname = unique_legalname.sort_values(
        by=['records'], ascending=False)
    unique_legalname2 = unique_legalname2.sort_values(
        by=['records'], ascending=False)
    unique_tradename = unique_tradename.sort_values(
        by=['records'], ascending=False)
    unique_agency = unique_agency.sort_values(by=['records'], ascending=False)
    unique_owner = unique_owner.sort_values(by=['records'], ascending=False)
    agency_df = agency_df.sort_values(by=['records'], ascending=False)
    agency_df_organization = agency_df_organization.sort_values(by=['records'], ascending=False)
    
    print_dict['DF_OG'] = Filter_for_Zipcode(print_dict['DF_OG'], "", "", "California") #hack to just california records
    DF_OG_ALL = print_dict['DF_OG'].copy()
    DF_OG_ALL = DropDuplicateRecords(DF_OG_ALL, debug['FLAG_DUPLICATE'], debug['bug_log_csv'])

    DF_OG_VLN = print_dict['DF_OG'].copy()
    DF_OG_VLN = DropDuplicateRecords(DF_OG_VLN, debug['FLAG_DUPLICATE'], debug['bug_log_csv'])
    DF_OG_VLN = Clean_Summary_Values(DF_OG_VLN)

    # report headers***************************************************
    # note that some headers have been renamed at the top of this program
    header_two_way_table = ["violtn_cnt", "ee_violtd_cnt", "bw_amt", "records", "ee_pmt_recv"]
    header = ["legal_nm", "trade_nm", "cty_nm"] + header_two_way_table
    header_two_way = header_two_way_table + \
        ["zip_cd", 'legal_nm', "juris_or_proj_nm", 'case_id_1',
            'violation', 'violation_code', 'backwage_owed']

    header += ["naics_desc."]

    prevailing_header = header + ["juris_or_proj_nm", "Note"]

    if signatories_report == 1:
        header += ["Signatory"]
        prevailing_header += ["Signatory"]

    dup_header = header + ["street_addr"]
    dup_agency_header = header_two_way_table + ["juris_or_proj_nm"]
    dup_owner_header = header_two_way_table + \
        ["Jurisdiction_region_or_General_Contractor"]

    multi_agency_header = header + ["agencies", "agency_names", "street_addr"]

    # textfile output***************************************
    # HTML opening

    # report main file--`w' create/zero text file for writing: the stream is positioned at the beginning of the file.
    
    write_style_html(temp_file_name)
    textFile = open(temp_file_name, 'a')

    write_style_html(print_dict['temp_file_name_HTML_to_PDF'])
    textFile_temp_html_to_pdf = open(print_dict['temp_file_name_HTML_to_PDF'], 'a')

    Title_Block(
        print_dict['TEST_'], DF_OG_VLN, DF_OG_ALL,
        print_dict['target_jurisdition'], print_dict['TARGET_INDUSTRY'],
        print_dict['prevailing_wage_report'], 
        print_dict['includeFedData'], print_dict['includeStateCases'], print_dict['includeStateJudgements'], 
        print_dict['target_organization'], print_dict['open_cases_only'], textFile)
    
    Title_Block(
        print_dict['TEST_'], DF_OG_VLN, DF_OG_ALL, 
        print_dict['target_jurisdition'], print_dict['TARGET_INDUSTRY'],
        print_dict['prevailing_wage_report'], 
        print_dict['includeFedData'], print_dict['includeStateCases'], print_dict['includeStateJudgements'],  
        print_dict['target_organization'], print_dict['open_cases_only'], textFile_temp_html_to_pdf)

    if print_dict['Nonsignatory_Ratio_Block'] == True:
        #Signatory_to_Nonsignatory_Block(DF_OG, DF_OG, textFile)
        do_nothing = "<p>Purposeful Omission of Nonsignatory Ratio Block</p>"

    #if math.isclose(DF_OG['bw_amt'].sum(), out_counts['bw_amt'].sum(), rel_tol=0.03, abs_tol=0.0):
    #    do_nothing = "<p>Purposful Omission of Industry Summary Block</p>"
    #else:
    Industry_Summary_Block(out_counts, out_counts, sum_dict['total_ee_violtd'], sum_dict['total_bw_atp'],
        sum_dict['total_case_violtn'], unique_legalname, agency_df_organization, print_dict['open_cases_only'], 
        textFile)
    Industry_Summary_Block(out_counts, out_counts, sum_dict['total_ee_violtd'], sum_dict['total_bw_atp'],
        sum_dict['total_case_violtn'], unique_legalname, agency_df_organization, print_dict['open_cases_only'], 
        textFile_temp_html_to_pdf)
    Proportion_Summary_Block(out_counts, sum_dict['total_ee_violtd'], sum_dict['total_bw_atp'],
        sum_dict['total_case_violtn'], unique_legalname, agency_df_organization, print_dict['YEAR_START'], 
        print_dict['YEAR_END'], print_dict['open_cases_only'], 
        print_dict['target_jurisdition'], print_dict['TARGET_INDUSTRY'], target_dict['case_disposition_series'], 
        textFile, debug['bug_log_csv'])
    Proportion_Summary_Block(out_counts, sum_dict['total_ee_violtd'], sum_dict['total_bw_atp'],
        sum_dict['total_case_violtn'], unique_legalname, agency_df_organization, print_dict['YEAR_START'], 
        print_dict['YEAR_END'], print_dict['open_cases_only'], 
        print_dict['target_jurisdition'], print_dict['TARGET_INDUSTRY'], target_dict['case_disposition_series'], 
        textFile_temp_html_to_pdf, debug['bug_log_csv'])

    if (len(unique_legalname.index) == 0):
        textFile = open(temp_file_name, 'a')
        textFile.write("<p> There were no records found to report.</p> \n")
        textFile_temp_html_to_pdf = open(temp_file_name, 'a')
        textFile_temp_html_to_pdf.write("<p> There were no records found to report.</p> \n")

    textFile.write("<HR> \n")  # horizontal line
    textFile_temp_html_to_pdf.write("<HR> \n")  # horizontal line

    # HTML closing
    #textFile.write("<P style='page-break-before: always'></p>")
    textFile.write("<div style='break-before:page'></div> \n")
    #textFile.write("</html></body>")
    textFile.close()

    #textFile_temp_html_to_pdf.write("<P style='page-break-before: always'></p>")
    textFile_temp_html_to_pdf.write("<div style='break-before:page'></div> \n")
    #textFile_temp_html_to_pdf.write("</html></body>")
    textFile_temp_html_to_pdf.close()

    # TABLES
    if (print_dict['include_tables'] == 1) and (len(unique_legalname.index) != 0):
        print_table_html_by_industry_and_city(temp_file_name, unique_legalname, header_two_way_table)
        print_table_html_by_industry_and_zipcode(temp_file_name, unique_legalname, header_two_way_table)

    if (print_dict['include_summaries'] == 1) and (len(unique_legalname.index) != 0): 
        print_table_html_Text_Summary(print_dict['include_summaries'], temp_file_name, unique_legalname, header_two_way, header_two_way_table,
            sum_dict['total_ee_violtd'], sum_dict['total_case_violtn'], print_dict['only_sig_summaries'], print_dict['TARGET_INDUSTRY'])
    
    if (print_dict['include_top_viol_tables'] == 1): #and (len(unique_address.index) != 0)
        print_top_viol_tables_html(out_target_all, unique_address, unique_legalname2, 
            unique_tradename, unique_agency, unique_owner, agency_df, out_sort_ee_violtd, 
            out_sort_bw_amt, out_sort_repeat_violtd, temp_file_name, signatories_report,
            target_dict['out_signatory_target'], print_dict['sig_file_name_csv'], prevailing_header, header, multi_agency_header, 
            dup_agency_header, dup_header, dup_owner_header, print_dict['prevailing_wage_report'], target_dict['out_prevailing_target'], 
            print_dict['prev_file_name_csv'], print_dict['TEST_'])
        print_top_viol_tables_html(out_target_all, unique_address, unique_legalname2, 
            unique_tradename, unique_agency, unique_owner, agency_df, out_sort_ee_violtd, 
            out_sort_bw_amt, out_sort_repeat_violtd, print_dict['temp_file_name_HTML_to_PDF'], signatories_report,
            target_dict['out_signatory_target'], print_dict['sig_file_name_csv'], prevailing_header, header, multi_agency_header, 
            dup_agency_header, dup_header, dup_owner_header, print_dict['prevailing_wage_report'], target_dict['out_prevailing_target'], 
            print_dict['prev_file_name_csv'], print_dict['TEST_'])

    if print_dict['include_methods']:
        textFile = open(temp_file_name, 'a')
        #textFile.write("<html><body> \n")
        #textFile.write("<P style='page-break-before: always'></p>")
        textFile.write("<div style='break-before:page'></div> \n")
        textFile.write("\n")
        #textFile.write("<HR> \n")  # horizontal line
        textFile.write("<h1> Notes and methods summary</h1> \n")  # horizontal line

        Footer_Block(print_dict['TEST_'], textFile)

        Notes_Block(textFile)

        Methods_Block(textFile)

        Sources_Block(textFile)



        textFile_temp_html_to_pdf = open(print_dict['temp_file_name_HTML_to_PDF'], 'a')
        #textFile_temp_html_to_pdf.write("<P style='page-break-before: always'></p>")
        textFile_temp_html_to_pdf.write("<div style='break-before:page'></div> \n")
        textFile_temp_html_to_pdf.write("\n")
        #textFile_temp_html_to_pdf.write("<html><body> \n")
        #textFile_temp_html_to_pdf.write("<HR> \n")  # horizontal line
        textFile_temp_html_to_pdf.write("<h1> Notes and methods summary</h1> \n")  # horizontal line

        Footer_Block(print_dict['TEST_'], textFile_temp_html_to_pdf)

        Notes_Block(textFile_temp_html_to_pdf)

        Methods_Block(textFile_temp_html_to_pdf)

        Sources_Block(textFile_temp_html_to_pdf)

    return
