
import pandas as pd
import os
import math

from wagetheft_clean_value_utils import is_string_series

from util_group import (
    GroupByMultpleAgency,
    GroupByX,
    )


def convert_html_to_pdf(source_html, output_filename): #https://stackoverflow.com/questions/57363375/creating-pdfs-from-html-javascript-in-python-with-no-os-dependencies
    from xhtml2pdf import pisa
    with open(source_html, 'r') as html_file:
        html_content = html_file.read()

    with open(output_filename, 'wb') as pdf_file:
        pisa_status = pisa.CreatePDF(html_content, dest=pdf_file)

    if pisa_status.err:
        return False
    
    return True


"""
def weasy_html_to_pdf(source_html): #https://dev.to/bowmanjd/python-pdf-generation-from-html-with-weasyprint-538h
    from weasyprint import HTML
    'Generate a PDF file from a string of HTML.'
    htmldoc = HTML(string=source_html, base_url="")
    return htmldoc.write_pdf()
"""

'''
def pdfkit_html_to_pdf(source_html, temp_file_name_PDF):
    import pdfkit
    #from pypdf import PdfReader, PdfWriter
    # Convert HTML to PDF
    pdfkit.from_file(source_html, temp_file_name_PDF)
    """
    # Read the generated PDF and manipulate it
    with open(temp_file_name_PDF, 'rb') as pdf_file: 
        reader = PdfReader(pdf_file) 
        writer = PdfWriter() 
        for page in reader.pages: 
            writer.add_page(page) 
        with open(temp_file_name_PDF, 'wb') as output_pdf: 
            writer.write(output_pdf)
    """
    return True
'''


def print_table_html_by_industry_and_city(temp_file_name, unique_legalname, header_two_way_table):

    # report main file--'a' Append, the file is created if it does not exist: stream is positioned at the end of the file.
    textFile = open(temp_file_name, 'a')
    textFile.write("<h2>Wage Theft for Selected Organizations by Industry and Region</h2> \n")
    textFile.write("<h3>Wage theft by industry and city region</h3> \n")
    textFile.close()

    df_all_industry = unique_legalname.groupby(['industry', pd.Grouper(key='cty_nm')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
        "bw_amt": 'sum',
        "violtn_cnt": 'sum',
        "ee_violtd_cnt": 'sum',
        "ee_pmt_recv": 'sum',
        "records": 'sum',
    }).reset_index().sort_values(['industry', 'cty_nm'], ascending=[True, True])

    df_all_industry = df_all_industry.set_index(['industry', 'cty_nm'])
    df_all_industry = df_all_industry.sort_index()

    df_all_industry = df_all_industry.reindex(columns=header_two_way_table)
    for industry, new_df in df_all_industry.groupby(level=0):

        new_df = pd.concat([
            new_df,
            new_df.sum().to_frame().T.assign(
                industry='', cty_nm='COUNTYWIDE').set_index(['industry', 'cty_nm'])
        ], sort=True).sort_index()

        new_df["bw_amt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
        new_df["violtn_cnt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
        new_df["ee_violtd_cnt"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)
        new_df["ee_pmt_recv"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
        new_df["records"] = new_df.apply(
            lambda x: "{0:,.0f}".format(x["records"]), axis=1)

        write_to_html_file(new_df, header_two_way_table,
                            "", file_path(temp_file_name))


def print_table_html_by_industry_and_zipcode(temp_file_name, unique_legalname, header_two_way_table):

    textFile = open(temp_file_name, 'a')  # append to main report file
    textFile.write(
        "<h3>Wage theft by zip code region and industry</h3> \n")
    textFile.close()

    df_all_industry_3 = unique_legalname.groupby(["zip_cd", pd.Grouper(key='industry')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
        "bw_amt": 'sum',
        "violtn_cnt": 'sum',
        "ee_violtd_cnt": 'sum',
        "ee_pmt_recv": 'sum',
        "records": 'sum',
    }).reset_index().sort_values(['zip_cd', 'industry'], ascending=[True, True])

    df_all_industry_3 = df_all_industry_3.set_index(['zip_cd', 'industry'])
    df_all_industry_3 = df_all_industry_3.sort_index()

    df_all_industry_3 = df_all_industry_3.reindex(columns=header_two_way_table)
    for zip_cd, new_df_3 in df_all_industry_3.groupby(level=0):

        new_df_3 = pd.concat([
            new_df_3,
            new_df_3.sum().to_frame().T.assign(
                zip_cd='', industry='ZIPCODEWIDE').set_index(['zip_cd', 'industry'])
        ], sort=True).sort_index()

        new_df_3["bw_amt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
        new_df_3["ee_pmt_recv"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
        new_df_3["records"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["records"]), axis=1)
        new_df_3["violtn_cnt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
        new_df_3["ee_violtd_cnt"] = new_df_3.apply(
            lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)

        write_to_html_file(new_df_3, header_two_way_table,
                            "", file_path(temp_file_name))


def print_table_html_Text_Summary(include_summaries, temp_file_name, unique_legalname, header_two_way, header_two_way_table,
    total_ee_violtd, total_case_violtn, only_sig_summaries, TARGET_INDUSTRY):
    if include_summaries == 1:
        
        if 'backwage_owed' not in unique_legalname.columns: unique_legalname['backwage_owed'] = 0 #hack bug fix 10/29/2022

        df_all_industry_n = unique_legalname.groupby(["cty_nm", pd.Grouper(key="zip_cd"), pd.Grouper(key='industry'),  pd.Grouper(key='legal_nm')]).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
            "bw_amt": 'sum',
            "violtn_cnt": 'sum',
            "ee_violtd_cnt": 'sum',
            "ee_pmt_recv": 'sum',
            'backwage_owed': 'sum',
            "records": 'sum',
        }).reset_index().sort_values(['cty_nm', "zip_cd", 'industry', 'legal_nm'], ascending=[True, True, True, True])

        df_all_industry_n = df_all_industry_n.set_index(
            ['cty_nm', "zip_cd", 'industry', 'legal_nm'])
        df_all_industry_n = df_all_industry_n.sort_index()

        df_all_industry_n = df_all_industry_n.reindex(columns=header_two_way)
    
        RunHeaderOnce = True
        for cty_nm, new_df_n, in df_all_industry_n.groupby(level=0):

            # new_df_2 = new_df_n.reset_index(level=1, drop=True).copy() #make a copy without zipcode level 0
            new_df_2 = new_df_n.droplevel("zip_cd").copy()

            new_df_legal_nm = new_df_2.drop(
                columns=['legal_nm'])  # delete empty column
            # pull out legal_nm column from level
            new_df_legal_nm = new_df_legal_nm.reset_index()
            city_unique_legalname = GroupByX(new_df_legal_nm, 'legal_nm')
            city_total_bw_atp = new_df_2['bw_amt'].sum()
            # debug 10/30/2020 this is an approximation based on records which is actually an overtstated mix of case and violations counts
            city_cases = new_df_2['records'].sum()

            new_df_drop1 = new_df_n.droplevel("zip_cd").copy()
            new_df_drop1 = new_df_drop1.droplevel('legal_nm')
            city_agency_df = GroupByMultpleAgency(new_df_drop1)

            #PRINT SECTION HEADER
            if RunHeaderOnce and (only_sig_summaries == 0 or city_cases > 10 or city_total_bw_atp > 10000):
                RunHeaderOnce = False
                textFile = open(temp_file_name, 'a')  # append to report main file
                textFile.write("<h2>Wage Theft for Selected Organizations by Industry and City</h2> \n")
                textFile.close()

            #PRINT SUMMARY BLOCK
            if only_sig_summaries == 0 or (city_cases > 10 or city_total_bw_atp > 10000):
                City_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn,
                                city_unique_legalname, city_agency_df, cty_nm, only_sig_summaries, file_path(temp_file_name))
                City_Summary_Block_4_Zipcode_and_Industry(
                    new_df_n, df_all_industry_n, TARGET_INDUSTRY, only_sig_summaries, file_path(temp_file_name))

                #Industry_Summary_Block(city_cases, new_df_2, total_ee_violtd, city_total_bw_atp, total_case_violtn, city_unique_legalname, city_agency_df, cty_nm, SUMMARY_SIG, file_path(temp_file_name))
                #Industry_Summary_Block_4_Zipcode_and_City(new_df_n, df_all_industry_n, TARGET_INDUSTRY, SUMMARY_SIG, file_path(temp_file_name) )

            new_df_2 = new_df_2.groupby(["cty_nm", 'industry']).agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
                "bw_amt": 'sum',
                "violtn_cnt": 'sum',
                "ee_violtd_cnt": 'sum',
                "ee_pmt_recv": 'sum',
                "records": 'sum',
            }).reset_index().sort_values(['cty_nm', 'industry'], ascending=[True, True])

            new_df_2 = new_df_2.set_index(['cty_nm', 'industry'])
            new_df_2 = new_df_2.sort_index()

            #commented out to test 1/29/2023
            
            new_df_2 = pd.concat([
                new_df_2.sum().to_frame().T.assign(
                    cty_nm='', industry='CITYWIDE').set_index(['cty_nm', 'industry'])
                , new_df_2
            ], sort=True).sort_index()
            

            # new_df_2 = new_df_2.loc[:,~new_df_2.columns.duplicated()] #https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns

            new_df_2["bw_amt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["bw_amt"]), axis=1)
            new_df_2["ee_pmt_recv"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["ee_pmt_recv"]), axis=1)
            new_df_2["records"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["records"]), axis=1)
            new_df_2["violtn_cnt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["violtn_cnt"]), axis=1)
            new_df_2["ee_violtd_cnt"] = new_df_2.apply(
                lambda x: "{0:,.0f}".format(x["ee_violtd_cnt"]), axis=1)

            #PRINT DATA TABLE
            if only_sig_summaries == 0 or (city_cases > 10 or city_total_bw_atp > 10000):
                write_to_html_file(new_df_2, header_two_way_table,
                                "", file_path(temp_file_name))


def print_top_viol_tables_html(df, unique_address, unique_legalname2, 
    unique_tradename, unique_agency, unique_owner, agency_df, out_sort_ee_violtd, 
    out_sort_bw_amt, out_sort_repeat_violtd, temp_file_name, signatories_report,
    out_signatory_target, sig_file_name_csv, prevailing_header, header, multi_agency_header, dup_agency_header, dup_header, 
    dup_owner_header, prevailing_wage_report, out_prevailing_target, prev_file_name_csv, TEST):

    if not df.empty and (len(unique_address) != 0):
        import matplotlib

        # format
        #unique_address = Clean_Repeat_Violator_HTML_Row(unique_address, 'street_addr') #removed 4/18/2024 -- F.Peterson, I have no idea what this did: looks like added 'no records' to some rows
        unique_address = FormatNumbersHTMLRow(unique_address)

        #unique_legalname2 = Clean_Repeat_Violator_HTML_Row(unique_legalname2, 'legal_nm')
        unique_legalname2 = FormatNumbersHTMLRow(unique_legalname2)

        #unique_tradename = Clean_Repeat_Violator_HTML_Row(unique_tradename, 'trade_nm')
        unique_tradename = FormatNumbersHTMLRow(unique_tradename)

        #unique_agency = Clean_Repeat_Violator_HTML_Row(unique_agency, 'juris_or_proj_nm')
        unique_agency = FormatNumbersHTMLRow(unique_agency)

        #unique_owner = Clean_Repeat_Violator_HTML_Row(unique_owner, 'Jurisdiction_region_or_General_Contractor')
        unique_owner = FormatNumbersHTMLRow(unique_owner)

        agency_df = FormatNumbersHTMLRow(agency_df)

        out_sort_ee_violtd = FormatNumbersHTMLRow(out_sort_ee_violtd)
        out_sort_bw_amt = FormatNumbersHTMLRow(out_sort_bw_amt)
        out_sort_repeat_violtd = FormatNumbersHTMLRow(out_sort_repeat_violtd)

        df.plot()  # table setup

        # tables top 10 violators

        #write_to_html_file(out_sort_bw_amt, header, "TEST: Top violators by amount of backwages stolen (by legal name)", file_path(temp_file_name), 6)
        #write_to_html_file(out_sort_ee_violtd, header, "TEST: Top violators by number of employees violated (by legal name)", file_path(temp_file_name), 6)
        #write_to_html_file(out_sort_repeat_violtd, header, "TEST: Top violators by number of repeat violations (by legal name)", file_path(temp_file_name), 6)
        
        #with open(temp_file_name, 'a', encoding='utf-8') as f:  # append to report main file
        #result += "<HR> \n"
        result = "<h2>Top Violators for Selected Region and Industry</h2> \n"

        if not out_sort_bw_amt.empty:
            # by backwages
            result += "<h3>Top violators by amount of backwages stolen (by legal name)</h3> \n"
            result += out_sort_bw_amt.head(6).to_html(columns=header, index=False)
            result += "\n"

        if not out_sort_ee_violtd.empty:
            # by employees
            result += "<h3>Top violators by number of employees violated (by legal name)</h3> \n"
            result += out_sort_ee_violtd.head(6).to_html(
                columns=header, index=False)
            result += "\n"

        if not out_sort_repeat_violtd.empty:
            # by repeated
            result += "<h3>Top violators by number of repeat violations (by legal name)</h3> \n"
            result += out_sort_repeat_violtd.head(6).to_html(
                columns=header, index=False)
            result += "\n"

        # by repeat violators******************
        row_head = 24
        if not unique_address.empty:
            # by unique_address
            result += "<h3>Top violators group by address and sort by records</h3> \n"
            result += unique_address.head(row_head).to_html(
                columns=dup_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no groups by address to report</p> \n"

        if not unique_legalname2.empty:
            # by 'legal_name'
            result += "<h3>Top violators group by legal name and sort by records</h3> \n"
            result += unique_legalname2.head(row_head).to_html(
                columns=dup_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no groups by legal name to report</p> \n"

        if not unique_tradename.empty and not (unique_tradename['trade_nm'].isna().all() | (unique_tradename['trade_nm']=="").all()) and TEST != 3:
            # by unique_trade_nm
            result += "<h3>Top violators group by trade name and sort by records</h3> \n"
            result += unique_tradename.head(row_head).to_html(
                columns=dup_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no groups by trade name to report</p> \n"

        if not agency_df.empty:
            # report for cases from multiple agencies
            result += "<h3>Top violators group by company and sort by number of agencies involved</h3> \n"
            result += agency_df.head(row_head).to_html(
                columns=multi_agency_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no groups by companies with violations by multiple agencies to report</p> \n"

        if not unique_agency.empty:
            # report agency counts
            # by unique_agency
            result += "<h3>Cases by agency or owner</h3> \n"
            result += unique_agency.head(row_head).to_html(
                columns=dup_agency_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no case groups by agency or owner to report</p> \n"

        if not unique_owner.empty:
            # by 'unique_owner'
            result += "<h3>Cases by jurisdiction (includes private jurisdictions)</h3> \n"
            result += unique_owner.head(row_head).to_html(
                columns=dup_owner_header, index=False)
            result += "\n"
        else:
            result += "<p> There are no case groups by jurisdiction to report</p> \n"

        # signatory
        if signatories_report == 1 and not out_signatory_target.empty:

            out_sort_signatory = pd.DataFrame()
            out_sort_signatory = out_signatory_target.sort_values(
                'legal_nm', ascending=True)

            out_sort_signatory['violtn_cnt'] = out_sort_signatory.apply(
                lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)
            out_sort_signatory['ee_pmt_recv'] = out_sort_signatory.apply(
                lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)

            #f.write("<P style='page-break-before: always'></p>")
            f.write("<div style='break-before:page'></div> \n")
            out_sort_signatory.to_csv(sig_file_name_csv)

            f.write("<h3>All signatory wage violators</h3> \n")

            if not len(out_sort_signatory.index) == 0:
                f.write("<p>Signatory wage theft cases: ")
                f.write(str.format('{0:,.0f}', len(
                    out_sort_signatory.index)))
                f.write("</p> \n")

            if not out_sort_signatory['bw_amt'].sum() == 0:
                f.write("<p>Total signatory wage theft: $")
                f.write(str.format(
                    '{0:,.0f}', out_sort_signatory['bw_amt'].sum()))
                f.write("</p> \n")
            '''
            if not out_sort_signatory['ee_violtd_cnt'].sum()==0:
                f.write("<p>Signatory wage employees violated: ")
                out_sort_signatory['ee_violtd_cnt'] = pd.to_numeric(out_sort_signatory['ee_violtd_cnt'] )
                f.write(str.format('{0:,.0f}',out_sort_signatory['ee_violtd_cnt'].sum() ) )
                f.write("</p> \n")

            if not out_sort_signatory['violtn_cnt'].sum()==0:
                f.write("<p>Signatory wage violations: ")
                out_sort_signatory['violtn_cnt'] = pd.to_numeric(out_sort_signatory['violtn_cnt'] )
                f.write(str.format('{0:,.0f}',out_sort_signatory['violtn_cnt'].sum() ) )
                f.write("</p> \n")
            '''

            f.write("\n")

            out_sort_signatory.to_html(
                f, max_rows=3000, columns=prevailing_header, index=False)

            f.write("\n")


        # prevailing wage
        if prevailing_wage_report == 1 and not out_prevailing_target.empty:
            out_sort_prevailing_wage = pd.DataFrame()
            #out_sort_prevailing_wage = out_prevailing_target.sort_values('records', ascending=False)
            out_sort_prevailing_wage = out_prevailing_target.sort_values(
                'legal_nm', ascending=True)

            out_sort_prevailing_wage['violtn_cnt'] = out_sort_prevailing_wage.apply(
                lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)
            out_sort_prevailing_wage['ee_pmt_recv'] = out_sort_prevailing_wage.apply(
                lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)

            #result += "<P style='page-break-before: always'></p>"
            result += "<div style='break-before:page'></div> \n"
            out_sort_prevailing_wage.to_csv(prev_file_name_csv)

            result += "<h3>All prevailing wage violators</h3> \n"

            result += "<p>Prevailing wage theft cases: "
            result += str.format('{0:,.0f}', len(
                out_sort_prevailing_wage.index))
            #f.write(str.format('{0:,.0f}',len(out_sort_prevailing_wage['records'].sum() ) ) )
            result += "</p> \n"

            result += "<p>Total prevailing wage theft: $"
            result += str.format(
                '{0:,.0f}', out_sort_prevailing_wage['bw_amt'].sum())
            result +="</p> \n"

            result += "<p>Total prevailing wage theft: $"
            result += str.format(
                '{0:,.0f}', out_sort_prevailing_wage['bw_amt'].sum())
            result += "</p> \n"

            # buggy 6/14/2021
            # f.write("<p>Prevailing wage employees violated: ")
            # out_sort_prevailing_wage['ee_violtd_cnt'] = pd.to_numeric(out_sort_prevailing_wage['ee_violtd_cnt'] )
            # f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['ee_violtd_cnt'].sum() ) )
            # f.write("</p> \n")

            # f.write("<p>Prevailing wage violations: ")
            # out_sort_prevailing_wage['violtn_cnt'] = pd.to_numeric(out_sort_prevailing_wage['violtn_cnt'] )
            # f.write(str.format('{0:,.0f}',out_sort_prevailing_wage['violtn_cnt'].sum() ) )
            # f.write("</p> \n")

            # 12/25/2021 added "float_format=lambda x: '%10.2f' % x" per https://stackoverflow.com/questions/14899818/format-output-data-in-pandas-to-html
            html_text_table = out_sort_prevailing_wage.to_html(
                max_rows=24, columns=prevailing_header, index=False, float_format=lambda x: '%10.2f' % x)

            result += "\n" + html_text_table + "\n"


        else:
            result += "\n"
            result += "<p> There are no prevailing wage cases to report.</p> \n"
            result += "\n"
            

    with open(temp_file_name, mode='a', encoding='utf-8') as f:  # append to report main file
        f.write(result)

def write_style_html(temp_file_name):

    result = '''
        <!DOCTYPE html>
        <html>
        <head>
        <title>Theft Violations</title>
        <style>

            h2, h3 {
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
        <body> \n
        '''

    with open(temp_file_name, mode='w', encoding='utf-8') as f:  # append to report main file
        f.write(result)


def Clean_Repeat_Violator_HTML_Row(df, COLUMN_NAME): #idk what this if for anymore -- F.PEterson 4/19/2024
    # https://stackoverflow.com/questions/18172851/deleting-dataframe-row-in-pandas-based-on-column-value
    df = df[df.records > 1]
    
    if COLUMN_NAME in df.columns:
        if (df[COLUMN_NAME].isna().all() | (df[COLUMN_NAME]=="").all()):
            df[COLUMN_NAME] = "no records"
    else:
        df = df.assign(COLUMN_NAME = "no records")
    
    #https://stackoverflow.com/questions/24284342/insert-a-row-to-pandas-dataframe
    for i in range(10000):
        df = pd.concat([pd.DataFrame([[1,2]], columns=df.columns), df], ignore_index=True)
    
    return df



def FormatNumbersHTMLRow(df):
    if not None:
        if not is_string_series(df['bw_amt']):
            df['bw_amt'] = df.apply(
                lambda x: "{0:,.0f}".format(x['bw_amt']), axis=1)
        if not is_string_series(df['ee_violtd_cnt']):
            df['ee_violtd_cnt'] = df.apply(
                lambda x: "{0:,.0f}".format(x['ee_violtd_cnt']), axis=1)
        if not is_string_series(df['ee_pmt_recv']):    
            df['ee_pmt_recv'] = df.apply(
                lambda x: "{0:,.0f}".format(x['ee_pmt_recv']), axis=1)
        if not is_string_series(df['violtn_cnt']):   
            df['violtn_cnt'] = df.apply(
                lambda x: "{0:,.0f}".format(x['violtn_cnt']), axis=1)

    return df


def Title_Block(TEST, DF_OG_VLN, DF_OG_ALL, target_jurisdition, TARGET_INDUSTRY, prevailing_wage_report, federal_data, \
                includeStateCases, includeStateJudgements, target_organization, open_cases_only, textFile):
    
    scale = ""
    if open_cases_only:
        scale = "Unpaid"
    else:
        scale = "Total"

    textFile.write(
        f"<h1>DRAFT REPORT: {scale} Wage Theft in the Jurisdiction of {target_jurisdition} for {TARGET_INDUSTRY[0][0]} Industry</h1> \n")
    if prevailing_wage_report == 1:
        textFile.write(
            f"<h2 align=center>***PREVAILING WAGE REPORT***</h2> \n")
    if (federal_data == 1) and ((includeStateCases and includeStateJudgements) == 0):
        textFile.write(
            f"<h2 align=center>***FEDERAL DOL WHD DATA ONLY***</h2> \n")  # 2/5/2022
    if federal_data == 0 and ((includeStateCases or includeStateJudgements) == 1):
        textFile.write(
            f"<h2 align=center>***CA STATE DLSE DATA ONLY***</h2> \n")
    if len(target_organization) > 3:
        textFile.write(f"<h2 align=center> ORGANIZATION SEARCH </h2> \n")
        textFile.write(f"<h2 align=center> {target_organization} </h2> \n")  
    
    textFile.write("\n")

    # all data summary block
    if TEST != 3:
        plural = ""
        if (federal_data == 1) and (includeStateCases == 0) and (includeStateJudgements) == 0:
            plural = "the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments)"
        if (federal_data == 0) and (includeStateCases == 1) and (includeStateJudgements) == 0:
            plural = "the Division of Labor Standards Enforcement Wage Claim Adjudications"
        if (federal_data == 0) and (includeStateCases == 0) and (includeStateJudgements == 1):
            plural = "the Division of Labor Standards Enforcement judgments"

        if (federal_data == 1) and (includeStateCases == 1) and (includeStateJudgements == 0):
            plural = "a combination of the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments) and the Division of Labor Standards Enforcement Wage Claim Adjudications"
        if (federal_data == 1) and (includeStateCases == 0) and (includeStateJudgements == 1):
            plural = "a combination of the Department of Labor Wage and Hour Division cases (not all result \
                       in judgments) and the Division of Labor Standards Enforcement judgments"
        if (federal_data == 0) and (includeStateCases == 1) and (includeStateJudgements == 1):
            plural = "a combination of the Division of Labor Standards Enforcement Wage Claim Adjudications and \
                        the Division of Labor Standards Enforcement judgments"
        if (federal_data == 1) and (includeStateCases == 1) and (includeStateJudgements == 1):
            plural = "a combination of the Division of Labor Standards Enforcement Wage Claim Adjudications, \
                the Division of Labor Standards Enforcement Wage Claim Adjudications, \
                    and the Division of Labor Standards Enforcement judgments"
            
        source_fed = ""
        if (federal_data == 1):
            source_fed = "WHD data were obtained from the DOL"
        source_state = ""
        if (includeStateCases == 1) or (includeStateJudgements == 1):
            source_state = "DLSE data pre-2020 were obtained through a Section 6250 CA Public Records Act request \
                       (does not include purged cases which are those settled and then purged typically after three years), \
                       and then post-2020 DLSE data are from the CA DIR websearch portal"
        tense = ""
        if (federal_data == 1) and ((includeStateCases == 1) or (includeStateJudgements == 1)):
            tense = ", and the "
        
        textFile.write(f"<p>These data are {plural}. The {source_fed} {tense} {source_state}.</p>")
        
    textFile.write("\n")

    textFile.write(
        "<p>The dataset in this report is pulled from a larger dataset that for all regions and sources contains ")
    textFile.write(str.format('{0:,.0f}', DF_OG_ALL['case_id_1'].size))
    textFile.write(" cases")

    if not DF_OG_VLN['violtn_cnt'].sum() == 0:
        textFile.write(", ")
        textFile.write(str.format('{0:,.0f}', DF_OG_VLN['violtn_cnt'].sum()))
        textFile.write(" violations")

    if not DF_OG_ALL['ee_violtd_cnt'].sum() == 0:
        textFile.write(", ")
        textFile.write(str.format(
            '{0:,.0f}', DF_OG_ALL['ee_violtd_cnt'].sum()))
        textFile.write(" employees")

    if not DF_OG_VLN['bw_amt'].sum() == 0:
        textFile.write(", and  $ ")
        textFile.write(str.format('{0:,.0f}', DF_OG_VLN['bw_amt'].sum()))
        textFile.write(" in backwages")

    # <--i have no idea, it works fin above but here I had to do this 11/3/2020
    test_sum = DF_OG_VLN['ee_pmt_recv'].sum()
    if not test_sum == 0:
        textFile.write(", and  $ ")
        textFile.write(str.format('{0:,.0f}', test_sum))
        textFile.write(" in restituted backwages")

    textFile.write(".")

    from datetime import datetime
    textFile.write(" This is approximately a ")
    DF_MIN_ALL = min(pd.to_datetime(
        DF_OG_ALL['findings_start_date'].dropna(), errors='coerce'))
    DF_MAX_ALL = max(pd.to_datetime(
        DF_OG_ALL['findings_start_date'].dropna(), errors='coerce'))
    DF_MAX_ALL_YEARS = (DF_MAX_ALL - DF_MIN_ALL).days / 365

    textFile.write(str.format(
        '{0:,.2f}', ((DF_OG_ALL['bw_amt'].sum()/DF_MAX_ALL_YEARS)/22000000000)*100))
    textFile.write("-percent sample of an estimated actual $2B annually in wage theft that occurs Statewide (see courts.ca.gov/opinions/links/S241812-LINK1.PDF#page=11).</p>")

    textFile.write("\n")

    if TEST != 3:

        fed_range = ""
        state_range = ""
        if (federal_data == 1):
            fed_range = "total Federal WHD dataset goes back to 2000"

        if (includeStateCases == 1) or (includeStateJudgements == 1):
            state_range = "total State DLSE dataset goes back to 2000 (Note: The State purged old closed cases and thus the above is an imperfect ratio)"

        tense = ""
        if (federal_data == 1) and ((includeStateCases == 1) or (includeStateJudgements == 1)):
            tense = ", and "


        textFile.write(f"<p>The {fed_range} {tense} {state_range}.</p>")

    textFile.write("<p> These data are internally incomplete, and do not include private lawsuits, stop notices, and complaints to the awarding agency, contractor, employment department, licensing board, and district attorney. ")
    textFile.write("Therefore, the following is a sample given the above data constraints and the reluctance by populations to file wage and hour claims.</p>")

    textFile.write("\n")
    textFile.write("\n")


def Notes_Block(textFile, default_zipcode="####X"):

    textFile.write("<p>Notes:</p>")
    textFile.write("\n")
    textFile.write("<p>")
    textFile.write(
        f"(1) In the tables and city summaries, the zip {default_zipcode} represents data that is missing the zip code field. ")
    textFile.write("(2) There are unlabeled industries, many of these are actually construction, care homes, restaurants, etc. just there is not an ability to label them as such--a label of 'other' could lead one to indicate that they are not these industries and therefore the category of 'undefined.' ")
    textFile.write("(3) Values may deviate by 10% within the report for camparable subcategories: this is due to labeling and relabeling of industry that may overwrite a previous industry label (for example Nail Hamburger could be labeled service or food). ")
    textFile.write("</p>")

    textFile.write("\n")

    textFile.write(
        "<p>Note that categorizations are based on both documented data and intelligent inferences, therefore, there are errors. ")
    textFile.write("For the fields used to prepare this report, please see <a href='https://docs.google.com/spreadsheets/d/19EPT9QlUgemOZBiGMrtwutbR8XyKwnrEhB5rZpZqM98/'>https://docs.google.com/spreadsheets/d/19EPT9QlUgemOZBiGMrtwutbR8XyKwnrEhB5rZpZqM98/</a> . ")
    textFile.write("And for the industry categories, which are given shortened names here, please see <a href='https://www.naics.com/search/'>https://www.naics.com/search/</a>  . </p>")
    #textFile.write("To see a visualization of the data by zip code and industry, please see (last updated Feb 2020) <a href='https://public.tableau.com/profile/forest.peterson#!/vizhome/Santa_Clara_County_Wage_Theft/SantaClaraCounty'></a> . </p>")

    textFile.write("\n")

    textFile.write("<p>The DIR DLSE uses one case per employee while the DOL WHD combines employees into one case. </p>")


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
    textFile.write(
        "<li>the sum of wages owed, monetary penalty, and interest</li>")
    textFile.write(
        "<li>df['backwage_owed'] = df['wages_owed'] + df['cmp_assd_cnt'] + df['interest_owed']</li>")
    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("estimate when missing:")
    textFile.write("</p>")
    
    textFile.write("<ul>")
    textFile.write(
        "<li>estimated backwage per employee = (df['bw_amt'].sum() / df['ee_violtd_cnt'].sum() ) </li>")
    textFile.write(
        "<li>estimated monetary penalty (CMP) assessed per employee = (est_bw_amt * 12.5%) </li>")
    textFile.write(
        "<li>where interest balance due is missing, then infer an interest balance based on a calaculated compounded interest of the backwages owed</li>")
    
    textFile.write("<ul>")
    textFile.write(
        "<li>df['interest_owed'] = np.where((df['interest_owed'].isna() | (df['interest_owed'] == '')), df['Interest_Accrued'], df['interest_owed'])</li>")
    textFile.write(
        "<li>df['Interest_Accrued'] = (df['wages_owed'] * (((1 + ((r/100.0)/n)) ** (n*df['Years_Unpaid']))) ) - df['wages_owed']</li>")
    textFile.write("</ul>")

    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("wages_owed: ")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write(
        "<li>unpaid backwages less payment recieved by employee</li>")
    textFile.write(
        "<li>df['wages_owed'] = df['bw_amt'] - df['ee_pmt_recv']</li>")
    textFile.write("</ul>")

    textFile.write("<p>")
    textFile.write("interest_owed: ")
    textFile.write("</p>")
    textFile.write("<ul>")
    textFile.write("<li>interestd due less interest payments recieved</li>")
    textFile.write(
        "<li>df['interest_owed'] = df['Interest_Accrued'] - df['Interest_Payments_Recd])</li>")
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

    textFile.write("\n")
    textFile.write("\n")

def Sources_Block(textFile):
    textFile.write("<p>")
    textFile.write("Data Sources: ")
    textFile.write("</p> \n")

    textFile.write("<p>")

    textFile.write("<ul>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Judgements from Aug 2019 to Jan 2024 <a href='https://cadir.my.site.com/'>https://cadir.my.site.com/</a>  </li>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Wage Claim Adjudications (WCA) from Aug 2019 to Jan 2024 <a href='https://cadir.my.site.com/wcsearch/s/'>https://cadir.my.site.com/wcsearch/s/</a>  </li>")
    textFile.write(
        "<li>CA Dept. of Labor Standards Enforcement (DLSE) Wage Claim Adjudications (WCA) from Jan 2001 to Jul 2019 <a href='https://www.researchgate.net/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019'>https://www.researchgate.net/publication/357767172_California_Dept_of_Labor_Standards_Enforcement_DLSE_PRA_Wage_Claim_Adjudications_WCA_for_all_DLSE_offices_from_January_2001_to_July_2019</a>  </li>")
    textFile.write(
        "<li>US DOL WHD Enforcement Database Compliance Actions from Jan 2005 to Jan 2024 <a href='https://enforcedata.dol.gov/views/data_catalogs.php'>https://enforcedata.dol.gov/views/data_catalogs.php</a> </li>")
    textFile.write("</ul>")

    textFile.write("</p>")

    textFile.write("\n")
    textFile.write("\n")


def Signatory_to_Nonsignatory_Block(df1, df2, filename):
    # construction
    # medical

    # ratio_construction = df1.query(
    # 	"signatory_industry == 'construction_signatories' and signatory_industry == 'signatories_UCON' and signatory_industry == 'signatories_CEA' "
    # 	)['bw_amt'].sum() / df1['bw_amt'].sum()

    ratio_construction_ = df1.loc[df1['signatory_industry']
                                  == 'Construction', 'backwage_owed'].sum()
    #ratio_construction_ucon = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
    #ratio_construction_cea = df1.loc[df1['signatory_industry'] == 'Construction','backwage_owed'].sum()
    construction_industry_backwage = df1.loc[df1['industry']
                                             == 'Construction', 'backwage_owed'].sum()
    ratio_construction = (ratio_construction_) / construction_industry_backwage

    ratio_hospital_ = df1.loc[df1['signatory_industry']
                              == 'Health_care', 'backwage_owed'].sum()
    carehome_industry_backwage = df1.loc[df1['industry']
                                         == 'Carehome', 'backwage_owed'].sum()
    healthcare_industry_backwage = df1.loc[df1['industry']
                                           == 'Health_care', 'backwage_owed'].sum()
    residential_carehome_industry_backwage = df1.loc[df1['industry']
                                                     == 'Residential_carehome', 'backwage_owed'].sum()
    ratio_hospital = ratio_hospital_ / \
        (carehome_industry_backwage + healthcare_industry_backwage +
         residential_carehome_industry_backwage)

    filename.write("<p>")
    filename.write("Of $ ")
    filename.write(str.format('{0:,.0f}', df1['backwage_owed'].sum()))
    filename.write(
        " in all industry back wages owed (inc. monetary penalty and interest), ")
    filename.write(" union signatory employers represent: </p>")
    filename.write("<p>")
    filename.write(str.format('{0:,.0f}', ratio_construction * 100))
    filename.write(
        " percent of total documented theft in the construction industry with $ ")
    filename.write(str.format('{0:,.0f}', (ratio_construction_)))
    filename.write(" of $ ")
    filename.write(str.format('{0:,.0f}', construction_industry_backwage))
    filename.write(" in construction specific backwages. ")
    filename.write("</p>")
    filename.write("<p>")
    filename.write(str.format('{0:,.0f}', ratio_hospital * 100))
    filename.write(
        " percent of total documented theft in the healthcare industry with $ ")
    filename.write(str.format('{0:,.0f}', ratio_hospital_))
    filename.write(" of $ ")
    filename.write(str.format('{0:,.0f}', (carehome_industry_backwage +
                                           healthcare_industry_backwage + residential_carehome_industry_backwage)))
    filename.write(" in healthcare specific backwages. ")
    filename.write("</p>")
    filename.write("<p> Due to the situation of the union worker as fairly paid, educated in labor rights, and represented both in bargaining and enforcement of that bargained for agreement--as well as that these are two heavily unionized industries and that much of the non-union data is lost as undefined industry--then these percentages are likely overly represented as union workers would know how and when to bring a wage and hour case. As such, it is fair to conclude that there effectively is no concernable degree of wage theft in the unionized workforce that requires outside enforcement. </p>")


def Footer_Block(TEST, textFile):
    textFile.write("<p> Report generated ")
    textFile.write(pd.to_datetime('today').strftime("%m/%d/%Y"))
    textFile.write("</p> \n")

    textFile.write("<p>Palo Alto Data Group pulled a list from databases of all sector judgments and adjudications \
        to generate this report using an open source software that was prepared by the Center for Integrated Facility Engineering (CIFE) \
        at Stanford University in collaboration with the Santa Clara County Wage Theft Coalition. These data \
        have not been audited and, therefore, are only intended as an indication of wage theft.</p> \n")

#write_to_html_file(new_df_3, header_HTML_EMP3, "", file_path('py_output/A4W_Summary_by_Emp_for_all2.html') )

# https://stackoverflow.com/questions/47704441/applying-styling-to-pandas-dataframe-saved-to-html-file
def write_to_html_file(df, header_HTML, title, filename, rows = 99):
    # added this line to avoid error 8/10/2022 f. peterson
    import pandas.io.formats.style
    import os  # added this line to avoid error 8/10/2022 f. peterso
    
    result = '<h3> %s </h3>\n' % title
    if type(df) == pd.io.formats.style.Styler:
        result += df.render()
    else:
        result += df.to_html(max_rows = rows, classes='wide', columns=header_HTML, escape=False)

    # with open(filename, mode='a') as f:
    # added this line to avoid error 8/10/2022 f. peterson
    # create directory if it doesn't exist
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    # https://stackoverflow.com/questions/27092833/unicodeencodeerror-charmap-codec-cant-encode-characters
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)


def debug_fileSetup_def(bug_filename):

    bug_filename.write("<!DOCTYPE html>")
    bug_filename.write("\n")
    bug_filename.write("<html><body>")
    bug_filename.write("\n")

    bug_filename.write("<h1>START</h1>")
    bug_filename.write("\n")




def City_Summary_Block_4_Zipcode_and_Industry(df, df_max_check, TARGET_INDUSTRY, SUMMARY_SIG, filename):

    result = ""

    # zip code = loop through
    df = df.reset_index(level=0, drop=True)  # drop city category

    #df = df.groupby(level=0)
    # df = df.agg({ #https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
    # 	"bw_amt":'sum',
    # 	"violtn_cnt":'sum',
    # 	"ee_violtd_cnt":'sum',
    # 	"records": 'sum',
    # 	}).reset_index().sort_values(["zip_cd"], ascending=True)
    zipcode = ""
    for zipcode, df_zipcode in df.groupby(level=0):  # .groupby(level=0):

        # print theft level in $ and employees
        test_num1 = pd.to_numeric(
            df_zipcode['bw_amt'].sum() )
        test_num2 = pd.to_numeric(
            df_zipcode['ee_violtd_cnt'].sum() )

        if test_num1 < 3000:
            # result +="<p> has no backwage data.</p>""
            dummy = ""  # just does nothing
        else:
            result += "<p>"
            result += "In the "
            result += str(zipcode) #10/26/2022 found bug "can only concatenate str (not "bool") to str"
            result += " zip code, "
            result += (str.format('{0:,.0f}', test_num2))
            if math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
                result += " worker suffered wage theft totaling $ "
            else:
                result += " workers suffered wage theft totaling $ "

            result += (str.format('{0:,.0f}', test_num1))
            result += " "

            # print the industry with highest theft

            # check df_max_check for industry with highest theft in this zip code
            df_zipcode = df_zipcode.reset_index(
                level=0, drop=True)  # drop zip code category
            df_zipcode = df_zipcode.groupby(level=0)

            df_zipcode = df_zipcode.agg({  # https://towardsdatascience.com/pandas-groupby-aggregate-transform-filter-c95ba3444bbb
                "bw_amt": 'sum',
                "violtn_cnt": 'sum',
                "ee_violtd_cnt": 'sum',
                "ee_pmt_recv": 'sum',
                "records": 'sum',
            }).reset_index().sort_values(["bw_amt"], ascending=False)

            # check df_max_check that industry in the top three for the County
            industry_list = len(TARGET_INDUSTRY)
            if industry_list > 2:
                result += ". The "
                result += df_zipcode.iloc[0, 0]
                result += " industry is one of the largest sources of theft in this zip code"
                # if math.isclose(df_zipcode.iloc[0,1], df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
                #result += " (this zip code has one of the highest levels of theft in this specific industry across the County)"
                # result += str.format('{0:,.0f}', df_zipcode.iloc[0,1])
                # result += "|"
                # result += str.format('{0:,.0f}', df_max_check['bw_amt'].max() )

            # note if this is the top 3 zip code for the county --> df_max_check
            # if test_num1 == df_max_check['bw_amt'].max():
            if math.isclose(test_num1, df_max_check['bw_amt'].max(), rel_tol=0.05, abs_tol=0.0):
                result += " (this zip code has one of the highest overall levels of theft in the County)."
                # result += str.format('{0:,.0f}', test_num1)
                # result += "|"
                # result += str.format('{0:,.0f}',df_max_check['bw_amt'].max() )
            else:
                result += ". "
            result += "</p>"
            result += ("\n")

    result += ("\n")
    result += ("\n")

    result += '''
		</body>
		</html>
		'''
    # with open(filename, mode='a') as f:
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)


def City_Summary_Block(city_cases, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, cty_nm, SUMMARY_SIG, filename):

    result = '<h2>'
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

    result += "<p>"
    test_num1 = pd.to_numeric(df['bw_amt'].sum() )
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
        result += "There is at least one wage theft case"
        if test_num1 <= 3000:
            result += " in the City of "
            result += cty_nm.title()
        result += ", "
    else:
        result += "There are "
        result += (str.format('{0:,.0f}', city_cases))
        result += " wage theft cases"
        if test_num1 <= 3000:
            result += " in the City of "
            result += cty_nm.title()
        result += ", "

    # total theft $
    #test_num1 = pd.to_numeric(df['bw_amt'].sum() )
    if test_num1 < 1 and city_cases < 1:
        do_nothing = ""
        #result +=" and, there is no backwage data. "
    elif test_num1 < 1 and city_cases >= 1:
        result += " however, the backwage data is missing. "
    elif test_num1 > 3000:
        result += " resulting in a total of $ "
        result += (str.format('{0:,.0f}', test_num1))
        result += " in stolen wages. "
    else:
        result += " resulting in stolen wages. "

    # total unpaid theft $
    test_num0 = pd.to_numeric(df['ee_pmt_recv'].sum() )
    if test_num0 < 1:
        do_nothing = ""
        #result +="Of that, an unknown amount is still due to the workers of this city. "
    else:
        result += ("Of that, $ ")
        result += (str.format('{0:,.0f}', test_num0))
        result += " has been returned to the workers of this city. "

    # total number of violations
    test_num2 = pd.to_numeric(df['ee_violtd_cnt'].sum() )
    if test_num2 < 1:
        do_nothing = ""
        #result +="Therefore, there is no case evidence of workers affected by stolen wages. "
    else:
        test_num3 = pd.to_numeric(df['violtn_cnt'].sum() )
        if math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
            result += "The theft comprises at least one discrete wage-and-hour violation "
        else:
            result += "The theft comprises "
            result += (str.format('{0:,.0f}', test_num3))
            result += " discrete wage-and-hour violations "

    if test_num2 < 1:
        do_nothing = ""
    elif math.isclose(test_num2, 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "affecting at least one worker: "
    else:
        result += "affecting "
        result += (str.format('{0:,.0f}', test_num2))
        result += " workers: "

    # xx companies have multiple violations
    if len(unique_legalname.index) < 1:
        do_nothing = ""
        #result +="No employer was found with more than one case. "
    if math.isclose(len(unique_legalname.index), 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "At least one employer has multiple cases. "
    else:
        result += (str.format('{0:,.0f}', len(unique_legalname.index)))
        result += " employers have multiple cases recorded against them. "

    # xx companies cited by multiple agencies
    if len(agency_df.index) < 1:
        #result +="No employer was found with cases from multiple agencies. "
        do_nothing = ""
    elif math.isclose(len(agency_df.index), 1.0, rel_tol=0.05, abs_tol=0.0):
        result += "at least one employer has cases from multiple agencies. "
    else:
        result += (str.format('{0:,.0f}', len(agency_df.index)))
        result += " employers have cases from multiple agencies. "

    # employer with top theft
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

        temp_row = unique_legalname.nlargest(
            3, 'backwage_owed').reset_index(drop=True)
        temp_value_1 = temp_row['backwage_owed'].iloc[0]
        if temp_row.size > 0 and temp_value_1 > 0:
            # indexNamesArr = temp_row.index.values[0] #https://thispointer.com/python-pandas-how-to-get-column-and-row-names-in-dataframe/
            #result += indexNamesArr.astype(str)
            result += temp_row['legal_nm'].iloc[0]
            result += " is the employer with the highest recorded theft in this city, "
            result += "it has unpaid wage theft claims of $ "
            result += (str.format('{0:,.0f}', temp_value_1))
            result += ". "

        result += "</p>"

        result += ("\n")
        result += ("\n")

    # close
    result += '''
		</body>
		</html>
		'''
    # with open(filename, mode='a') as f:
    with open(filename, mode='a', encoding="utf-8") as f:
        f.write(result)


def Industry_Summary_Block(out_counts, df, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, OPEN_CASES, textFile):
    
    textFile.write("<h2>Summary for Reported Industry and Organizations</h2> \n")

    if not df.empty:

        DF_MIN = min(pd.to_datetime(
            df['findings_start_date'].dropna(), errors='coerce'))
        DF_MAX = max(pd.to_datetime(
            df['findings_start_date'].dropna(), errors='coerce'))

        textFile.write(f"<p>Actual date range: ")
        textFile.write(DF_MIN.strftime("%m/%Y"))
        textFile.write(" to ")
        textFile.write(DF_MAX.strftime("%m/%Y"))
        textFile.write("</p> \n")
    else:
        textFile.write("<p>Actual date range: <undefined></p> \n")

    if OPEN_CASES == 1:
        textFile.write(
            "<p>This report has cases removed that are documented as paid or claimant withdrew or the amount repaid matches the backwages owed.</p> \n")

    if not len(out_counts.index) == 0:
        if OPEN_CASES == 1:
            textFile.write("<p>Active unpaid wage theft cases: ")
        else:
            textFile.write("<p>Total wage theft cases: ")
        textFile.write(str.format('{0:,.0f}', len(out_counts.index)))
        textFile.write(" <i>(Note: sum is of several types of 'open' disposition)</i></p> \n")

    if not (out_counts['bw_amt'].sum() == 0) and not out_counts.empty:
        textFile.write("<p>Total wage theft:  $ ")
        textFile.write(str.format('{0:,.0f}', out_counts['bw_amt'].sum()))
        if not df.empty:
            if total_ee_violtd == 0:
                textFile.write(
                    " <i> Note: Backwages per employee violated is not calulated in this report</i></p> \n")
            else:
                textFile.write(" <i>(gaps estimated as $")
                textFile.write(str.format(
                    '{0:,.0f}', total_bw_atp//total_ee_violtd))
                textFile.write(" in backwage ")
                textFile.write(" and $")
                textFile.write(str.format(
                    '{0:,.0f}', ((total_bw_atp//total_ee_violtd) * .125) ) )
                textFile.write(" monetary penalty per employee violated ) ")
                textFile.write("</i>")
        textFile.write("</p> \n")

    if ('backwage_owed' not in out_counts.columns) and not out_counts.empty: #<-- probably a problem point
        out_counts['backwage_owed'] = 0
    
    if not (out_counts['backwage_owed'].sum() == 0) and not out_counts.empty:
        textFile.write(
            "<p>Including monetary penalties and accrued interest, the amount owed is:  $ ")
        textFile.write(str.format(
            '{0:,.0f}', out_counts['backwage_owed'].sum()))  # bw_amt
        textFile.write("</p>")
    else: 
        textFile.write(
            " <i> Note: Monetary penalties and accrued interest not calculated in this report</i></p> \n")

    if not (out_counts['ee_violtd_cnt'].sum() == 0) and not out_counts.empty:
        textFile.write("<p>Total employees: ")
        textFile.write(str.format(
            '{0:,.0f}', out_counts['ee_violtd_cnt'].sum()))
        textFile.write("</p> \n")

    if not (out_counts['violtn_cnt'].sum() == 0):
        textFile.write("<p>Total violations: ")
        textFile.write(str.format('{0:,.0f}', out_counts['violtn_cnt'].sum()))
        if not df.empty:
            if total_ee_violtd == 0:
                textFile.write(
                    " <i> Note: Violations per employee is not calculated in this report</i></p> \n")
            else:
                textFile.write(" <i>(gaps estimated as ")
                textFile.write(str.format(
                    '{0:,.2g}', total_case_violtn//total_ee_violtd))
                textFile.write(" violation(s) per employee violated)</i>")
        textFile.write("</p>")

    textFile.write("\n")
    textFile.write("\n")

    textFile.write("<p>Companies that are involved in multiple cases: ")
    textFile.write(str.format('{0:,.0f}', len(unique_legalname.index)))  # here
    textFile.write("</p> \n")

    if not len(agency_df.index) == 0:
        textFile.write("<p>Companies that are cited by multiple agencies: ")
        textFile.write(str.format('{0:,.0f}', len(agency_df.index)))  # here
        textFile.write("</p> \n")

    textFile.write("\n")
    textFile.write("\n")


def Proportion_Summary_Block(out_counts, total_ee_violtd, total_bw_atp, total_case_violtn, unique_legalname, agency_df, 
                             YEAR_START, YEAR_END, OPEN_CASES, target_jurisdition, TARGET_INDUSTRY, case_disposition_series, textFile, bug_log_csv):

    if not len(out_counts.index) == 0:
        textFile.write("\n")
        textFile.write("\n")

        textFile.write("<p>")
        textFile.write("Number and proportion of wage theft -- ")
    
        #variables
        total_number_of_cases = str.format('{0:,.0f}', len(case_disposition_series ) )
        
        report_type = "cases"
        if OPEN_CASES:
            report_type = "judgments"
        formated_start = YEAR_START.strftime("%m/%Y")

        #TEXT
        textFile.write(f" out of all {total_number_of_cases} wage theft {report_type} against \
                {TARGET_INDUSTRY[0][0]} industry companies in {target_jurisdition} \
                from {formated_start} to the present:")
        textFile.write("</p> \n")

        textFile.write("<ul>")

        case_disposition_series = case_disposition_series.value_counts()
        
        count = 0
        cutoff_size = 1
        for n in case_disposition_series:
            test_spot = case_disposition_series.index[count] #len(test_spot)
            if (len(test_spot) < 3):
                test_spot = '<Not Defined>'
            if n > cutoff_size:
                textFile.write(f"<li>{n} are {test_spot} \
                    ({str.format('{0:,.0%}', float(n)/float(total_number_of_cases.replace(',','')))})</li>")
                textFile.write("\n")
            count = (count +1)
            
            #textFile.write(f"<li>{n} are on {case_disposition_series.index[count] } </li>")

        textFile.write(f"<li><i>*disposition types with less than {cutoff_size + 1} records are not listed</i></li>")
        
        textFile.write("</ul>")

        
        
        textFile.write("\n")
        textFile.write("\n")





def file_path(relative_path):
    if os.path.isabs(relative_path):
        return relative_path
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

