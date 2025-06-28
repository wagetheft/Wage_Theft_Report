
import os


def debug_fileSetup_def(bug_filename):

    bug_filename.write("<!DOCTYPE html>")
    bug_filename.write("\n")
    bug_filename.write("<html><body>")
    bug_filename.write("\n")

    bug_filename.write("<h1>START</h1>")
    bug_filename.write("\n")


def file_path(relative_path):
    if os.path.isabs(relative_path):
        return relative_path
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


def save_backup_to_folder(df_csv, out_file_report_name = 'backup_', out_file_report_path = ""):
    from os.path import exists
    import os

    if out_file_report_path == "": out_file_report_path = out_file_report_path + '_backup/' #defualt folder name
    script_dir = os.path.dirname(os.path.dirname(__file__))
    abs_path = os.path.join(script_dir, out_file_report_path)
    if not os.path.exists(abs_path):  # create folder if necessary
        os.makedirs(abs_path)
    file_type = '.csv'
    
    file_name_backup = os.path.join(abs_path, (out_file_report_name).replace('/', '') + file_type)  # <-- absolute dir and file name
    df_csv.to_csv(file_name_backup) #uncomment for testing


def append_log(bug_log, LOGBUG, text):
    if LOGBUG:
        bugFile = open(bug_log, 'a')
        bugFile.write(text)
        bugFile.close()
        print(text)


