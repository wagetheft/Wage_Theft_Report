
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


def append_log(bug_log, LOGBUG, text):
    if LOGBUG:
        bugFile = open(bug_log, 'a')
        bugFile.write(text)
        bugFile.close()
        print(text)


