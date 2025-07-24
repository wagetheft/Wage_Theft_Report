
import os


def debug_fileSetup_def(bug_log, LOGBUG = True):
    if LOGBUG:
        bugFile = open(bug_log, 'w')
        bugFile.write("<!DOCTYPE html>")
        bugFile.write("\n")
        bugFile.write("<html><body>")
        bugFile.write("\n")

        bugFile.write("<h1>START</h1>")
        bugFile.write("\n")
        bugFile.close()


def debug_fileClose_def(bug_log, LOGBUG = True):
    if LOGBUG:
        bugFile = open(bug_log, 'a')

        bugFile.write("<h1>DONE</h1>")
        bugFile.write("\n")
        bugFile.write("</html></body>")
        bugFile.write("\n")
        bugFile.close()


def file_path(relative_path):
    if os.path.isabs(relative_path):
        return relative_path
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


def append_log(bug_log, text = "", LOGBUG = True):
    if LOGBUG:
        bugFile = open(bug_log, 'a')
        bugFile.write("\n")
        bugFile.write(text)
        bugFile.write("\n")
        bugFile.close()
        print(text)


