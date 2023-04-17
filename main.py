import os
import sys
import pygui_util

if __name__ == '__main__':
    _source_file_path = os.path.abspath(sys.argv[0]) 
    _source_directory = os.path.dirname(_source_file_path)
    os.chdir(_source_directory)

    pygui_util.pygui_init()
    pygui_util.pygui_destroy()
