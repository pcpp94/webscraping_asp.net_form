import os
import sys
sys.path.insert(0,'\\'.join(os.path.dirname(__file__).split('\\')[:-1]))

from src.data.web_scraping import parse_new_data_from_new_to_old

import warnings
warnings.filterwarnings("ignore")

def run_etl():
    parse_new_data_from_new_to_old()
    from src.data.file_compiler import compile_all
    compile_all()

if __name__ == '__main__':
    run_etl()
