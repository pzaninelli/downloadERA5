#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 09:10:43 2022
Main program of 'donwloadERA5'
@author: Pablo G. Zaninelli
"""

import os
import sys
from copy import deepcopy
import multiprocessing as mp
import time
from src.Era5Process import * 
from src.read_params_from_file import get_params_text 
from src.ParamsERA5 import *
from optparse import OptionParser,OptionGroup
parser = OptionParser(usage="usage: %prog  [options] ",\
                      version='%prog v0.0.1')
    
# general options
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")
# groupal options
query_opts=OptionGroup(parser,'Query Options',"These options control the query mode")

# file in to take the parameters
query_opts.add_option("-f", "--filein", dest="file", action="store",
    default="text/params.ini", help=".ini file to take the parameters")

# timeout  
query_opts.add_option("-t", "--timeout", dest="timeout", action="store",
    default=60*60*2, help="Waiting time in seconds to stop the process")

# timeout  
query_opts.add_option("-y", "--byyears", dest="byyears", action="store_true",
    default=False, help="Start process donwloading by years?")

parser.add_option_group(query_opts)
(options, args) = parser.parse_args()


os.chdir(os.path.dirname(__file__)) # change dir to the current file


def is_installed_CDO():
    is_installed_cdo = subp.run(['which','cdo'],stdout=subp.PIPE, \
                                stderr=subp.PIPE)
    return is_installed_cdo.stdout.decode('utf-8') != ''

def downloadERA5_params_from_text(_params_text_file='./text/params.txt'):
    params = get_params_text(_params_text_file)
    _Download = Era5Process(dataset_name=params['dataset'], 
                 product_type = params['product_type'], 
                 var = params['variable'], 
                 year = params['year'], 
                 month = params['month'], 
                 day = params['day'], 
                 time = params['time'], 
                 pressure_level=params['pressure_level'], 
                 grid=params['grid'], 
                 area=params['area'], 
                 stat=params['stat'], 
                 freq=params['frequency'])
    return _Download
    
def downloadERA5_params_from_ini(_params_ini_file = options.file):
    assert path_exists(_params_ini_file), "ERROR:: File .ini does not exist!!"
    params = ParamsERA5.from_file(_params_ini_file)
    return Era5Process(dataset_name=params.dataset, 
                 product_type = params.product_type, 
                 var = params.variable, 
                 year = params.year, 
                 month = params.month, 
                 day = params.day, 
                 time = params.time, 
                 pressure_level=params.pressure_level, 
                 grid=params.grid, 
                 area=params.area, 
                 stat=params.statistic, 
                 freq=params.frequency,
                 filename=params.filename,
                 dirout = params.out_dir)
        
def run_era5_process(ERA5obj):
    assert isinstance(ERA5obj, Era5Process), "Must be an Era5Process object!"
    ERA5obj.run()
    
def set_years(ERA5obj,year):
    assert isinstance(ERA5obj, Era5Process), "Must be an Era5Process object!"
    ERA5obj.year = [year]
    
def options_app(ERA5obj,options):
    return ERA5Process.byobj(ERA5obj, byyears=options.byyears)
        
def split_era5_process(ERA5obj):
    ERA5obj_c = ERA5obj
    filename = ERA5obj_c.filename # copy the file name
    years = ERA5obj_c.year
    times = 0
    print(f"Process will be split in years {years}\n")
    ERA5_l = []
    for iy in years:
        print(f"Start with year {iy}")
        set_years(ERA5obj, iy)
        print(ERA5obj_c)
        ERA5_l.append(deepcopy(ERA5obj_c))
    return ERA5_l
                 
def confirmation():
    should_continue = False
    count = 1
    while True:
        if count == 10:
            print("Run the script again!\n")
            break
        option = str(input("Is your request OK: type yes[Y/y] or cancel[C/c]: "))
        if option.upper()=='Y':
            print("Starting process...\n")
            should_continue = True
            break
        elif option.upper()=='C':
            print("Process stopped!")
            break
        else:
            print("Incorrect option\n")
            continue
        count += 1
    return should_continue

def main():
    assert is_installed_CDO(), """
    CDO is not installed!!
        type 'sudo apt install cdo' for Debian, Ubuntu
        or visit https://code.mpimet.mpg.de/projects/cdo/files
        """
    _Download = downloadERA5_params_from_ini()
    _Download = options_app(_Download,options)
    TIMEOUT = options.timeout # wait 2 hours
    print(_Download)
    should_continue = confirmation()    
    if not should_continue:
        sys.exit(0)
    if not _Download.byyears:
        p = mp.Process(target=run_era5_process, name = "run_era5_process", args= (_Download,))
        p.start()
        p.join(TIMEOUT)        
        if p.is_alive():
            print("Killing the process...\n")
            p.terminate()
            p.join()
            filename = _Download.filename # copy the file name
            if file_exists(filename):
                print(f"{filename} was found, so this will be removed!")
                subp.run(['rm',filename], stdout=subp.PIPE, stderr=subp.PIPE)
                split_era5_process(_Download)      
            else:
                if not file_exists(_Download.filename):
                    print(f"ERROR:: {_Download.filename} was not downloaded\n")
                    # print("Start process to download year by year and concatenate with CDO\n")
                    p.terminate()
                    p.join()
                    split_era5_process(deepcopy(_Download))
    else:        
        print("Download will be done on a yearly basis")
        ERA5_l = split_era5_process(deepcopy(_Download))
        pool = mp.Pool(mp.cpu_count())
        results = pool.map(run_era5_process, ERA5_l)
        pool.close()
        pool.join()

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))