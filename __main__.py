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
from functools import partial
import subprocess as subp
from os.path import exists as file_exists

from src.Era5Process import Era5Process, ERA5PBuilder 
from src.read_params_from_file import get_params_text 
from src.ParamsERA5 import ParamsERA5
from src.cdoProcess import concat_cdo
from optparse import OptionParser,OptionGroup
parser = OptionParser(usage="usage: %prog  [options] ",\
                      version='%prog v0.0.3')
    
# general options
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")
# groupal options
query_opts=OptionGroup(parser,'Query Options',"These options control the query mode")

# file in to take the parameters
query_opts.add_option("-f", "--filein", dest="file", action="store",
    default="parameters/params.ini", help=".ini file to take the parameters")

# # timeout  
# query_opts.add_option("-t", "--timeout", dest="timeout", action="store",
#     default=60*60*2, help="Waiting time in seconds to stop the process")

parser.add_option_group(query_opts)
(options, args) = parser.parse_args()


os.chdir(os.path.dirname(__file__)) # change dir to the current file

    
def downloadERA5_params_from_ini(_params_ini_file = options.file):
    if not file_exists(_params_ini_file):
        raise FileExistsError(f"{_params_ini_file} does not exist!")
    params = ParamsERA5.from_file(_params_ini_file)
    supp_options= dict(
        concat_files = params.concat_files,
        ncpus = params.ncpu,
        rem_tempfiles = params.remove_tempfiles
    )
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
                 dirout = params.out_dir), supp_options
        
def check_completed_task(Files: list)-> bool:
    return all(file_exists(ifile) for ifile in Files)
                    
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
    
    _Download, supp_options = downloadERA5_params_from_ini()
    # TIMEOUT = options.timeout # wait 2 hours
    print(_Download)
    should_continue = confirmation()    
    if not should_continue:
        sys.exit(0)       
    
    years = _Download.year
    era5b = ERA5PBuilder(_Download)
    download_function = era5b.era5builder()
    if supp_options['ncpus'] is None:
        Ncpus = mp.cpu_count()
    else:
        Ncpus = int(supp_options['ncpus'])
    pool = mp.Pool(Ncpus)
    print(supp_options["rem_tempfiles"])
    if not _Download.frequency == 'hour':
        Download_func = partial(download_function, remove_file = supp_options["rem_tempfiles"])
    else:
        Download_func = download_function
    
    files = pool.map(Download_func, years)
    pool.close()
    pool.join()
    
    if not _Download.frequency == 'hour':
        Files = [ifile[1] for ifile in files]
    else:
        Files = [ifile[0] for ifile in files]

    if check_completed_task(Files):
        print("##### FILES DOWNLOADED! #####\n")

    if supp_options['concat_files'] and len(Files)>1:
        concat_cdo(Files, _Download.filename, remove_tempfiles=supp_options["rem_tempfiles"], verbose=options.verbose)
        if file_exists(_Download.filename):
            print(f"*** Concatenated file: {_Download.filename} ***\n")
        else:
            print("No concatenation file has been created\n")
    

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))