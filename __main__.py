#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 09:10:43 2022
Main program of 'donwloadERA5'
@author: Pablo G. Zaninelli
"""

import os
import multiprocessing as mp
from src.Era5Process import * 
from src.read_params_from_file import get_params_text 

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
    
def run_era5_process(ERA5obj):
    assert isinstance(ERA5obj, Era5Process), "Must be an Era5Process object!"
    ERA5obj.run()
    
def set_years(ERA5obj,year):
    assert isinstance(ERA5obj, Era5Process), "Must be an Era5Process object!"
    ERA5obj.year = [year]

def main():
    assert is_installed_CDO(), """
    CDO is not installed!!
        type 'sudo apt install cdo' for Debian, Ubuntu
        or visit https://code.mpimet.mpg.de/projects/cdo/files
        """
    _Download = downloadERA5_params_from_text()
    print(_Download)
    should_continue = False
    count = 1
    TIMEOUT = 60*60*2 # wait 2 hours
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
            print("Process stoped!")
            break
        else:
            print("Incorrect option\n")
            continue
        count += 1
    if should_continue:
        p = mp.Process(target=run_era5_process, name = "run_era5_process", args= (_Download,))
        p.start()
        p.join(TIMEOUT)        
        if p.is_alive():
            print("Killing the process...\n")
            p.terminate()
            p.join()
            filename = _Download.filename # copy the file name
            years = _Download.year
            times = 0
            if file_exists(filename):
                subp.run(['rm',filename], stdout=subp.PIPE, stderr=subp.PIPE)
            for iy in years:
                set_years(_Download, iy)
                times += len(_Download.dates())
                run_era5_process(_Download)
                if iy==years[0] and _Download.Was_runned():
                    subp.run(['cdo','copy',_Download.filename,filename], \
                             stdout=subp.PIPE, stderr=subp.PIPE)
                    if file_exists(filename):
                        subp.run(['rm',_Download.filename],
                                 stdout=subp.PIPE, stderr=subp.PIPE)
                else:
                    subp.run(['cdo','cat',_Download.filename,filename], \
                             stdout=subp.PIPE, stderr=subp.PIPE)
                    cdo_ntime = subp.run(['cdo','ntime',filename], \
                             stdout=subp.PIPE, stderr=subp.PIPE)
                    if times == int(cdo_ntime.stdout.decode('utf-8').replace('\n','')):
                        subp.run(['rm',_Download.filename], \
                                 stdout=subp.PIPE, stderr=subp.PIPE)
                    else:
                         raise ValueError("ERROR:: time steps do not match!!")       
                                

if __name__ == "__main__":
    
    main()