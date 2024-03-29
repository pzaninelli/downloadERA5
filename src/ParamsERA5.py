#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 31 16:11:07 2022

@author: pzaninelli
"""

from configparser import ConfigParser
from os.path import exists as path_exists
import numpy as np

class ConfigParserERA5(ConfigParser):
    
    def __init__(self, filename):
        super(ConfigParserERA5,self).__init__()
        if not path_exists(filename):
            raise AttributeError(f"{filename} does not exist!")
        else:
            self.read(filename)
    def update_sect(self,section = "PARAMETERS"):
        out = {}
        for key, value in self[section].items():
            if value == "":
                out[key] = None
            else:
                out[key] = value
        return out
        

class ParamsERA5:
    
    
    def __init__(self,
                 dataset_name=None, 
                              product_type = None,
                              var = None, 
                              year = None,
                              month = None,
                              day = None,
                              time = None,
                              pressure_level=None,
                              grid=None,
                              area=None,
                              stat=None,
                              freq=None,
                              filename=None,
                              outdir=None,
                              concat_files=False,
                              ncpu=None,
                              rem_tempfiles=False):
        self._dataset_name = dataset_name
        self._product_type = product_type
        self._var = var 
        self._year = year
        self._month = month 
        self._day = day 
        self._time = time 
        self._pressure_level = pressure_level 
        self._grid = grid 
        self._area = area 
        self._stat = stat 
        self._freq = freq 
        self._filename=filename
        if not path_exists(outdir):
            raise FileExistsError(f"{outdir} does not exist!")
        self._outdir = outdir 
        self._concat_files=concat_files
        self._ncpu=ncpu
        self._rem_tempfiles=rem_tempfiles
        
    @property
    def variable(self):
         return self._var
         
    @property  
    def dataset(self):
         return self._dataset_name
      
    @property  
    def product_type(self):
         return self._product_type 
         
    @property
    def year(self):
         return self._year
         
    @property
    def month(self):
         return self._month
         
    @property
    def day(self):
         return self._day
         
    @property
    def time(self):
         return self._time
         
    @property 
    def pressure_level(self):
         return self._pressure_level
         
    @property 
    def grid(self):
         return self._grid
         
    @property 
    def area(self):
         return self._area
         
    @property 
    def statistic(self):
         return self._stat
         
    @property 
    def frequency(self):
         return self._freq
     
    @property  
    def filename(self):
        return self._filename
    
    @property
    def concat_files(self):
        return self._concat_files
      
    @property  
    def out_dir(self):
        return self._outdir
    
    @property
    def ncpu(self):
        return self._ncpu
    
    @property
    def remove_tempfiles(self):
        return self._rem_tempfiles
    
    def __repr__(self):
            return f"Era5Process(dataset_name={self._dataset_name}, " \
                         f"product_type={self._product_type}, "\
                         f"var={self._var}, " \
                         f"year={self._year}, "\
                         f"month={self._month}, "\
                         f"day={self._day}, "\
                         f"time={self._time}, "\
                         f"pressure_level={self._pressure_level}, "\
                         f"grid={self._grid}, "\
                         f"area={self._area}, "\
                         f"stat={self._stat} , "\
                         f"freq={self._freq}, "\
                         f"out_dir={self._outdir}, "\
                         f"concat_files={self._concat_files}, "\
                         f"ncpu={self._ncpu}, "\
                         f"remove_tempfiles={self._rem_tempfiles})"
                         
    @staticmethod
    def arg_options(char,time = False):
        if not char == None:
            if not time:
                if char.find(":")==-1:
                    return char.split(",")
                else:
                    ch = char.split(":")
                    if char.find(":")==char.rfind(":"):
                        return list(map(str,np.arange(int(ch[0]),int(ch[1])+1)))
                    else:
                        return list(map(str, np.arange(int(ch[0]),int(ch[1])+1,int(ch[2]))))
            else:
                if char.find("-")==-1:
                    return char.split(",")
                else:
                    ch = char.split("-")
                    if char.find("-")==char.rfind("-"):
                        return list(map("{:02}:00".format,np.arange(int(ch[0].split(":")[0]),
                                              int(ch[1].split(":")[0])+1)))
                    else:
                        return list(map("{:02}:00".format,np.arange(int(ch[0].split(":")[0]), 
                                              int(ch[1].split(":")[0]), 
                                              int(ch[2].split(":")[0])+1)))
        else:
             return None           
        
            
    @classmethod
    def from_file(cls, filepath):
        """
        Function to load parameters from externar '.ini' file

        Parameters
        ----------
        filepath : '.ini' File
            File with the parameters information. 
            See https://docs.python.org/3/library/configparser.html

        Returns
        -------
        ParamsInit object
            
        """
        if not path_exists(filepath):
            raise AttributeError(f"the initial parameters file '{filepath}' does not exist!")
        config = ConfigParserERA5(filepath)
        params = config.update_sect()
        if params["filename"] == "":
            Filename = None
        else:
            Filename = params["filename"]
        if params["ncpus"] == "":
            ncpus = None
        else:
            ncpus = params["ncpus"]
            
        return cls(params["datasetname"],
                   params["producttype"],
                   params["variable"].split(","),
                   cls.arg_options(params["year"]),
                   cls.arg_options(params["month"]),
                   cls.arg_options(params["day"]),
                   cls.arg_options(params["time"],time=True),
                   cls.arg_options(params["pressurelevel"]),
                   params["grid"].split(","),
                   params["area"].split(","),
                   params["statistic"],
                   params["frequency"],
                   Filename,
                   params["outdir"],
                   config.getboolean('PARAMETERS',"concatenate_files"),
                   ncpus,
                   config.getboolean('PARAMETERS',"remove_temporal_files")
                   )
    
if __name__ == "__main__":
    parameters = ParamsERA5.from_file("./text/params.ini")
    print(parameters)