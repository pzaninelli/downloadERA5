#!/usr/bin/env python3 
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 08:32:27 2022
@author: Pablo G. Zaninelli
"""

import subprocess as subp
from os.path import exists as file_exists
import pandas as pd
    
class Era5Process:
    """
    Object to donwload ERA5 Reanalysis
    
    Attributes
    ---------
        __STAT: list
            Allowed statistic values 
        __FREQ: list
            Allowed frequencies
        __CDO_STAT: dictionary
            Converter values to CDO commands according to __STAT
        __CDO_FREQ: dictionary
            Converter values to CDO commands according to __FREQ
    
    Methods
    -------
    run()
        donwload ERA5 file
    """
    
    __STAT = ['instantaneous', 'mean', 'accumulated']
    __FREQ = ['hour','day','month','year']
    __CDO_STAT = {'instantaneous':None, "mean":"mean", "accumulated":"sum"}
    __CDO_FREQ = {'hour':'hour', 'day':'day', 'year':'year'}
    
    def __init__(self,dataset_name='reanalysis-era5-single-levels', 
                 product_type = 'reanalysis',
                 var = None, 
                 year = None,
                 month = None,
                 day = None,
                 time = None,
                 pressure_level=['0'],
                 grid=['1.0', '1,0'],
                 area=['90', '-180', '-90', '180'],
                 stat='instantaneous',
                 freq='hour', 
                 filename=None):
        self._dataset_name = dataset_name
        self._product_type = product_type
        if not isinstance(var,list):
            self._var = [var]
        else:
            self._var = var
        if not isinstance(year,list):
            self._year = [year]
        else:
            self._year = year
        if not isinstance(month, list):
            self._month = [month]
        else:
            self._month = month
        if not isinstance(day, list):
            self._day = [day]
        else:
            self._day = day
        if not isinstance(time, list):
            self._time = [time]
        else:
            self._time = time
        if not isinstance(pressure_level, list):
            self._pressure_level = [pressure_level] 
        else:
            self._pressure_level = pressure_level 
        self._grid = grid 
        self._area = area
        if not stat in self.__STAT:
            raise AttributeError(f"ERROR:: 'stat' must be in this list {self.__STAT}")
        else:
            self._stat=stat
        
        if not freq in self.__FREQ:
            raise AttributeError(f"ERROR:: 'freq' must be in this list {self.__FREQ}")
        else:
            self._freq=freq
    
        if self._freq=='hour' and not self._stat=='instantaneous':
            raise ValueError("ERROR:: hourly frequency only is compatible with 'instantaneous' statistics!")
        
        if self._stat=='instantaneous' and not self._freq in ['day','hour']:
            raise ValueError("ERROR:: instantaneous statistic only is allowed with daily or hourly frequency!")
        
        if self._freq=="month" and self._stat=='mean':
            if self._pressure_level==['0']:
                if not self._dataset_name in ["reanalysis-era5-single-levels-monthly-means",\
                                              "reanalysis-era5-land-monthly-means"]:
                    raise AttributeError("ERROR:: Dataset must be 'reanalysis-era5-single-levels-monthly-means' or 'reanalysis-era5-land-monthly-means'")
            elif not self._pressure_level==['0']:
                if not self._dataset_name == 'reanalysis-era5-pressure-levels-monthly-means':
                    raise AttributeError("ERROR:: Dataset must be 'reanalysis-era5-pressure-levels-monthly-means'")
        self._filename = self._set_filename(self._var, self._stat, self._freq, self._year, filename)
        
        self._was_runned = False
        

    def __str__(self):
            return f"""
            ERA5 Donwload object:
                Dataset = {self._dataset_name}
                Product Type = {self._product_type}
                Variable = {self._var}
                Year = {self._year}
                Month = {self._month}
                Day = {self._day}
                Time = {self._time}
                Pressure levels = {self._pressure_level}
                Grid = {self._grid}
                Area (ºN ºW ºS ºE) = {self._area}
                Statistic = {self._stat}
                Frequency = {self._freq}
                File Name = {self._filename}"""
            
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
                         f"filename={self._filename})"
                         
    
    def __eq__(self, o):
        is_equal = dict(equal_dataset_name = self._dataset_name == o._dataset_name,
        equal_product_type = self._product_type == o._product_type,
        equal_var = self._var == o._var,
        equal_year = self._year == o._year,
        equal_month = self._month == o._month,
        equal_day = self._day == o._day,
        equal_time = self._time == o._time,
        equal_pressure_level = self._pressure_level == o._pressure_level,
        equal_grid = self._grid == o._grid,
        equal_area = self._area == o._area,
        equal_stat = self._stat == o._stat,
        equal_freq = self._freq == o._freq)
        return all(is_equal.values()) 
    
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

    @year.setter
    def year(self,new_year_l):
        assert isinstance(new_year_l, list), "ERROR::'new_year_l' must be a list!\n"
        self._year = new_year_l
        self._filename = self._set_filename(self._var, self._stat, self._freq, new_year_l,filename = None)
        self._was_runned = False
        
    def Was_runned(self):
        return self._was_runned
    
    def dates(self):
        _dates = []
        if self._freq=='hour':
            for iy in self._year:
                for im in self._month:
                    for iday in self._day:
                        for it in self._time:
                            _dates.append(pd.to_datetime(iy+'-'+im+'-'+iday+' '+it,\
                                                         format = "%Y-%m-%d %H:%M", errors = 'coerce'))
                            
        elif self._freq == 'day':
             for iy in self._year:
                for im in self._month:
                    for iday in self._day:
                        _dates.append(pd.to_datetime(iy+'-'+im+'-'+iday+' 00:00',\
                                                     format = "%Y-%m-%d %H:%M", errors = 'coerce'))
                        
        elif self._freq == 'month':
            for iy in self._year:
               for im in self._month:
                   _dates.append(pd.to_datetime(iy+'-'+im+'-15 00:00',\
                                                format = "%Y-%m-%d %H:%M", errors = 'coerce'))
                   
        elif self._freq == 'year':
            for iy in self._year:
                _dates.append(pd.to_datetime(iy+'-06-15 00:00',\
                                             format = "%Y-%m-%d %H:%M", errors = 'coerce'))
                    
        return [ix for ix in _dates if not isinstance(ix,pd._libs.tslibs.nattype.NaTType)]
    
    def run(self,rm_temporal=True):
        assert isinstance(self._year, list), "ERROR:: 'year' variable must be a list\n"
         
        if self._freq in ["hour", 'month'] and self._stat == "mean":
            self._get_era5(dataset_name = self._dataset_name,
                      product_type= self._product_type,
                      var=self._var, 
                      year = self._year,
                      month = self._month,
                      day = self._day,
                      time = self._time,
                      pressure_level=self._pressure_level,
                      grid=self._grid,
                      area=self._area,
                      download_file='./'+ self._filename)
            if not file_exists('./' + self._filename):
                raise ValueError(f"ERROR:: File {self._filename} does not exist!")
            else:
                print(f"{self._filename} was downloaded!")
                self._was_runned = True
        elif self._freq in ['hour', 'day'] and self._stat == "instantaneous":
            self._get_era5(dataset_name = self._dataset_name,
                      product_type= self._product_type,
                      var=self._var, 
                      year = self._year,
                      month = self._month,
                      day = self._day,
                      time = self._time,
                      pressure_level=self._pressure_level,
                      grid=self._grid,
                      area=self._area,
                      download_file='./'+ self._filename)
            if not file_exists('./' + self._filename):
                raise ValueError(f"ERROR:: File {self._filename} does not exist!")
            else:
                print(f"{self._filename} was downloaded!")
                self._was_runned = True
        else:
            temp_name = f"./{self._var}_ERA5_temp_{self._year[0]}-{self._year[-1]}.nc"
            if len(self._time)==1:
                raise ValueError("ERROR:: 'time' variable must have more than one element to get mean or accumulated!")
            print(f"Downloading {temp_name} as temporal file to process with CDO...\n")    
            self._get_era5(dataset_name = self._dataset_name,
                      product_type= self._product_type,
                      var=self._var, 
                      year = self._year,
                      month = self._month,
                      day = self._day,
                      time = self._time,
                      pressure_level=self._pressure_level,
                      grid=self._grid,
                      area=self._area,
                      download_file='./'+ temp_name)
            if not file_exists('./' + temp_name):
                raise ValueError(f"ERROR:: File {temp_name} for frequency {self._freq} does not exist!")
            else:
                print(f"{temp_name} was downloaded!")
            print(f"Computing '{self.__CDO_FREQ[self._freq] + self.__CDO_STAT[self._stat]}' with CDO on {temp_name}...\n")
            cmd = ['cdo','-b','32',self.__CDO_FREQ[self._freq]+self.__CDO_STAT[self._stat],temp_name, self._filename]
            print(cmd)
            proc = subp.run(cmd,stdout=subp.PIPE,stderr=subp.PIPE)
            print(f"""
                  STDOUT:
                      {proc.stdout.decode('utf-8')}
                  STDERR:
                      {proc.stderr.decode('utf-8')}
                  """)
            if not file_exists('./' + self._filename):
                raise ValueError(f"ERROR:: Problems with CDO computing the {self._stat} in {self._freq}!")
            else:
                print(f"{self._filename} was computed!")
                self._was_runned = True
                if rm_temporal:
                    subp.run(['rm',temp_name], stdout = subp.PIPE,stderr=subp.PIPE)
                    if file_exists(temp_name):
                        raise ValueError(f"ERROR:: {temp_name} has not been deleted!")
                    else:
                        print(f"{temp_name} has been deleted!")
    @staticmethod
    def _set_filename(var,stat,freq,year,filename):
        if filename == None:
            if len(var)==1:
                var_fname = f"{var[0]}"
            else:
                var_fname = "variables"
            if not len(year)==1:
                year_fname = f"{year[0]}-{year[-1]}"
            else:
                year_fname = f"{year[0]}"
                
            Filename = f"./{var_fname}_ERA5_{freq.upper()}_{stat.upper()}_{year_fname}.nc"
        elif filename != None and file_exists(filename):
            raise AttributeError("ERROR:: Output file name {filename} already exists!")
        else:
            assert isinstance(filename, str), "'filename' must be an string!"
            Filename = filename
        return Filename
    
    @staticmethod
    def _get_era5(dataset_name='reanalysis-era5-single-levels', 
                     product_type = "reanalysis",
                     var=None, 
                     year = None,
                     month = None,
                     day = None,
                     time = None,
                     pressure_level=None,
                     grid=[1.0, 1,0],
                     area=[90, -180, -90, 180],
                     download_file='./output.nc'):
        import cdsapi, sys
        import numpy as np
        # start the cdsapi client
        c = cdsapi.Client(timeout=600,quiet=False,debug=True)
            
        # parameters
        params = dict(
                format = "netcdf",
                product_type = product_type,
                variable = var,
                grid = grid,
                area = area,
                year = year,
                month = month,
                day = day,
                time = time,
                pressure_level = pressure_level
            )
        # function to check if a list is included in another
        def is_include_list(a,b):
            set_a = set(a)
            set_b = set(b)
            return False if (set_b.intersection(set_a)==set()) else True
        # test if acceptable pressure level
        acceptable_pressures = [0,1, 2, 3, 5, 7, 10, 20, 30, 50, 70] + list(np.arange(100, 1000, 25))
        if not is_include_list(params['pressure_level'], [str(lev) for lev in acceptable_pressures]):
            sys.stderr.write(f"ERROR:: Pressure level must be in this list: {acceptable_pressures}\n")
            sys.exit(100)
        if params['pressure_level'] == ['0']:
            _ = params.pop('pressure_level')
        # what to do if asking for monthly means
        if dataset_name in ["reanalysis-era5-single-levels-monthly-means", 
                                "reanalysis-era5-pressure-levels-monthly-means",
                                "reanalysis-era5-land-monthly-means"]:
            params["product_type"] = "monthly_averaged_reanalysis"
            _ = params.pop("day")
            params["time"] = "00:00"
                
        # product_type not needed for era5_land
        if dataset_name in ["reanalysis-era5-land"]:
                _ = params.pop("product_type")
                 
            # file object
        fl=c.retrieve(dataset_name, params)
        fl.download(f"{download_file}")
    
class ERA5Process(Era5Process):
    
    def __init__(self,
                 dataset_name='reanalysis-era5-single-levels', 
                              product_type = 'reanalysis',
                              var = None, 
                              year = None,
                              month = None,
                              day = None,
                              time = None,
                              pressure_level=['0'],
                              grid=['1.0', '1,0'],
                              area=['90', '-180', '-90', '180'],
                              stat='instantaneous',
                              freq='hour', 
                              filename=None,
                              byyears = False):
        super().__init__(dataset_name, product_type,var,year,month,day,time, pressure_level,
                     grid, area, stat,freq,filename)
        assert isinstance(byyears,bool), "'byyears' must be boolean!"
        self._byyears = byyears
        
    def __str__(self):
        return f"""
                {super().__str__()}
                Donwload by years? = {self._byyears}
                """
    @property
    def byyears(self):
         return self._byyears
        
    @classmethod 
    def byobj(cls, Era5obj, byyears):
        assert isinstance(Era5obj, Era5Process), "'Era5obj' must be a Era5Process object"
        return cls(Era5obj.dataset, Era5obj.product_type,Era5obj.variable,
                   Era5obj.year,Era5obj.month,Era5obj.day,Era5obj.time, 
                   Era5obj.pressure_level, Era5obj.grid, Era5obj.area,
                   Era5obj.statistic, Era5obj.frequency,Era5obj.filename, 
                   byyears)
    
if __name__ == "__main__":
    mydownload = Era5Process(dataset_name='reanalysis-era5-single-levels', \
                 var = '2m_temperature', \
                 year = ['1990'],\
                 month = ["{:02}".format(num) for num in range(1,13)],\
                 day = ["{:02}".format(num) for num in range(1,32)],\
                 time = ['00:00', '06:00', '12:00', '18:00'],\
                 pressure_level=['0'],\
                 grid=['0.5', '0.5'],\
                 area=['90', '-180', '-90', '180'],\
                 stat= 'accumulated',\
                 freq='day')
        
    myDownload = ERA5Process.byobj(mydownload, True)
    print(myDownload)
    # mydownload.run(rm_temporal=False)