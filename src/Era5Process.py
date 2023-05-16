
"""
Created on Sat Mar 12 08:32:27 2022
@author: Pablo G. Zaninelli
"""

from os.path import exists as file_exists
import pandas as pd
import os
from functools import partial
    
from src.cdoProcess import CDOProcess, concat_cdo
    
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
    
    __STAT = ['instantaneous', 'mean', 'accumulated', 'max', 'min', 
              'average', 'range', 'std', 'std1', 'var', 'var1']
    __FREQ = ['hour', 'day', 'month', 'season', 'year']
    __CDO_STAT = {'instantaneous': None, "mean": "mean", "accumulated": "sum", "max": "max", "min": "min",
                  'average': 'avg', 'range': 'range','std':'std', 'std1':'std1', 'var':'var', 'var1':'var1'}
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
                 filename=None,
                 dirout = None):
        
        
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
            raise ValueError("ERROR:: hourly frequency only is compatible with 'instantaneous' values!")
        
        if not self._freq=='hour' and self._stat=='instantaneous':
            raise ValueError("ERROR:: 'instantaneous' values are only compatible with hourly data!")
        
        self._dirout = dirout
        
        self._was_runned = False
        
        self._filename = self._set_filename(self._var, 
                                            self._stat, 
                                            self._freq, 
                                            self._year, 
                                            self._month,
                                            self._day,
                                            filename, 
                                            self._dirout)
        
        
        
        
        

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
                File Name = {self._filename}
                Directory output = {self._dirout}"""
            
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
    
    @property
    def out_dir(self):
        return self._dirout
    
    def _check_file(self):
        if not file_exists(self._filename):
                raise ValueError(f"ERROR:: File {self._filename} does not exist!")
        else:
                print(f"{self._filename} was downloaded!")
                self._was_runned = True
    
    @year.setter
    def year(self,new_year_l):
        assert isinstance(new_year_l, list), "ERROR::'new_year_l' must be a list!\n"
        self._year = new_year_l
        self._filename = self._set_filename(self._var, 
                                            self._stat, 
                                            self._freq, 
                                            new_year_l, 
                                            self._month, 
                                            self._day, 
                                            filename = None,
                                            dirout=self.out_dir)
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
    # TODO: Fix invalid date problem
    @staticmethod
    def _set_filename(var, stat, freq, year, month, day, filename, dirout):
        assert dirout is not None, "dirout was not defined"
        if filename is None:
            var_fname = '_'.join(var)
            date_fname = year[0] + '-' + month[0] + '-' + day[0] + '_' + year[-1] + '-' + month[-1] + '-' + day[-1]
            if dirout.endswith('/'):
                dirout_b=dirout[:-1]
            else:
                dirout_b = dirout
            Filename = f"{dirout_b}/{var_fname}_ERA5_{freq.upper()}_{stat.upper()}_{date_fname}.nc"
        elif filename != None and file_exists(filename):
            raise AttributeError(f"ERROR:: Output file name {filename} already exists!")
        else:
            assert isinstance(filename, str), "ERROR:: The name of the file must be an string!"
            Filename = filename
        return Filename
    
    
def _get_era5(year = None,
              dataset_name='reanalysis-era5-single-levels', 
              product_type = "reanalysis",
              var=None, 
              month = None,
              day = None,
              time = None,
              pressure_level=None,
              grid=[1.0, 1.0],
              area=[90, -180, -90, 180],
              download_file='./output.nc',
              ):
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
                              dirout=None,
                              byyears = True):
        super().__init__(dataset_name, product_type,var,year,month,day,time, pressure_level,
                     grid, area, stat,freq,filename,dirout)
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
        assert isinstance(Era5obj, Era5Process), "'Era5obj' must be a Era5Process class"
        return cls(Era5obj.dataset,
                   Era5obj.product_type,
                   Era5obj.variable,
                   Era5obj.year,
                   Era5obj.month,
                   Era5obj.day,
                   Era5obj.time, 
                   Era5obj.pressure_level, 
                   Era5obj.grid, 
                   Era5obj.area,
                   Era5obj.statistic, 
                   Era5obj.frequency,
                   Era5obj.filename,
                   Era5obj.out_dir,
                   byyears)
 
def _cdo_download_era5(era5obj, # cambiar el orden de los argumentos https://stackoverflow.com/questions/26182068/typeerror-got-multiple-values-for-argument-after-applying-functools-partial
              year,
              remove_file) -> tuple:
    download_file = f'{era5obj.out_dir}/{era5obj.variable[0]}_{year}.nc'
    if file_exists(download_file):
        raise ValueError(f"File: {download_file} already exists!")
    _get_era5_p = partial(_get_era5, 
                dataset_name=era5obj.dataset, 
                product_type=era5obj.product_type,
                var=era5obj.variable, 
                month=era5obj.month,
                day=era5obj.day,
                time=era5obj.time,
                pressure_level=era5obj.pressure_level,
                grid=era5obj.grid,
                area=era5obj.area,
                download_file=download_file)    
    _get_era5_p(year)
    fileout=os.path.splitext(download_file)[0] + '_' + era5obj.frequency.upper() + '_' + era5obj.statistic.upper() + '.nc'
    Cdo=CDOProcess(download_file, fileout, era5obj.statistic, era5obj.frequency, remove_file)
    Cdo.run()
    if not Cdo.stdout is None or not Cdo.stderr is None:
        print(f"""
                  STDOUT:
                      {Cdo.stdout}
                  STDERR:
                      {Cdo.stderr}
                  """)
    return download_file, fileout
    
def _download_era5(era5obj,
              year) -> str:
    download_file = f'{era5obj.out_dir}/{era5obj.variable[0]}_{year}_HOUR_INSTANTANEOUS.nc'
    if file_exists(download_file):
        raise ValueError(f"File: {download_file} already exists!")
    _get_era5_p = partial(_get_era5, 
                dataset_name=era5obj.dataset, 
                product_type=era5obj.product_type,
                var=era5obj.variable, 
                month=era5obj.month,
                day=era5obj.day,
                time=era5obj.time,
                pressure_level=era5obj.pressure_level,
                grid=era5obj.grid,
                area=era5obj.area,
                download_file=download_file)    
    _get_era5_p(year)
    return download_file

class ERA5PBuilder:
    
    __ALLOWED_DATASET = ["reanalysis-era5-single-levels", "reanalysis-era5-land", "reanalysis-era5-pressure-levels"]
    
    def __init__(self,
                 Era5c : Era5Process,
                ):
         self._Era5c = Era5c
         if not self._Era5c.dataset in self.__ALLOWED_DATASET:
             raise ValueError(f"Only dataset included in {*self.__ALLOWED_DATASET,} are allowed!")

    def era5builder(self):
        if self._Era5c.frequency in ['day', 'month', 'season', 'year']:
            return partial(_cdo_download_era5, self._Era5c)
        else:
            return partial(_download_era5, self._Era5c)

        
if __name__ == "__main__":
    mydownload = Era5Process(dataset_name='reanalysis-era5-single-levels', \
                 var = '2m_temperature', \
                 year = ['1990', '1991', '1992'],\
                 month = ["{:02}".format(num) for num in range(1,3)],\
                 day = ["{:02}".format(num) for num in range(1,32)],\
                 time = ['06:00','12:00'],\
                 pressure_level=['0'],\
                 grid=['2', '2'],\
                 area=['90', '-180', '-90', '180'],\
                 stat= 'mean',\
                 freq='day',
                 dirout = '/home/pabloz/prueba_era5')
        
    print(mydownload)
    era5b = ERA5PBuilder(mydownload)
    year = mydownload.year
    download_era5 = era5b.era5builder()
    file_orig_l = []
    fileout_l = []
    for iy in year:
        file_orig, fileout = download_era5(iy, remove_file=False)
        file_orig_l.append(file_orig)
        fileout_l.append(fileout)
    concat_cdo(fileout_l, mydownload.filename, verbose=True)
    # mydownload.run(rm_temporal=False)