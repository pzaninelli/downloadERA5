# downloadERA5

ERA5 Download program.

To run this program type
```Bash
python downloadERA5
```

and set the parameters to download ERA5 in the _params.ini_ file. The variables _variable_, _year_, _month_, _day_, _time_ can be defined as strings or numbers (_int_) separated with comma **(,)** and each variable is terminated with semi-colon **(;)**.
_year_, _month_, _day_ and _pressure_level_ can be defined with colon **(:)** and _time_ with middle dash **(-)** for consecutive values.

 Surface levtype **(sfc)** only works with 
 
```
 pressure_level = 0

```

or

```
pressure_level = [0]
```
For now, only **netcdf** format is available.

_frequency_ can be set to _year_, _season_, _month_, _day_ or _hour_. While _stat_ can be set to _instantaneous_, _mean_, _accumulated_, _max_, _min_, _average_, _range_, _std_, _std1_, _var_ and _var1_ (see https://code.mpimet.mpg.de/projects/cdo for more information).

To change default options see

```Bash
python downloadERA5 --help

```
This program requires CDO (_Climate Data Operator_) to be installed. See https://code.mpimet.mpg.de/projects/cdo/wiki or install it typing (in _Linux_):

```Bash
sudo apt install cdo

```
and it is also necessary to have the CDS API key installed. See https://cds.climate.copernicus.eu/api-how-to
