# downloadERA5

ERA5 Download program.

To run this program type
```python
python downloadERA5

```

and set the parameters to download ERA5 in the _params.txt_ file. The variables _variable_, _year_, _month_, _day_, _time_ can be defined as strings or numbers (_int_) separated with comma **(,)** and each variable is terminated with semi-colon **(;)**.
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

_frequency_ can be set to _year_, _month_, _day_ or _hour_. While _stat_ can be set to _instantaneous_, _mean_ or _accumulated_.

This program requires CDO (_Climate Data Operator_) to be installed. See https://code.mpimet.mpg.de/projects/cdo/wiki or install it typing (in _Linux_):

```bash
sudo apt install cdo

```

