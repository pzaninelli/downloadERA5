#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 15:10:08 2022
Load parameters for CDSAPI Copernicus 
from text file
@author: pzaninelli
"""
import re

def convert2list(params):
    # for iv in ['area', 'variable','grid']:
    #     params[iv]=ast.literal_eval('['+params[iv]+']')
    for iname,ivalue in params.items():
        params[iname] = params[iname].replace("'", '')
        # params[iname] = params[iname].split(',')
    for iv in ['year', 'month','day','pressure_level']:
        if re.match(r'[0-9]+\:[0-9]+$', params[iv]):
            n1, n2 = params[iv].split(':')
            n1=int(n1); n2=int(n2);
            if n1>n2:
                raise ValueError(f"ERROR:: parameters in {iv} must be minor-major sequence\n")
            if iv=='year' or iv=='pressure_level':
                params[iv] = [str(inum) for inum in range(n1,n2+1)]
            if iv=='month':
                params[iv] = ["{:02}".format(inum) for inum in range(n1,n2+1)]
            if iv=='day':
                params[iv] = ["{:02}".format(inum) for inum in range(n1,n2+1)]
        else:
            params[iv]=params[iv].split(',')
    if re.match(r"[0-9]+\:00\-[0-9]+\:00$",params['time']) or re.match(r"[0-9]+\:00(,[0-9]+\:00)*$",params['time']):
        if re.match(r"[0-9]+\:00\-[0-9]+\:00$",params['time']):
            n1 = params['time'].split('-')[0].split(':')[0]
            n2 = params['time'].split('-')[1].split(':')[0]
            n1 = n1.replace("'",""); n2 = n2.replace("'", "");
            assert int(n1)>=0 and int(n1)<=23, "ERROR:: hour must be between 0:00 and 23:00!" 
            assert int(n2)>=0 and int(n2)<=23, "ERROR:: hour must be between 0:00 and 23:00!" 
            params['time'] = ["{:02}:00".format(num) for num in range(int(n1),int(n2)+1)]
        elif re.match(r"[0-9]+\:00(,[0-9]+\:00)*$",params['time']):
            params['time']=params['time'].split(',')
    else:
        raise ValueError("ERROR:: Time format incorrect!!\n")
    for iv in ['variable', 'area', 'grid']:
        params[iv] = params[iv].split(',')
    return params
# initial check for download parameters and post computation
def initalCheck(params):
    categories=['dataset', 'product_type', 'levtype', 'variable','year','month',\
                'day','time','area','frequency','stat','format','pressure_level',\
                    'grid']
    cont=0
    for name,value in params.items():
        if name in categories:
            cont+=1
    if cont==len(categories):
        return True
    else:
        return False
                
# take parameters from the txt file
def get_params_text(filename='./text/params.txt', print_content = False):
    with open(filename,'r') as f:
        content = ''
        for line in f.readlines():
            li = line.lstrip()
            if not li.startswith('#'):
                content += li
        if print_content:
            print(content)
        lines1 = content.replace('\n','').split(';')
        lines1.pop()
        params = {ii.split('=')[0]:re.sub(r"\s+", "",ii.split('=')[1]) \
                   for ii in lines1}
        if not initalCheck(params):
            raise ValueError("ERROR:: Have not been defined all ERA5 donwload parameters!\n")
        
        params = convert2list(params)
        if re.match(r'^(reanalysis-era5-single-levels).*',params['dataset'][0]):
            if params['levtype']=='sfc' and params['pressure_level']==['0']:
                params.pop('pressure_level')
            elif params['pressure_level']==['0'] and params['levtype']!='sfc':
                params['levtype']='sfc'
            else:
                raise ValueError("ERROR:: Check level pressure parameters!\n")
    return params


