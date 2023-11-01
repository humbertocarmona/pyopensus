import datetime as dt

# -- set path for R (need to make it more consistent)
import os
os.environ["R_HOME"] = os.path.join(os.environ["LOCALAPPDATA"], "Programs", "R", "R-4.3.1") 

import rpy2
import rpy2.robjects as robjects
from rpy2.robjects.packages import importr

from rpy2.robjects.packages import PackageNotInstalledError

try:
    read_dbc = importr('read.dbc')
except PackageNotInstalledError as err:
    raise Exception("Need to install package from rpy2")

# -- functions

def dbc2dbf(path_to_dbc, path_to_dbf):
    read_dbc.dbc2dbf(path_to_dbc, path_to_dbf)


# -- retrieval specifics
def retrieve_siasih(baseftp, dest, uf, year, preffix, to_dbf, verbose):
    '''
    
    '''
    # -- validate year
    this_year = dt.date.today().year
    if year < 2008 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')

    year_str = f'{year}'[2:]
    filename = f"{preffix}{uf.upper()}{year_str}"
    month_lst = ['01', '02', '03', '04', '05', '06', 
                 '07', '08', '09', '10', '11', '12']
    
    for cur_month in month_lst:
        filename_dbc = f'{filename}{cur_month}.dbc'
        filename_dbf = f'{filename}{cur_month}.dbf'
        if verbose:
            print(f'Download do arquivo {filename_dbc} ...', end='')
            
        with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
            baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)
                
        # -- conversion to DBF
        if to_dbf:
            path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
            path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
            dbc2dbf(path_to_dbc, path_to_dbf)
            
        # --
        if verbose:
            print(' Feito.')

def retrieve_vital(baseftp, dest, origin, uf, year, to_dbf, verbose):
    '''
    
    '''
    # -- validate year
    this_year = dt.date.today().year
    if year < 1996 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')
    
    year_str = f'{year}'
    if origin.lower()=='sim':
        filename = f"DO{uf.upper()}{year_str}"
    elif origin.lower()=='sinasc':
        filename = f"DN{uf.upper()}{year_str}"

    filename_dbc = f'{filename}.dbc'
    filename_dbf = f'{filename}.dbf'

    if verbose:
        print(f'Download do arquivo {filename_dbc} ...', end='')
    
    with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
        baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)

    # -- conversion to DBF
    if to_dbf:
        path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
        path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
        dbc2dbf(path_to_dbc, path_to_dbf)

    # --
    if verbose:
        print(' Feito.')
