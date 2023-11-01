# -- set path for R (need to make it more consistent)
import os
os.environ["R_HOME"] = os.path.join(os.environ["LOCALAPPDATA"], "Programs", "R", "R-4.3.1") 

import rpy2
import rpy2.robjects as robjects
from rpy2.robjects.packages import importr

utils = importr("utils")

try:
    read_dbc = importr('read.dbc')
except rpy2.errors.nopackage as err:
    raise Exception("Need to install package from rpy2")

def dbc2dbf(path_to_dbc, path_to_dbf):
    read_dbc.dbc2dbf(path_to_dbc, path_to_dbf)