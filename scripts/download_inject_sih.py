'''
    Download SIHSUS data from the northeast Brazilian states and structure the
    data into a database. 
'''

import sys
sys.path.append("..")
from pathlib import Path
import pandas as pd

from pyopensus import Opensus
from pyopensus.storage.whandler_sus import HandlerSIH

basefolder = Path.home().joinpath("Documents", "data", "opendatasus", "sihsus")
warehouse_location = Path.home().joinpath("Documents", "data", "opendatasus")
#warehouse_name = "SIHSUS_NORDESTE_NO_SERVICE.db"
warehouse_name = "SIHSUS_SUDESTE_NO_SERVICE.db"

# -- load the database schema
warehouse_injector = HandlerSIH(warehouse_location, warehouse_name)
opensus = Opensus()

preffix_dict = {
    #"SIHSUS": ["RD", "SP", "RJ"],
    "SIHSUS": ["RD", "RJ"],
}

#uf_list = ["CE", "BA", "PE", "RN", "PI", "PB", "MA", "SE", "AL"]
uf_list = ["SP", "MG", "RJ", "ES"]
origin_list = [ "SIHSUS" ]
yy_list = ['08', '09', '10', '11', '12', '13', '14', 
           '15', '16', '17', '18', '19', '20', '21', 
           '22', '23', '24']
mm_list = [ '01', '02', '03', '04', '05', '06', '07', 
            '08', '09', '10', '11', '12' ]


for origin in origin_list: # -- loop on the database (only SIHSUS)
    for preffix in preffix_dict[origin]: # -- specific table
        for uf in uf_list: # -- Brazilian states to consider
            # -- year and month of downloaded data
            for yy in yy_list:
                for mm in mm_list:
                    fname = f"{preffix}{uf}{yy}{mm}"
                    try:
                        opensus.retrieve_file(basefolder, origin, fname, to_dbf=True, to_parquet=True, verbose=True)                    
                    except:
                        continue

                    df = pd.read_parquet(basefolder.joinpath("PARQUET", f"{fname}.parquet"))
                    if origin=="SIHSUS":
                        warehouse_injector.insert_sih(df, fname, preffix)
                    # -- delete the downloaded files after the injecting the data into the database.
                    # ---- delete DBF file
                    if basefolder.joinpath("DBF", f"{fname}.dbf").is_file():
                        basefolder.joinpath("DBF", f"{fname}.dbf").unlink()
                    # ---- delete DBC file
                    if basefolder.joinpath("DBC", f"{fname}.dbc").is_file():
                        basefolder.joinpath("DBC", f"{fname}.dbc").unlink()
                    # ---- delete PARQUET file
                    if basefolder.joinpath("PARQUET", f"{fname}.parquet").is_file():
                        basefolder.joinpath("PARQUET", f"{fname}.parquet").unlink()

