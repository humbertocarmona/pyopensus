'''
    Download SIHSUS data from the northeast Brazilian states and structure the
    data into a database. 
'''

import sys
sys.path.append("..")
import subprocess
from pathlib import Path
import pandas as pd
from pyopensus.storage.whandler_sus import HandlerSIH

basefolder = Path.home().joinpath("Documents", "data", "opendatasus", "sihsus")
warehouse_location = Path.home().joinpath("Documents", "data", "opendatasus")
#warehouse_name = "SIHSUS_SUDESTE_NO_SERVICE_V2.db"

# -- load the database schema
#warehouse_injector = HandlerSIH(warehouse_location, warehouse_name)

dbc_location = basefolder.joinpath("DBC_SUDESTE_RD") # -- input
parquet_location = basefolder.joinpath("PARQUET_SUDESTE_RD") # -- output

uf_names = ["ES", "SP", "MG", "RJ"]
patterns = [f"*{prefix}*.dbc" for prefix in uf_names]
# -- collect matching files
matching_files = []
for pattern in patterns:
    curr = [ fn for fn in dbc_location.glob(pattern) ]
    matching_files.extend(curr)

for current_file in sorted(matching_files):
    print(f'converting {current_file.stem} into parquet ... ', end='')
    output_file = parquet_location.joinpath(f'{current_file.stem}.parquet')

    try:
        subprocess.run(
            ["Rscript", "dbc_to_parquet.R", current_file, output_file], 
            check=True
        )
        print("done.")
    except subprocess.CalledProcessError as e:
        print(f"error occurred: {e}")

#for origin in origin_list: # -- loop on the database (only SIHSUS)
#    for preffix in preffix_dict[origin]: # -- specific table
#        for uf in uf_list: # -- Brazilian states to consider
#            # -- year and month of downloaded data
#            for yy in yy_list:
#                for mm in mm_list:
#                    fname = f"{preffix}{uf}{yy}{mm}"
#                    
#
#                    df = pd.read_parquet(basefolder.joinpath("PARQUET", f"{fname}.parquet"))
#                    if origin=="SIHSUS":
#                        warehouse_injector.insert_sih(df, fname, preffix)
#                    # -- delete the downloaded files after the injecting the data into the database.
#                    # ---- delete DBF file
#                    if basefolder.joinpath("DBF", f"{fname}.dbf").is_file():
#                        basefolder.joinpath("DBF", f"{fname}.dbf").unlink()
#                    # ---- delete DBC file
#                    if basefolder.joinpath("DBC", f"{fname}.dbc").is_file():
#                        basefolder.joinpath("DBC", f"{fname}.dbc").unlink()
#                    # ---- delete PARQUET file
#                    if basefolder.joinpath("PARQUET", f"{fname}.parquet").is_file():
#                        basefolder.joinpath("PARQUET", f"{fname}.parquet").unlink()
#
