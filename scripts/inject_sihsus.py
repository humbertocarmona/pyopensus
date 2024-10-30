'''
    Download SIHSUS data from the northeast Brazilian states and structure the
    data into a database. 
'''
import sys
import pandas as pd
sys.path.append("..")
from pathlib import Path
from pyopensus.storage.whandler_sus import HandlerSIH

basefolder = Path.home().joinpath("Documents", "data", "opendatasus", "sihsus")
warehouse_location = Path.home().joinpath("Documents", "data", "opendatasus")
warehouse_name = "SIHSUS_SUDESTE_NO_SERVICE_V2.db"
# -- load the database schema
warehouse_injector = HandlerSIH(warehouse_location, warehouse_name)

parquet_location_rd = basefolder.joinpath("PARQUET_SUDESTE_RD")
parquet_location_rj = basefolder.joinpath("PARQUET_SUDESTE_RJ")
name_dict = { "RD": parquet_location_rd, "RJ": parquet_location_rj }

verbose = True
for preffix in ["RD", "RJ"]:
    cur_loc = name_dict[preffix]
    parq_files = sorted([ nm for nm in cur_loc.glob("*") ])
    for current_file in parq_files:
        cur_df = pd.read_parquet(current_file)
        # -- inject data
        if verbose:
            print(f"Injecting {current_file.stem} file ... ", end='')
        warehouse_injector.insert_sih(cur_df, current_file.stem, preffix)