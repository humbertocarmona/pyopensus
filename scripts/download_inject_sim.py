import sys
sys.path.append("..")
from pathlib import Path
import pandas as pd

from pyopensus import Opensus
from storage import WarehouseInjectorSim

basefolder = Path.home().joinpath("Documents", "data", "opendatasus", "sim")
warehouse_location = Path.home().joinpath("Documents", "data", "opendatasus")
warehouse_name = "SIM_WAREHOUSE.db"

warehouse_injector = WarehouseInjectorSim(warehouse_location, warehouse_name)
opensus = Opensus()

#uf_list = ["CE"]
uf_list = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
           "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
           "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
yy_list = [f"{yy}" for yy in range(2000, 2022+1)]
origin_list = ["SIM"]

for origin in origin_list:
    for uf in uf_list:
        for yy in yy_list:
            fname = f"DO{uf}{yy}"
            try:
                opensus.retrieve_file(basefolder, origin, fname, to_dbf=True, to_parquet=True, verbose=True)                    
            except:
                continue

            df = pd.read_parquet(basefolder.joinpath("PARQUET", f"{fname}.parquet"))
            warehouse_injector.insert_sim(df, fname)
            # -- delete DBF file
            if basefolder.joinpath("DBF", f"{fname}.dbf").is_file():
                basefolder.joinpath("DBF", f"{fname}.dbf").unlink()
            # -- delete DBC file
            if basefolder.joinpath("DBC", f"{fname}.dbc").is_file():
                basefolder.joinpath("DBC", f"{fname}.dbc").unlink()
            # -- delete PARQUET file
            if basefolder.joinpath("PARQUET", f"{fname}.parquet").is_file():
                basefolder.joinpath("PARQUET", f"{fname}.parquet").unlink()
                





