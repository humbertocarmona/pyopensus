from pathlib import Path
import pandas as pd
import numpy as np
from pyopensus.opensus.opensus import Opensus
from pyopensus.storage.warehouse_sus import WarehouseCNES
import pyopensus.utils.utils as utils

class ProcessCNES:
    def __init__(self, dest, preffix_to_table=None):
        self.dest = dest
        self.origin = "CNES"
        self.preffixes = utils.preffix_dictionary()['cnes']
        self.opensus = Opensus()

        if preffix_to_table is None:
            self.preffix_to_table = {
                'ST': [ 'cnes', 'estabelecimentos_mes' ],
                'PF': ['profissionais_mes'],
                'SR': ['servicos_especializados_mes'],
                'EQ': ['equipamentos_mes'],
                'EP': ['equipes_mes'],
                'LT': ['leitos_mes']
            }

    def download_data(self, uf, list_of_months, list_of_years, preffixes=None, keep_dbf=False, keep_parquet=True):
        '''

            Args:
            -----
                uf:
                    String.
                list_of_months:
                    List of Strings.
                list_of_years:
                    List of Strings.
                preffixes:
                    None or List of Strings.
                keep_dbf:
                    Bool.
                keep_parquet:
                    Bool.
        '''
        if preffixes is None:
            preffixes = list(self.preffixes.keys())
        else:
            if not set(preffixes).issubset(set(self.preffixes.keys())):
                raise Exception("Some or all of preffixes parsed are not included in the official document.")

        for yy in list_of_years:
            for mm in list_of_months:
                for preffix in preffixes:
                    fname = f"{preffix}{uf}{yy}{mm}" 
                    #self.opensus.retrieve_file(self.dest, self.origin, fname, preffix_folder=preffix, to_dbf=True, to_parquet=True, verbose=True)    
                    try:
                        self.opensus.retrieve_file(self.dest, self.origin, fname, preffix_folder=preffix, to_dbf=True, to_parquet=True, verbose=True)                    
                    except:
                        continue

                    # -- delete dbf and/or parquet if signaled.
                    if self.dest.joinpath("DBF", f"{fname}.dbf").is_file() and not keep_dbf:
                        self.dest.joinpath("DBF", f"{fname}.dbf").unlink()
                    # -- delete PARQUET file
                    if self.dest.joinpath("PARQUET", f"{fname}.parquet").is_file() and not keep_parquet:
                        self.dest.joinpath("PARQUET", f"{fname}.parquet").unlink()

    def insert_data(self, warehouse_location, warehouse_name, preffixes=None, verbose=True):
        '''

        '''
        engine_url = f"sqlite:///{warehouse_location.joinpath(warehouse_name)}"
        warehouse = WarehouseCNES(engine_url)
        engine = warehouse.db_init()

        # -- expected columns to each table
        expected_cols_models = warehouse.expected_columns()

        if preffixes is None:
            preffixes = list(self.preffixes.keys())
        else:
            if not set(preffixes).issubset(set(self.preffixes.keys())):
                raise Exception("Some or all of preffixes parsed are not included in the official document.")

        list_of_files = sorted([ filename for filename in self.dest.joinpath("PARQUET").glob("*.parquet") ])
        for filename in list_of_files:
            stem = filename.stem
            preffix = stem[:2]
            print(stem)
            
            df = pd.read_parquet(filename)
            # -- any transformation to the data
            df["COMPET"] = pd.to_datetime(df["COMPETEN"].apply(lambda x: f"{x[:4]}-{x[4:]}-01"), format="%Y-%m-%d", errors="coerce")
            # -- find the table(s) associated with the current preffix
            tables = self.preffix_to_table[preffix]
            for table_name in tables:
                model_column = expected_cols_models[table_name]
                for col in model_column:
                    if col not in df.columns:
                        df[col] = [ np.nan for n in range(df.shape[0]) ]

                # -- insert the data into the table
                warehouse.insert(table_name, df, batchsize=200, verbose=verbose)
        


        