# -- lib
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from pathlib import Path

# -- import the warehouse class
from storage import WarehouseSUS

class WarehouseInjector:
    def __init__(self, warehouse_location, warehouse_name):
        self.warehouse_location = Path(warehouse_location)
        self.warehouse_name = Path(warehouse_name)

        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseSUS(self.engine_url)
        self.engine = self.warehouse.db_init()

    def drop_tables(self):
        '''
            If it is necessary to delete all current stored data before injection, then
            this function will perform this operation.
        '''
        table_names = list(self.warehouse.tables.keys())
        for tb_name in table_names:
            self.warehouse.delete_table(tb_name, is_sure=True, authkey="###!Y!.")
        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()
    
    def insert_sih(self, sih_df, sih_fname, preffix, verbose=False):
        '''
            ...

            Args:
            -----
                sih_df:
                    pandas.DataFrame.
                sih_fname:
                    String. Original name of the file.

        '''
        fonte_name = Path(sih_fname).stem
        sih_df["FONTE"] = [ fonte_name for n in range(sih_df.shape[0]) ]
        
        if preffix == "RD":
            self.warehouse.insert('aih_reduzida', sih_df, batchsize=200, verbose=verbose)
        elif preffix == "SP":
            self.warehouse.insert('servicos_profissionais', sih_df, batchsize=200, verbose=verbose)

    def insert_cnes(self, cnes_df, cnes_fname, preffix, verbose=False):
        '''
            ...

            Args:
            -----
                cnes_df:
                    pandas.DataFrame.
                cnes_fname:
                    String.
                preffix:
                    String.
        '''
        fonte_name = Path(cnes_fname).stem
        cnes_df["FONTE"] = [ fonte_name for n in range(cnes_df.shape[0]) ]

        if preffix == "ST":
            self.warehouse.insert('cnes_estabelecimentos', cnes_df, batchsize=200, verbose=verbose)
        elif preffix == "LT":
            self.warehouse.insert('cnes_leitos', cnes_df, batchsize=200, verbose=verbose)
        elif preffix == "PF":
            self.warehouse.insert('cnes_profissionais', cnes_df, batchsize=200, verbose=verbose)
        elif preffix == "SR":
            self.warehouse.insert('cnes_servico_especializado', cnes_df, batchsize=200, verbose=verbose)
        
