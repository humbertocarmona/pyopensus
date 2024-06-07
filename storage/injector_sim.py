# -- lib
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from pathlib import Path

# -- import the warehouse class
from storage import WarehouseSIM

class WarehouseInjectorSim:
    def __init__(self, warehouse_location, warehouse_name):
        self.warehouse_location = Path(warehouse_location)
        self.warehouse_name = Path(warehouse_name)

        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseSIM(self.engine_url)
        self.engine = self.warehouse.db_init()

    def drop_tables(self, tables_to_delete=None):
        '''
            If it is necessary to delete all current stored data before injection, then
            this function will perform this operation.
        '''
        table_names = tables_to_delete
        if tables_to_delete is None:
            table_names = list(self.warehouse.tables.keys())
        for tb_name in table_names:
            self.warehouse.delete_table(tb_name, is_sure=True, authkey="###!Y!.")
        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()
    
    def insert_sim(self, sim_df, sim_fname, verbose=False):
        '''

        '''
        fonte_name = Path(sim_fname).stem
        sim_df["DTOBITO"] = pd.to_datetime(sim_df["DTOBITO"], format="%d%m%Y", errors='coerce')
        sim_df["DTNASC"] = pd.to_datetime(sim_df["DTNASC"], format="%d%m%Y", errors='coerce')
        sim_df["FONTE_DADOS"] = [ fonte_name for n in range(sim_df.shape[0]) ]
        sim_df = sim_df.rename({"contador": "CONTADOR"}, axis=1)
        sim_df["CHAVE_CONTADOR_FONTE"] = sim_df["CONTADOR"] + sim_df["FONTE_DADOS"]

        self.warehouse.insert('sim', sim_df, batchsize=200, verbose=verbose)
        
