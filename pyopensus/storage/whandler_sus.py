# -- lib
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from pathlib import Path

# -- import the warehouse class
from pyopensus.storage.whandler_base import HandlerBase
from pyopensus.storage.warehouse_sus import WarehouseSIM, WarehouseSIH

# ------------------ SIHSUS & CNES --------------------

class HandlerSIH(HandlerBase):
    def __init__(self, warehouse_location, warehouse_name):
        super().__init__(warehouse_location, warehouse_name)
    
        self.warehouse = WarehouseSIH(self.engine_url)
        self.engine = self.warehouse.db_init()
    
    def insert_sih(self, sih_df, sih_fname, preffix, verbose=False):
        '''
            Given a dataframe originated from the SIHSUS database, inserts it into the
            SIH integrated database.

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
            for dt_col in ["NASC", "DT_INTER", "DT_SAIDA"]:
                sih_df[dt_col] = pd.to_datetime(sih_df[dt_col], errors="coerce")
            self.warehouse.insert('aih_reduzida', sih_df, batchsize=200, verbose=verbose)
        elif preffix == "SP":
            self.warehouse.insert('servicos_profissionais', sih_df, batchsize=200, verbose=verbose)
        elif preffix == "RJ":
            for dt_col in ["NASC", "DT_INTER", "DT_SAIDA"]:
                sih_df[dt_col] = pd.to_datetime(sih_df[dt_col], errors="coerce")
            self.warehouse.insert('aih_rejeitada', sih_df, batchsize=200, verbose=verbose)

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


# ------------------ SIM --------------------

class HandlerSIM(HandlerBase):
    def __init__(self, warehouse_location, warehouse_name):
        super().__init__(warehouse_location, warehouse_name)
    
        self.warehouse = WarehouseSIM(self.engine_url)
        self.engine = self.warehouse.db_init()
    
    def insert_sim(self, sim_df, sim_fname, verbose=False):
        '''
            Insert records to SIM warehouse from data expected to come
            from the official SIM data (official schema).
        '''
        fonte_name = Path(sim_fname).stem
        sim_df["DTOBITO"] = pd.to_datetime(sim_df["DTOBITO"], format="%d%m%Y", errors='coerce')
        sim_df["DTNASC"] = pd.to_datetime(sim_df["DTNASC"], format="%d%m%Y", errors='coerce')
        sim_df["FONTE_DADOS"] = [ fonte_name for n in range(sim_df.shape[0]) ]
        sim_df = sim_df.rename({"contador": "CONTADOR"}, axis=1)
        sim_df["CHAVE_CONTADOR_FONTE"] = sim_df["CONTADOR"] + sim_df["FONTE_DADOS"]

        self.warehouse.insert('sim', sim_df, batchsize=200, verbose=verbose)
        
