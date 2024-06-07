# -- lib
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5
from pathlib import Path

# -- import the warehouse class
from pyopensus.storage.warehouse_base import WarehouseBase

class HandlerBase:
    '''
        ...
    '''
    def __init__(self, warehouse_location, warehouse_name):
        self.warehouse_location = Path(warehouse_location)
        self.warehouse_name = Path(warehouse_name)

        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseBase(self.engine_url)
        self.engine = self.warehouse.db_init()

    def drop_tables(self, tables_to_delete=None):
        '''
            If it is necessary to delete all current stored data before insertion, then
            this function will perform this operation.
        '''
        table_names = tables_to_delete
        if tables_to_delete is None:
            table_names = list(self.warehouse.tables.keys())
        for tb_name in table_names:
            self.warehouse.delete_table(tb_name, is_sure=True, authkey="###!Y!.")
        self.warehouse = WarehouseIST(self.engine_url)
        self.engine = self.warehouse.db_init()
    
    def insert_data(self):
        pass
        
