'''

'''
from sqlalchemy import create_engine
from sqlalchemy import Column, Table, MetaData

from pyopensus.storage.warehouse_base import WarehouseBase

# -- import the data models
from pyopensus.storage.sih_data_models import AIH, ServicosAIH, Rejeitadas
from pyopensus.storage.cnes_data_models import BaseCnes, Estabelecimentos, Equipamentos, Leitos, Equipes, Profissionais, ServicoEspecializados
from pyopensus.storage.sim_data_models import SIM

class WarehouseSIH(WarehouseBase):
    '''
        Warehouse schema for SIHSUS data.
    '''
    def __init__(self, engine_url):
        self._engine = create_engine(engine_url, future=True)
        self._metadata = MetaData()
        self._tables = {}
        self._mappings = {}

        # -- include the data models
        self._imported_data_models = [ AIH(self._metadata).define(),
                                       ServicosAIH(self._metadata).define(),
                                       Rejeitadas(self._metadata).define() ]

        for elem in self._imported_data_models:
            self._tables.update(elem[0])
            self._mappings.update(elem[1])

class WarehouseCNES(WarehouseBase):
    '''
        Warehouse schema for CNES data.
    '''
    def __init__(self, engine_url):
        self._engine = create_engine(engine_url, future=True)
        self._metadata = MetaData()
        self._tables = {}
        self._mappings = {}

        # -- include the data models
        self._imported_data_models = [ BaseCnes(self._metadata).define(),
                                       Estabelecimentos(self._metadata).define(),
                                       Profissionais(self._metadata).define(),
                                       Leitos(self._metadata).define(),
                                       Equipamentos(self._metadata).define(),
                                       Equipes(self._metadata).define(),
                                       ServicoEspecializados(self._metadata).define()]

        for elem in self._imported_data_models:
            self._tables.update(elem[0])
            self._mappings.update(elem[1])      

class WarehouseSIM(WarehouseBase):
    '''
        Warehouse schema for SIM data.
    '''
    def __init__(self, engine_url):
        self._engine = create_engine(engine_url, future=True)
        self._metadata = MetaData()
        self._tables = {}
        self._mappings = {}

        # -- include the data models
        self._imported_data_models = [ SIM(self._metadata).define()]

        for elem in self._imported_data_models:
            self._tables.update(elem[0])
            self._mappings.update(elem[1]) 