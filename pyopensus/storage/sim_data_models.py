'''
    Define the data models to store the main information on individuals 
    and linkage between different records.

    Author: Higor S. Monteiro
    Email: higor.monteiro@fisica.ufc.br
'''

import datetime as dt
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import DateTime, Integer, Numeric, String, Float, Sequence, ForeignKey, CheckConstraint
from sqlalchemy.exc import InternalError, IntegrityError

class SIM:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'sim'
        self._dummy_ = ['CHAVE_CONTADOR_FONTE', 'TIPOBITO', 'DTOBITO', 'NATURAL', 'DTNASC', 
                        'IDADE', 'SEXO', 'RACACOR', 'ESTCIV', 'ESC', 'OCUP',
                        'CODMUNRES', 'LOCOCOR', 'CODMUNOCOR', 'IDADEMAE', 'ESCMAE', 'OCUPMAE',
                        'QTDFILVIVO', 'QTDFILMORT', 'GRAVIDEZ', 'GESTACAO', 'PARTO', 'OBITOPARTO',
                        'PESO', 'OBITOGRAV', 'OBITOPUERP', 'ASSISTMED', 'EXAME', 'CIRURGIA',
                        'NECROPSIA', 'CAUSABAS', 'LINHAA', 'LINHAB', 'LINHAC', 'LINHAD',
                        'LINHAII', 'CIRCOBITO', 'ACIDTRAB', 'FONTE_DADOS']

        self.model = Table(
            self.table_name, self.metadata,
            Column("CHAVE_CONTADOR_FONTE", String, primary_key=True),
            Column("TIPOBITO", String(1), nullable=True),
            Column("DTOBITO", DateTime, nullable=False),
            Column("NATURAL", String(3), nullable=True),
            Column("DTNASC", DateTime, nullable=True),
            Column("IDADE", String(3), nullable=True),
            Column("SEXO", String(1), nullable=True),
            Column("RACACOR", String(1), nullable=True),
            Column("ESTCIV", String(1), nullable=True),
            Column("ESC", String(1), nullable=True),
            Column("OCUP", String(6), nullable=True),
            Column("CODMUNRES", String(7), nullable=True),
            Column("LOCOCOR", String(1), nullable=True),
            Column("CODMUNOCOR", String(8), nullable=True),
            Column("IDADEMAE", String(2), nullable=True),
            Column("ESCMAE", String(1), nullable=True),
            Column("OCUPMAE", String(6), nullable=True),
            Column("QTDFILVIVO", String(2), nullable=True),
            Column("QTDFILMORT", String(2), nullable=True),
            Column("GRAVIDEZ", String(1), nullable=True),
            Column("GESTACAO", String(1), nullable=True),
            Column("PARTO", String(1), nullable=True),
            Column("OBITOPARTO", String(1), nullable=True),
            Column("PESO", String(4), nullable=True),
            Column("OBITOGRAV", String(1), nullable=True),
            Column("OBITOPUERP", String(1), nullable=True),
            Column("ASSISTMED", String(1), nullable=True),
            Column("EXAME", String(1), nullable=True),
            Column("CIRURGIA", String(1), nullable=True),
            Column("NECROPSIA", String(1), nullable=True),
            Column("CAUSABAS", String(4), nullable=True),
            Column("LINHAA", String(20), nullable=True),
            Column("LINHAB", String(20), nullable=True),
            Column("LINHAC", String(20), nullable=True),
            Column("LINHAD", String(20), nullable=True),
            Column("LINHAII", String(30), nullable=True),
            Column("CIRCOBITO", String(1), nullable=True),
            Column("ACIDTRAB", String(1), nullable=True),
            Column('FONTE_DADOS', String(8), nullable=True),
        )

        self.mapping = { n:n for n in self._dummy_ }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem