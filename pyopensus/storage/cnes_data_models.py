'''
    Define the data models to store the main information on establishments 
    and linkage between different records.

    Author: Your Name
    Email: your.email@example.com
'''
import datetime as dt
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import DateTime, Integer, Numeric, String, Float, Sequence, ForeignKey, CheckConstraint
from sqlalchemy.exc import InternalError, IntegrityError

class BaseCnes:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'cnes'
        self._dummy_ = [
            'CNES', 'CODUFMUN', 'COD_CEP', 'CPF_CNPJ', 'PF_PJ',
            'VINC_SUS', 'TPGESTAO', 'ESFERA_A', 'NATUREZA', 'TP_UNID', 
            'NIV_HIER', 'TP_PREST', 'LATITUDE', 'LONGITUDE'
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), primary_key=True),
            Column('CODUFMUN', String(7), nullable=True),
            Column('COD_CEP', String(8), nullable=True),
            Column('CPF_CNPJ', String(14), nullable=True),
            Column('PF_PJ', String(1), nullable=True),
            Column('VINC_SUS', String(1), nullable=True),
            Column('TPGESTAO', String(1), nullable=True),
            Column('ESFERA_A', String(2), nullable=True),
            Column('NATUREZA', String(2), nullable=True),
            Column('TP_UNID', String(2), nullable=True),
            Column('NIV_HIER', String(2), nullable=True),
            Column('TP_PREST', String(2), nullable=True),
            Column('LATITUDE', Numeric(11,8), nullable=True),
            Column('LONGITUDE', Numeric(11,8), nullable=True),
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

class Estabelecimentos:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'estabelecimentos_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'VINC_SUS', 'TPGESTAO', 'ESFERA_A', 'NATUREZA', 
            'TP_UNID', 'NIV_HIER', 'TP_PREST', 'QTLEITP1', 'QTLEITP2', 'QTLEITP3',
            'LEITHOSP', 'MOTDESAB'
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('VINC_SUS', String(1), nullable=True),
            Column('TPGESTAO', String(1), nullable=True),
            Column('ESFERA_A', String(2), nullable=True),
            Column('NATUREZA', String(2), nullable=True),
            Column('TP_UNID', String(2), nullable=True),
            Column('NIV_HIER', String(2), nullable=True),
            Column('TP_PREST', String(2), nullable=True),
            Column('QTLEITP1', Numeric(4), nullable=True),
            Column('QTLEITP2', Numeric(4), nullable=True),
            Column('QTLEITP3', Numeric(4), nullable=True),
            Column('LEITHOSP', String(1), nullable=True),
            Column('MOTDESAB', String(2), nullable=True),
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

class Equipamentos:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'equipamentos_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'TIPEQUIP', 'CODEQUIP', 'QT_EXIST', 'QT_USO',
            'IND_SUS', 'IND_NSUS' 
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('TIPEQUIP', String(1), nullable=True),
            Column('CODEQUIP', String(2), nullable=True),
            Column('QT_EXIST', String(4), nullable=True),
            Column('QT_USO', String(4), nullable=True),
            Column('IND_SUS', String(1), nullable=True),
            Column('IND_NSUS', String(1), nullable=True),
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

class Leitos:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'leitos_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'TP_LEITO', 'CODLEITO', 'QT_EXIST', 'QT_CONTR', 'QT_SUS'
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('TP_LEITO', String(2), nullable=True),
            Column('CODLEITO', String(2), nullable=True),
            Column('QT_EXIST', Numeric, nullable=True),
            Column('QT_CONTR', Numeric, nullable=True),
            Column('QT_SUS', Numeric, nullable=True),
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


class Profissionais:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'profissionais_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'CBO', 'CBO_UNICO', 'CNS_PROF', 'CONSELHO',
            'REGISTRO', 'VINCULAC', 'VINCUL_C', 'VINCUL_A', 'VINCUL_N',
            'PROF_SUS', 'PROFNSUS', 'HORAHOSP', 'HORA_AMB' 
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('CBO', String(6), nullable=True),
            Column('CBO_UNICO', String(2), nullable=True),
            Column('CNS_PROF', String(15), nullable=True),
            Column('CONSELHO', String(2), nullable=True),
            Column('REGISTRO', String(13), nullable=True),
            Column('VINCULAC', String(6), nullable=True),
            Column('VINCUL_C', String(1), nullable=True),
            Column('VINCUL_A', String(1), nullable=True),
            Column('VINCUL_N', String(1), nullable=True),
            Column('PROF_SUS', String(1), nullable=True),
            Column('PROFNSUS', String(1), nullable=True),
            Column('HORAHOSP', String(3), nullable=True),
            Column('HORA_AMB', String(3), nullable=True),
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

class ServicoEspecializados:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'servicos_especializados_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'SERV_ESP', 'CLASS_SR', 'SRVUNICO', 'CARACTER',
            'AMB_HOSP', 'CONTSRVU', 'CNESTERC'
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('SERV_ESP', String(3), nullable=True),
            Column('CLASS_SR', String(3), nullable=True),
            Column('SRVUNICO', String(3), nullable=True),
            Column('CARACTER', String(1), nullable=True),
            Column('AMB_HOSP', String(4), nullable=True),
            Column('CONTSRVU', String(1), nullable=True),
            Column('CNESTERC', String(7), nullable=True),
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

class Equipes:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'equipes_mes'
        self._dummy_ = [
            'CNES', 'COMPET', 'AP01CV01', 'AP01CV02', 'AP01CV05', 'AP01CV06', 
            'AP01CV03', 'AP01CV04', 'AP02CV01', 'AP02CV02', 'AP02CV05', 'AP02CV06', 
            'AP02CV03', 'AP02CV04', 'AP03CV01', 'AP03CV02', 'AP03CV05', 'AP03CV06', 
            'AP03CV03', 'AP03CV04', 'AP04CV01', 'AP04CV02', 'AP04CV05', 'AP04CV06', 
            'AP04CV03', 'AP04CV04', 'AP05CV01', 'AP05CV02', 'AP05CV05', 'AP05CV06', 
            'AP05CV03', 'AP05CV04', 'AP06CV01', 'AP06CV02', 'AP06CV05', 'AP06CV06', 
            'AP06CV03', 'AP06CV04', 'AP07CV01', 'AP07CV02', 'AP07CV05', 'AP07CV06', 
            'AP07CV03', 'AP07CV04', 'ATEND_PR', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E',
            'GESPRG2M', 'GESPRG4E', 'GESPRG4M', 'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 
            'GESPRG5E', 'GESPRG5M', 'GESPRG6E', 'GESPRG6M', 'NIVATE_H', 'NIVATE_A',
            'ID_EQUIPE', 'TIPO_EQP', 'AREA_EQP', 'ID_SEGM', 'TIPOSEGM' 
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), ForeignKey("cnes.CNES")),
            Column("COMPET", DateTime, nullable=False),
            Column('GESPRG1E', String(1), nullable=True),
            Column('GESPRG1M', String(1), nullable=True),
            Column('GESPRG2E', String(1), nullable=True),
            Column('GESPRG2M', String(1), nullable=True),
            Column('GESPRG4E', String(1), nullable=True),
            Column('GESPRG4M', String(1), nullable=True),
            Column('NIVATE_A', String(1), nullable=True),
            Column('GESPRG3E', String(1), nullable=True),
            Column('GESPRG3M', String(1), nullable=True),
            Column('GESPRG5E', String(1), nullable=True),
            Column('GESPRG5M', String(1), nullable=True),
            Column('GESPRG6E', String(1), nullable=True),
            Column('GESPRG6M', String(1), nullable=True),
            Column('NIVATE_H', String(1), nullable=True),
            Column('AP01CV01', String(1), nullable=True),
            Column('AP01CV02', String(1), nullable=True),
            Column('AP01CV05', String(1), nullable=True),
            Column('AP01CV06', String(1), nullable=True),
            Column('AP01CV03', String(1), nullable=True),
            Column('AP01CV04', String(1), nullable=True),
            Column('AP02CV01', String(1), nullable=True),
            Column('AP02CV02', String(1), nullable=True),
            Column('AP02CV05', String(1), nullable=True),
            Column('AP02CV06', String(1), nullable=True),
            Column('AP02CV03', String(1), nullable=True),
            Column('AP02CV04', String(1), nullable=True),
            Column('AP03CV01', String(1), nullable=True),
            Column('AP03CV02', String(1), nullable=True),
            Column('AP03CV05', String(1), nullable=True),
            Column('AP03CV06', String(1), nullable=True),
            Column('AP03CV03', String(1), nullable=True),
            Column('AP03CV04', String(1), nullable=True),
            Column('AP04CV01', String(1), nullable=True),
            Column('AP04CV02', String(1), nullable=True),
            Column('AP04CV05', String(1), nullable=True),
            Column('AP04CV06', String(1), nullable=True),
            Column('AP04CV03', String(1), nullable=True),
            Column('AP04CV04', String(1), nullable=True),
            Column('AP05CV01', String(1), nullable=True),
            Column('AP05CV02', String(1), nullable=True),
            Column('AP05CV05', String(1), nullable=True),
            Column('AP05CV06', String(1), nullable=True),
            Column('AP05CV03', String(1), nullable=True),
            Column('AP05CV04', String(1), nullable=True),
            Column('AP06CV01', String(1), nullable=True),
            Column('AP06CV02', String(1), nullable=True),
            Column('AP06CV05', String(1), nullable=True),
            Column('AP06CV06', String(1), nullable=True),
            Column('AP06CV03', String(1), nullable=True),
            Column('AP06CV04', String(1), nullable=True),
            Column('AP07CV01', String(1), nullable=True),
            Column('AP07CV02', String(1), nullable=True),
            Column('AP07CV05', String(1), nullable=True),
            Column('AP07CV06', String(1), nullable=True),
            Column('AP07CV03', String(1), nullable=True),
            Column('AP07CV04', String(1), nullable=True),
            Column('ATEND_PR', String(1), nullable=True),
            Column('ID_EQUIPE', String(18), nullable=True),
            Column('TIPO_EQP', String(2), nullable=True),
            Column('AREA_EQP', String(10), nullable=True),
            Column('ID_SEGM', String(8), nullable=True),
            Column('TIPOSEGM', String(1), nullable=True),
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


# ============================================================================================================ #

class Estabelecimentos_old:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'cnes_estabelecimentos'
        self._dummy_ = [
            'CNES', 'CODUFMUN', 'COD_CEP', 'CPF_CNPJ', 'PF_PJ', 'NIV_DEP', 'CNPJ_MAN', 'COD_IR', 'REGSAUDE',
            'MICR_REG', 'DISTRSAN', 'DISTRADM', 'VINC_SUS', 'TPGESTAO', 'ESFERA_A', 'ATIVIDAD', 'RETENCAO',
            'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TP_PREST', 'CO_BANCO', 'CO_AGENC',
            'C_CORREN', 'CONTRATM', 'DT_PUBLM', 'CONTRATE', 'DT_PUBLE', 'ALVARA', 'DT_EXPED', 'ORGEXPED',
            'AV_ACRED', 'CLASAVAL', 'DT_ACRED', 'AV_PNASS', 'DT_PNASS', 'GESPRG1E', 'GESPRG1M', 'GESPRG2E',
            'GESPRG2M', 'GESPRG4E', 'GESPRG4M', 'NIVATE_A', 'GESPRG3E', 'GESPRG3M', 'GESPRG5E', 'GESPRG5M',
            'GESPRG6E', 'GESPRG6M', 'NIVATE_H', 'QTLEITP1', 'QTLEITP2', 'QTLEITP3', 'LEITHOSP', 'QTINST01',
            'QTINST02', 'QTINST03', 'QTINST04', 'QTINST05', 'QTINST06', 'QTINST07', 'QTINST08', 'QTINST09',
            'QTINST10', 'QTINST11', 'QTINST12', 'QTINST13', 'QTINST14', 'URGEMERG', 'QTINST15', 'QTINST16',
            'QTINST17', 'QTINST18', 'QTINST19', 'QTINST20', 'QTINST21', 'QTINST22', 'QTINST23', 'QTINST24',
            'QTINST25', 'QTINST26', 'QTINST27', 'QTINST28', 'QTINST29', 'QTINST30', 'ATENDAMB', 'QTINST31',
            'QTINST32', 'QTINST33', 'CENTRCIR', 'QTINST34', 'QTINST35', 'QTINST36', 'QTINST37', 'CENTROBS',
            'QTLEIT05', 'QTLEIT06', 'QTLEIT07', 'QTLEIT09', 'QTLEIT19', 'QTLEIT20', 'QTLEIT21', 'QTLEIT22',
            'QTLEIT23', 'QTLEIT32', 'QTLEIT34', 'QTLEIT38', 'QTLEIT39', 'QTLEIT40', 'CENTRNEO', 'ATENDHOS',
            'SERAP01P', 'SERAP01T', 'SERAP02P', 'SERAP02T', 'SERAP03P', 'SERAP03T', 'SERAP04P', 'SERAP04T',
            'SERAP05P', 'SERAP05T', 'SERAP06P', 'SERAP06T', 'SERAP07P', 'SERAP07T', 'SERAP08P', 'SERAP08T',
            'SERAP09P', 'SERAP09T', 'SERAP10P', 'SERAP10T', 'SERAP11P', 'SERAP11T', 'SERAPOIO', 'RES_BIOL',
            'RES_QUIM', 'RES_RADI', 'RES_COMU', 'COLETRES', 'COMISS01', 'COMISS02', 'COMISS03', 'COMISS04',
            'COMISS05', 'COMISS06', 'COMISS07', 'COMISS08', 'COMISS09', 'COMISS10', 'COMISS11', 'COMISS12',
            'COMISSAO', 'AP01CV01', 'AP01CV02', 'AP01CV05', 'AP01CV06', 'AP01CV03', 'AP01CV04', 'AP02CV01',
            'AP02CV02', 'AP02CV05', 'AP02CV06', 'AP02CV03', 'AP02CV04', 'AP03CV01', 'AP03CV02', 'AP03CV05',
            'AP03CV06', 'AP03CV03', 'AP03CV04', 'AP04CV01', 'AP04CV02', 'AP04CV05', 'AP04CV06', 'AP04CV03',
            'AP04CV04', 'AP05CV01', 'AP05CV02', 'AP05CV05', 'AP05CV06', 'AP05CV03', 'AP05CV04', 'AP06CV01',
            'AP06CV02', 'AP06CV05', 'AP06CV06', 'AP06CV03', 'AP06CV04', 'AP07CV01', 'AP07CV02', 'AP07CV05',
            'AP07CV06', 'AP07CV03', 'AP07CV04', 'ATEND_PR', 'MOTDESAB', 'DT_ATUA', 'COMPETEN', 'NAT_JUR', "FONTE",
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), nullable=True),
            Column('CODUFMUN', String(7), nullable=True),
            Column('COD_CEP', String(8), nullable=True),
            Column('CPF_CNPJ', String(14), nullable=True),
            Column('PF_PJ', String(1), nullable=True),
            Column('NIV_DEP', String(1), nullable=True),
            Column('CNPJ_MAN', String(14), nullable=True),
            Column('COD_IR', String(2), nullable=True),
            Column('REGSAUDE', String(4), nullable=True),
            Column('MICR_REG', String(6), nullable=True),
            Column('DISTRSAN', String(4), nullable=True),
            Column('DISTRADM', String(4), nullable=True),
            Column('VINC_SUS', String(1), nullable=True),
            Column('TPGESTAO', String(1), nullable=True),
            Column('ESFERA_A', String(2), nullable=True),
            Column('ATIVIDAD', String(2), nullable=True),
            Column('RETENCAO', String(2), nullable=True),
            Column('NATUREZA', String(2), nullable=True),
            Column('CLIENTEL', String(2), nullable=True),
            Column('TP_UNID', String(2), nullable=True),
            Column('TURNO_AT', String(2), nullable=True),
            Column('NIV_HIER', String(2), nullable=True),
            Column('TP_PREST', String(2), nullable=True),
            Column('CO_BANCO', String(4), nullable=True),
            Column('CO_AGENC', String(5), nullable=True),
            Column('C_CORREN', String(14), nullable=True),
            Column('CONTRATM', String(11), nullable=True),
            Column('DT_PUBLM', String(8), nullable=True),
            Column('CONTRATE', String(20), nullable=True),
            Column('DT_PUBLE', String(8), nullable=True),
            Column('ALVARA', String(25), nullable=True),
            Column('DT_EXPED', String(8), nullable=True),
            Column('ORGEXPED', String(2), nullable=True),
            Column('AV_ACRED', String(1), nullable=True),
            Column('CLASAVAL', String(1), nullable=True),
            Column('DT_ACRED', String(6), nullable=True),
            Column('AV_PNASS', String(1), nullable=True),
            Column('DT_PNASS', String(6), nullable=True),
            Column('GESPRG1E', String(1), nullable=True),
            Column('GESPRG1M', String(1), nullable=True),
            Column('GESPRG2E', String(1), nullable=True),
            Column('GESPRG2M', String(1), nullable=True),
            Column('GESPRG4E', String(1), nullable=True),
            Column('GESPRG4M', String(1), nullable=True),
            Column('NIVATE_A', String(1), nullable=True),
            Column('GESPRG3E', String(1), nullable=True),
            Column('GESPRG3M', String(1), nullable=True),
            Column('GESPRG5E', String(1), nullable=True),
            Column('GESPRG5M', String(1), nullable=True),
            Column('GESPRG6E', String(1), nullable=True),
            Column('GESPRG6M', String(1), nullable=True),
            Column('NIVATE_H', String(1), nullable=True),
            Column('QTLEITP1', Numeric(3), nullable=True),
            Column('QTLEITP2', Numeric(3), nullable=True),
            Column('QTLEITP3', Numeric(3), nullable=True),
            Column('LEITHOSP', String(1), nullable=True),
            Column('QTINST01', Numeric(3), nullable=True),
            Column('QTINST02', Numeric(3), nullable=True),
            Column('QTINST03', Numeric(3), nullable=True),
            Column('QTINST04', Numeric(3), nullable=True),
            Column('QTINST05', Numeric(3), nullable=True),
            Column('QTINST06', Numeric(3), nullable=True),
            Column('QTINST07', Numeric(3), nullable=True),
            Column('QTINST08', Numeric(3), nullable=True),
            Column('QTINST09', Numeric(3), nullable=True),
            Column('QTINST10', Numeric(3), nullable=True),
            Column('QTINST11', Numeric(3), nullable=True),
            Column('QTINST12', Numeric(3), nullable=True),
            Column('QTINST13', Numeric(3), nullable=True),
            Column('QTINST14', Numeric(3), nullable=True),
            Column('URGEMERG', String(1), nullable=True),
            Column('QTINST15', Numeric(3), nullable=True),
            Column('QTINST16', Numeric(3), nullable=True),
            Column('QTINST17', Numeric(3), nullable=True),
            Column('QTINST18', Numeric(3), nullable=True),
            Column('QTINST19', Numeric(3), nullable=True),
            Column('QTINST20', Numeric(3), nullable=True),
            Column('QTINST21', Numeric(3), nullable=True),
            Column('QTINST22', Numeric(3), nullable=True),
            Column('QTINST23', Numeric(3), nullable=True),
            Column('QTINST24', Numeric(3), nullable=True),
            Column('QTINST25', Numeric(3), nullable=True),
            Column('QTINST26', Numeric(3), nullable=True),
            Column('QTINST27', Numeric(3), nullable=True),
            Column('QTINST28', Numeric(3), nullable=True),
            Column('QTINST29', Numeric(3), nullable=True),
            Column('QTINST30', Numeric(3), nullable=True),
            Column('ATENDAMB', String(1), nullable=True),
            Column('QTINST31', Numeric(3), nullable=True),
            Column('QTINST32', Numeric(3), nullable=True),
            Column('QTINST33', Numeric(3), nullable=True),
            Column('CENTRCIR', String(1), nullable=True),
            Column('QTINST34', Numeric(3), nullable=True),
            Column('QTINST35', Numeric(3), nullable=True),
            Column('QTINST36', Numeric(3), nullable=True),
            Column('QTINST37', Numeric(3), nullable=True),
            Column('CENTROBS', String(1), nullable=True),
            Column('QTLEIT05', Numeric(3), nullable=True),
            Column('QTLEIT06', Numeric(3), nullable=True),
            Column('QTLEIT07', Numeric(3), nullable=True),
            Column('QTLEIT09', Numeric(3), nullable=True),
            Column('QTLEIT19', Numeric(3), nullable=True),
            Column('QTLEIT20', Numeric(3), nullable=True),
            Column('QTLEIT21', Numeric(3), nullable=True),
            Column('QTLEIT22', Numeric(3), nullable=True),
            Column('QTLEIT23', Numeric(3), nullable=True),
            Column('QTLEIT32', Numeric(3), nullable=True),
            Column('QTLEIT34', Numeric(3), nullable=True),
            Column('QTLEIT38', Numeric(3), nullable=True),
            Column('QTLEIT39', Numeric(3), nullable=True),
            Column('QTLEIT40', Numeric(3), nullable=True),
            Column('CENTRNEO', String(1), nullable=True),
            Column('ATENDHOS', String(1), nullable=True),
            Column('SERAP01P', String(1), nullable=True),
            Column('SERAP01T', String(1), nullable=True),
            Column('SERAP02P', String(1), nullable=True),
            Column('SERAP02T', String(1), nullable=True),
            Column('SERAP03P', String(1), nullable=True),
            Column('SERAP03T', String(1), nullable=True),
            Column('SERAP04P', String(1), nullable=True),
            Column('SERAP04T', String(1), nullable=True),
            Column('SERAP05P', String(1), nullable=True),
            Column('SERAP05T', String(1), nullable=True),
            Column('SERAP06P', String(1), nullable=True),
            Column('SERAP06T', String(1), nullable=True),
            Column('SERAP07P', String(1), nullable=True),
            Column('SERAP07T', String(1), nullable=True),
            Column('SERAP08P', String(1), nullable=True),
            Column('SERAP08T', String(1), nullable=True),
            Column('SERAP09P', String(1), nullable=True),
            Column('SERAP09T', String(1), nullable=True),
            Column('SERAP10P', String(1), nullable=True),
            Column('SERAP10T', String(1), nullable=True),
            Column('SERAP11P', String(1), nullable=True),
            Column('SERAP11T', String(1), nullable=True),
            Column('SERAPOIO', String(1), nullable=True),
            Column('RES_BIOL', String(1), nullable=True),
            Column('RES_QUIM', String(1), nullable=True),
            Column('RES_RADI', String(1), nullable=True),
            Column('RES_COMU', String(1), nullable=True),
            Column('COLETRES', String(1), nullable=True),
            Column('COMISS01', String(1), nullable=True),
            Column('COMISS02', String(1), nullable=True),
            Column('COMISS03', String(1), nullable=True),
            Column('COMISS04', String(1), nullable=True),
            Column('COMISS05', String(1), nullable=True),
            Column('COMISS06', String(1), nullable=True),
            Column('COMISS07', String(1), nullable=True),
            Column('COMISS08', String(1), nullable=True),
            Column('COMISS09', String(1), nullable=True),
            Column('COMISS10', String(1), nullable=True),
            Column('COMISS11', String(1), nullable=True),
            Column('COMISS12', String(1), nullable=True),
            Column('COMISSAO', String(1), nullable=True),
            Column('AP01CV01', String(1), nullable=True),
            Column('AP01CV02', String(1), nullable=True),
            Column('AP01CV05', String(1), nullable=True),
            Column('AP01CV06', String(1), nullable=True),
            Column('AP01CV03', String(1), nullable=True),
            Column('AP01CV04', String(1), nullable=True),
            Column('AP02CV01', String(1), nullable=True),
            Column('AP02CV02', String(1), nullable=True),
            Column('AP02CV05', String(1), nullable=True),
            Column('AP02CV06', String(1), nullable=True),
            Column('AP02CV03', String(1), nullable=True),
            Column('AP02CV04', String(1), nullable=True),
            Column('AP03CV01', String(1), nullable=True),
            Column('AP03CV02', String(1), nullable=True),
            Column('AP03CV05', String(1), nullable=True),
            Column('AP03CV06', String(1), nullable=True),
            Column('AP03CV03', String(1), nullable=True),
            Column('AP03CV04', String(1), nullable=True),
            Column('AP04CV01', String(1), nullable=True),
            Column('AP04CV02', String(1), nullable=True),
            Column('AP04CV05', String(1), nullable=True),
            Column('AP04CV06', String(1), nullable=True),
            Column('AP04CV03', String(1), nullable=True),
            Column('AP04CV04', String(1), nullable=True),
            Column('AP05CV01', String(1), nullable=True),
            Column('AP05CV02', String(1), nullable=True),
            Column('AP05CV05', String(1), nullable=True),
            Column('AP05CV06', String(1), nullable=True),
            Column('AP05CV03', String(1), nullable=True),
            Column('AP05CV04', String(1), nullable=True),
            Column('AP06CV01', String(1), nullable=True),
            Column('AP06CV02', String(1), nullable=True),
            Column('AP06CV05', String(1), nullable=True),
            Column('AP06CV06', String(1), nullable=True),
            Column('AP06CV03', String(1), nullable=True),
            Column('AP06CV04', String(1), nullable=True),
            Column('AP07CV01', String(1), nullable=True),
            Column('AP07CV02', String(1), nullable=True),
            Column('AP07CV05', String(1), nullable=True),
            Column('AP07CV06', String(1), nullable=True),
            Column('AP07CV03', String(1), nullable=True),
            Column('AP07CV04', String(1), nullable=True),
            Column('ATEND_PR', String(1), nullable=True),
            Column('MOTDESAB', String(2), nullable=True),
            Column('DT_ATUA', String(6), nullable=True),
            Column('COMPETEN', String(6), nullable=True),
            Column('NAT_JUR', String(4), nullable=True),
            Column('FONTE', String, nullable=True)
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

class ServicoEspecializado_old:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'cnes_servico_especializado'
        self._dummy_ = [
            'CNES', 'CODUFMUN', 'SERV_ESP', 'CLASS_SR', 'SRVUNICO', 'REGSAUDE', 'MICR_REG', 'DISTRSAN',
            'DISTRADM', 'TPGESTAO', 'PF_PJ', 'CPF_CNPJ', 'NIV_DEP', 'ESFERA_A', 'ATIVIDAD', 'RETENCAO',
            'NATUREZA', 'CLIENTEL', 'TP_UNID', 'TURNO_AT', 'NIV_HIER', 'TERCEIRO', 'CNPJ_MAN', 'CARACTER',
            'AMB_HOSP', 'COMPETEN', 'CONTSRVU', 'CNESTERC', 'NAT_JUR', "FONTE"
        ]

        self.model = Table(
            self.table_name, self.metadata,
            Column('CNES', String(7), nullable=True),
            Column('CODUFMUN', String(6), nullable=True),
            Column('SERV_ESP', String(3), nullable=True),
            Column('CLASS_SR', String(3), nullable=True),
            Column('SRVUNICO', String(3), nullable=True),
            Column('REGSAUDE', String(4), nullable=True),
            Column('MICR_REG', String(6), nullable=True),
            Column('DISTRSAN', String(4), nullable=True),
            Column('DISTRADM', String(4), nullable=True),
            Column('TPGESTAO', String(1), nullable=True),
            Column('PF_PJ', String(1), nullable=True),
            Column('CPF_CNPJ', String(14), nullable=True),
            Column('NIV_DEP', String(1), nullable=True),
            Column('ESFERA_A', String(2), nullable=True),
            Column('ATIVIDAD', String(2), nullable=True),
            Column('RETENCAO', String(2), nullable=True),
            Column('NATUREZA', String(2), nullable=True),
            Column('CLIENTEL', String(2), nullable=True),
            Column('TP_UNID', String(2), nullable=True),
            Column('TURNO_AT', String(2), nullable=True),
            Column('NIV_HIER', String(2), nullable=True),
            Column('TERCEIRO', String(1), nullable=True),
            Column('CNPJ_MAN', String(14), nullable=True),
            Column('CARACTER', String(1), nullable=True),
            Column('AMB_HOSP', String(4), nullable=True),
            Column('COMPETEN', String(6), nullable=True),
            Column('CONTSRVU', String(1), nullable=True),
            Column('CNESTERC', String(7), nullable=True),
            Column('NAT_JUR', String(4), nullable=True),
            Column('FONTE', String, nullable=False)
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

