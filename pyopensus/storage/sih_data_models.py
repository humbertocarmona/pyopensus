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


class AIH:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'aih_reduzida'
        self._dummy_ = ['UF_ZI','ANO_CMPT','MES_CMPT','ESPEC',
                        #'CGC_HOSP',
                        'N_AIH',
                        'IDENT',
                        #'CEP',
                        'MUNIC_RES','NASC','SEXO',
                        #'UTI_MES_IN','UTI_MES_AN', 'UTI_MES_AL',
                        'UTI_MES_TO','MARCA_UTI',
                        #'UTI_INT_IN','UTI_INT_AN', 'UTI_INT_AL',
                        'UTI_INT_TO','DIAR_ACOM','QT_DIARIAS','PROC_SOLIC',
                        'PROC_REA','VAL_SH','VAL_SP',
                        #'VAL_SADT','VAL_RN','VAL_ACOMP','VAL_ORTP',
                        #'VAL_SANGUE','VAL_SADTSR','VAL_TRANSP','VAL_OBSANG','VAL_PED1AC',
                        'VAL_TOT','VAL_UTI','US_TOT', 'DT_INTER', 'DT_SAIDA','DIAG_PRINC',
                        'DIAG_SECUN','COBRANCA','NATUREZA',
                        #'IND_VDRL',
                        'MUNIC_MOV','COD_IDADE','IDADE',
                        #'DIAS_PERM',
                        'MORTE',
                        'NACIONAL',
                        #'NUM_PROC',
                        'CAR_INT',
                        #'TOT_PT_SP',
                        #'CPF_AUT','HOMONIMO',
                        #'NUM_FILHOS','INSTRU','CID_NOTIF','CONTRACEP1','CONTRACEP2','GESTRISCO',
                        #'INSC_PN','SEQ_AIH5','CBOR','CNAER', 'VINCPREV',
                        'GESTOR_COD','GESTOR_TP',
                        #'GESTOR_CPF','GESTOR_DT',
                        'CNES',
                        #'CNPJ_MANT',
                        #'INFEHOSP','CID_ASSO',
                        'CID_MORTE','COMPLEX','FINANC',
                        #'FAEC_TP','REGCT',
                        #'RACA_COR',
                        #'ETNIA',
                        #'REMESSA',
                        "FONTE"]

        self.model = Table(
            self.table_name, self.metadata,
            Column('N_AIH', String(13), primary_key=True),
            Column('UF_ZI', String(6), nullable=True),
            Column('ANO_CMPT', String(4), nullable=True),
            Column('MES_CMPT', String(2), nullable=True),
            Column('ESPEC', String(2), nullable=True),
            #Column('CGC_HOSP', String(14), nullable=True),
            Column('IDENT', String(1), nullable=True),
            #Column('CEP', String(8), nullable=True),
            Column('MUNIC_RES', String(6), nullable=True),
            Column('NASC', DateTime, nullable=True),
            Column('SEXO', String(1), nullable=True),
            #Column('UTI_MES_IN', Numeric(2), nullable=True),
            #Column('UTI_MES_AN', Numeric(2), nullable=True),
            #Column('UTI_MES_AL', Numeric(2), nullable=True),
            Column('UTI_MES_TO', Numeric(3), nullable=True),
            Column('MARCA_UTI', String(2), nullable=True),
            #Column('UTI_INT_IN', Numeric(2), nullable=True),
            #Column('UTI_INT_AN', Numeric(2), nullable=True),
            #Column('UTI_INT_AL', Numeric(2), nullable=True),
            Column('UTI_INT_TO', Numeric(3), nullable=True),
            Column('DIAR_ACOM', Numeric(3), nullable=True),
            Column('QT_DIARIAS', Numeric(3), nullable=True),
            Column('PROC_SOLIC', String(10), nullable=True),
            Column('PROC_REA', String(10), nullable=True),
            Column('VAL_SH', Numeric(13, 2), nullable=True),
            Column('VAL_SP', Numeric(13, 2), nullable=True),
            #Column('VAL_SADT', Numeric(13, 2), nullable=True),
            #Column('VAL_RN', Numeric(13, 2), nullable=True),
            #Column('VAL_ACOMP', Numeric(13, 2), nullable=True),
            #Column('VAL_ORTP', Numeric(13, 2), nullable=True),
            #Column('VAL_SANGUE', Numeric(13, 2), nullable=True),
            #Column('VAL_SADTSR', Numeric(11, 2), nullable=True),
            #Column('VAL_TRANSP', Numeric(13, 2), nullable=True),
            #Column('VAL_OBSANG', Numeric(11, 2), nullable=True),
            #Column('VAL_PED1AC', Numeric(11, 2), nullable=True),
            Column('VAL_TOT', Numeric(14, 2), nullable=True),
            Column('VAL_UTI', Numeric(8, 2), nullable=True),
            Column('US_TOT', Numeric(10, 2), nullable=True),
            Column('DT_INTER', DateTime, nullable=True),
            Column('DT_SAIDA', DateTime, nullable=True),
            Column('DIAG_PRINC', String(4), nullable=True),
            Column('DIAG_SECUN', String(4), nullable=True),
            Column('COBRANCA', String(2), nullable=True),
            Column('NATUREZA', String(2), nullable=True),
            #Column('RUBRICA', Numeric(5), nullable=True),
            #Column('IND_VDRL', String(1), nullable=True),
            Column('MUNIC_MOV', String(6), nullable=True),
            Column('COD_IDADE', String(1), nullable=True),
            Column('IDADE', Numeric(2), nullable=True),
            #Column('DIAS_PERM', Numeric(5), nullable=True),
            Column('MORTE', Numeric(1), nullable=True),
            Column('NACIONAL', String(2), nullable=True),
            #Column('NUM_PROC', String(4), nullable=True),
            Column('CAR_INT', String(2), nullable=True),
            #Column('TOT_PT_SP', Numeric(6), nullable=True),
            #Column('CPF_AUT', String(11), nullable=True),
            #Column('HOMONIMO', String(1), nullable=True),
            #Column('NUM_FILHOS', Numeric(2), nullable=True),
            #Column('INSTRU', String(1), nullable=True),
            #Column('CID_NOTIF', String(4), nullable=True),
            #Column('CONTRACEP1', String(2), nullable=True),
            #Column('CONTRACEP2', String(2), nullable=True),
            #Column('GESTRISCO', String(1), nullable=True),
            #Column('INSC_PN', String(12), nullable=True),
            #Column('SEQ_AIH5', String(3), nullable=True),
            #Column('CBOR', String(3), nullable=True),
            #Column('CNAER', String(3), nullable=True),
            #Column('VINCPREV', String(1), nullable=True),
            Column('GESTOR_COD', String(3), nullable=True),
            Column('GESTOR_TP', String(1), nullable=True),
            #Column('GESTOR_CPF', String(11), nullable=True),
            #Column('GESTOR_DT', String(8), nullable=True),
            Column('CNES', String(7), nullable=True),
            #Column('CNPJ_MANT', String(14), nullable=True),
            #Column('INFEHOSP', String(1), nullable=True),
            #Column('CID_ASSO', String(4), nullable=True),
            Column('CID_MORTE', String(4), nullable=True),
            Column('COMPLEX', String(2), nullable=True),
            Column('FINANC', String(2), nullable=True),
            #Column('FAEC_TP', String(6), nullable=True),
            #Column('REGCT', String(4), nullable=True),
            #Column('RACA_COR', String(4), nullable=True),
            #Column('ETNIA', String(4), nullable=True),
            #Column('SEQUENCIA', Numeric(9), nullable=True),
            #Column('REMESSA', String(21), nullable=True),
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


class ServicosAIH:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'servicos_profissionais'
        self._dummy_ = ['SP_GESTOR',#'SP_UF',
                        'SP_AA','SP_MM','SP_CNES','SP_NAIH',
                        'SP_PROCREA',#'SP_DTINTER','SP_DTSAIDA',
                        #'SP_NUM_PR','SP_TIPO',
                        #'SP_CPFCGC',
                        'SP_ATOPROF',
                        #'SP_TP_ATO',
                        'SP_QTD_ATO','SP_PTSP',
                        'SP_VALATO','SP_M_HOSP','SP_M_PAC',
                        #'SP_DES_HOS','SP_DES_PAC',
                        'SP_COMPLEX','SP_FINANC',
                        #'SP_CO_FAEC',
                        'SP_PF_CBO',
                        #'SP_PF_DOC','SP_PJ_DOC',
                        #'IN_TP_VAL',
                        #'SEQUENCIA','REMESSA',
                        'SP_CIDPRI',
                        #'SP_CIDSEC',
                        'SP_QT_PROC','SP_U_AIH', 'FONTE']

        self.model = Table(
            self.table_name, self.metadata,
            Column('SP_NAIH', String(13), ForeignKey("aih_reduzida.N_AIH")),
            Column('SP_GESTOR', String(6), nullable=True),
            #Column('SP_UF', String(2), nullable=True),
            Column('SP_AA', String(4), nullable=True),
            Column('SP_MM', String(2), nullable=True),
            Column('SP_CNES', String(7), nullable=True),
            Column('SP_PROCREA', String(10), nullable=True),
            #Column('SP_DTINTER', String(8), nullable=True),
            #Column('SP_DTSAIDA', String(8), nullable=True),
            #Column('SP_NUM_PR', String(8), nullable=True),
            #Column('SP_TIPO', String(2), nullable=True),
            #Column('SP_CPFCGC', String(14), nullable=True),
            Column('SP_ATOPROF', String(10), nullable=True),
            #Column('SP_TP_ATO', String(2), nullable=True),
            Column('SP_QTD_ATO', Numeric(4), nullable=True),
            Column('SP_PTSP', Numeric(6), nullable=True),
            #Column('SP_NF', String(8), nullable=True),
            Column('SP_VALATO', Numeric(14, 2), nullable=True),
            Column('SP_M_HOSP', String(6), nullable=True),
            Column('SP_M_PAC', String(6), nullable=True),
            #Column('SP_DES_HOS', String(1), nullable=True),
            #Column('SP_DES_PAC', String(1), nullable=True),
            Column('SP_COMPLEX', String(2), nullable=True),
            Column('SP_FINANC', String(2), nullable=True),
            #Column('SP_CO_FAEC', String(6), nullable=True),
            Column('SP_PF_CBO', String(6), nullable=True),
            #Column('SP_PF_DOC', String(15), nullable=True),
            #Column('SP_PJ_DOC', String(14), nullable=True),
            #Column('IN_TP_VAL', String(1), nullable=True),
            #Column('SEQUENCIA', String(9), nullable=True),
            #Column('REMESSA', String(21), nullable=True),
            Column('SP_CIDPRI', String(4), nullable=True),
            #Column('SP_CIDSEC', String(4), nullable=True),
            Column('SP_QT_PROC', Numeric(4), nullable=True),
            Column('SP_U_AIH', String(1), nullable=True),
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

class Rejeitadas:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'aih_rejeitada'
        self._dummy_ = ['UF_ZI', 'ANO_CMPT', 'MES_CMPT', 
                        #'ESPEC', 'CGC_HOSP', 'N_AIH', 'IDENT', 'CEP', 
                        'MUNIC_RES', 'NASC', 'SEXO', 
                        #'UTI_MES_IN', 'UTI_MES_AN', 'UTI_MES_AL', 
                        'UTI_MES_TO', 'MARCA_UTI', 
                        #'UTI_INT_IN', 'UTI_INT_AN', 'UTI_INT_AL', 
                        'UTI_INT_TO', 
                        #'DIAR_ACOM', 'QT_DIARIAS', 
                        'PROC_SOLIC',
                        'PROC_REA', 'VAL_SH', 'VAL_SP', 
                        #'VAL_SADT', 'VAL_RN', 'VAL_ACOMP',
                        #'VAL_ORTP', 'VAL_SANGUE', 'VAL_SADTSR', 'VAL_TRANSP', 'VAL_OBSANG',
                        #'VAL_PED1AC', 
                        'VAL_TOT', 'VAL_UTI', 'US_TOT', 'DT_INTER', 'DT_SAIDA',
                        'DIAG_PRINC', 'DIAG_SECUN', 'COBRANCA', 
                        #'NATUREZA', 'NAT_JUR', 'GESTAO',
                        #'RUBRICA', 'IND_VDRL', 
                        'MUNIC_MOV', 'COD_IDADE', 'IDADE', 
                        #'DIAS_PERM',
                        'MORTE', 'NACIONAL', 
                        #'NUM_PROC', 
                        'CAR_INT', 
                        #'TOT_PT_SP', 
                        #'CPF_AUT',
                        #'HOMONIMO', 'NUM_FILHOS', 'INSTRU', 'CID_NOTIF', 'CONTRACEP1',
                        #'CONTRACEP2', 'GESTRISCO', 'INSC_PN', 'SEQ_AIH5', 'CBOR', 'CNAER',
                        #'VINCPREV', 'GESTOR_COD', 'GESTOR_TP', 'GESTOR_CPF', 'GESTOR_DT',
                        'CNES', 
                        #'CNPJ_MANT', 'INFEHOSP', 'CID_ASSO', 'CID_MORTE', 'COMPLEX',
                        #'FINANC', 'FAEC_TP', 'REGCT', 
                        'RACA_COR', 
                        #'ETNIA', 
                        'ST_SITUAC',
                        'ST_BLOQ', 'ST_MOT_BLO', 
                        #'SEQUENCIA', 
                        #'REMESSA'
                        'FONTE']

        self.model = Table(
            self.table_name, self.metadata,
            Column('N_AIH', String(13), ForeignKey("aih_reduzida.N_AIH")),
            Column('UF_ZI', String(6), nullable=True),
            Column('ANO_CMPT', String(4), nullable=True),
            Column('MES_CMPT', String(2), nullable=True),
            #Column('ESPEC', String(2), nullable=True),
            #Column('CGC_HOSP', String(14), nullable=True),
            #Column('IDENT', String(1), nullable=True),
            #Column('CEP', String(8), nullable=True),
            Column('MUNIC_RES', String(6), nullable=True),
            Column('NASC', DateTime, nullable=True),
            Column('SEXO', String(1), nullable=True),
            #Column('UTI_MES_IN', Numeric(2), nullable=True),
            #Column('UTI_MES_AN', Numeric(2), nullable=True),
            #Column('UTI_MES_AL', Numeric(2), nullable=True),
            Column('UTI_MES_TO', Numeric(3), nullable=True),
            Column('MARCA_UTI', String(2), nullable=True),
            #Column('UTI_INT_IN', Numeric(2), nullable=True),
            #Column('UTI_INT_AN', Numeric(2), nullable=True),
            #Column('UTI_INT_AL', Numeric(2), nullable=True),
            Column('UTI_INT_TO', Numeric(3), nullable=True),
            #Column('DIAR_ACOM', Numeric(3), nullable=True),
            #Column('QT_DIARIAS', Numeric(3), nullable=True),
            Column('PROC_SOLIC', String(10), nullable=True),
            Column('PROC_REA', String(10), nullable=True),
            Column('VAL_SH', Numeric(13, 2), nullable=True),
            Column('VAL_SP', Numeric(13, 2), nullable=True),
            #Column('VAL_SADT', Numeric(13, 2), nullable=True),
            #Column('VAL_RN', Numeric(13, 2), nullable=True),
            #Column('VAL_ACOMP', Numeric(13, 2), nullable=True),
            #Column('VAL_ORTP', Numeric(13, 2), nullable=True),
            #Column('VAL_SANGUE', Numeric(13, 2), nullable=True),
            #Column('VAL_SADTSR', Numeric(11, 2), nullable=True),
            #Column('VAL_TRANSP', Numeric(13, 2), nullable=True),
            #Column('VAL_OBSANG', Numeric(11, 2), nullable=True),
            #Column('VAL_PED1AC', Numeric(11, 2), nullable=True),
            Column('VAL_TOT', Numeric(14, 2), nullable=True),
            Column('VAL_UTI', Numeric(8, 2), nullable=True),
            Column('US_TOT', Numeric(10, 2), nullable=True),
            Column('DT_INTER', DateTime, nullable=True),
            Column('DT_SAIDA', DateTime, nullable=True),
            Column('DIAG_PRINC', String(4), nullable=True),
            Column('DIAG_SECUN', String(4), nullable=True),
            Column('COBRANCA', String(2), nullable=True),
            #Column('NATUREZA', String(2), nullable=True),
            #Column('NAT_JUR', String(5), nullable=True),
            #Column('GESTAO', String(3), nullable=True),
            #Column('RUBRICA', Numeric(5), nullable=True),
            #Column('IND_VDRL', String(1), nullable=True),
            Column('MUNIC_MOV', String(6), nullable=True),
            Column('COD_IDADE', String(1), nullable=True),
            Column('IDADE', Numeric(2), nullable=True),
            #Column('DIAS_PERM', Numeric(5), nullable=True),
            Column('MORTE', Numeric(1), nullable=True),
            Column('NACIONAL', String(2), nullable=True),
            #Column('NUM_PROC', String(4), nullable=True),
            Column('CAR_INT', String(2), nullable=True),
            #Column('TOT_PT_SP', Numeric(6), nullable=True),
            #Column('CPF_AUT', String(11), nullable=True),
            #Column('HOMONIMO', String(1), nullable=True),
            #Column('NUM_FILHOS', Numeric(2), nullable=True),
            #Column('INSTRU', String(1), nullable=True),
            #Column('CID_NOTIF', String(4), nullable=True),
            #Column('CONTRACEP1', String(2), nullable=True),
            #Column('CONTRACEP2', String(2), nullable=True),
            #Column('GESTRISCO', String(1), nullable=True),
            #Column('INSC_PN', String(12), nullable=True),
            #Column('SEQ_AIH5', String(3), nullable=True),
            #Column('CBOR', String(3), nullable=True),
            #Column('CNAER', String(3), nullable=True),
            #Column('VINCPREV', String(1), nullable=True),
            #Column('GESTOR_COD', String(3), nullable=True),
            #Column('GESTOR_TP', String(1), nullable=True),
            #Column('GESTOR_CPF', String(11), nullable=True),
            #Column('GESTOR_DT', String(8), nullable=True),
            Column('CNES', String(7), nullable=True),
            #Column('CNPJ_MANT', String(14), nullable=True),
            #Column('INFEHOSP', String(1), nullable=True),
            #Column('CID_ASSO', String(4), nullable=True),
            #Column('CID_MORTE', String(4), nullable=True),
            #Column('COMPLEX', String(2), nullable=True),
            #Column('FINANC', String(2), nullable=True),
            #Column('FAEC_TP', String(6), nullable=True),
            #Column('REGCT', String(4), nullable=True),
            Column('RACA_COR', String(4), nullable=True),
            #Column('ETNIA', String(4), nullable=True),
            Column('ST_SITUAC', String(1), nullable=True),
            Column('ST_BLOQ', String(1), nullable=True),
            Column('ST_MOT_BLO', String(2), nullable=True),
            #Column('SEQUENCIA', Numeric(9), nullable=True),
            #Column('REMESSA', String(21), nullable=True)
            #Column('FONTE', String, nullable=True)
        )

        self.mapping = {n: n for n in self._dummy_}

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem