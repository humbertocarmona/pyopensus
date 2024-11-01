import sqlalchemy
from sqlalchemy import text, inspect, MetaData
import pandas as pd
import networkx as nx

# -- test
def query_metadata(engine):
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    table_dict = { table_name : inspector.get_columns(table_name) for table_name in tables }
    return table_dict


def perform_query(query_str, engine, batchsize=1000):

    schema_data = {
        'rows': [],
        'columns': [],
    }

    query_str = text(query_str)
    with engine.connect() as conn:
        qres = conn.execute(query_str)
        schema_data['columns'] = list(qres.keys())

        while True:
            rows = qres.fetchmany(batchsize)
            if not rows:
                break
            schema_data["rows"] += [ row for row in rows ]
    
    res_df = pd.DataFrame(schema_data['rows'], columns=schema_data['columns'])
    return res_df

def select_period_aih(engine, start_date, final_date, diag_level=0):
    '''
        Filter the AIH records for the period selected and considering the 
        diagnostic level of information required.

        'diag_level' refers to the number of chars to be considered in the 
        diagnostic ICD-10 of a hospital admission.
    '''
    if diag_level>4:
        diag_level = 4
    elif diag_level<0:
        diag_level = 0
    
    query = f'''
        SELECT 
            N_AIH, CNES, MUNIC_RES, MUNIC_MOV, VAL_TOT,
            SUBSTR(DIAG_PRINC,1,{diag_level}) as DIAG_CATEG 
        FROM aih_reduzida
        WHERE DT_INTER >= '{start_date.strftime("%Y-%m-%d")}' AND DT_INTER <= '{final_date.strftime("%Y-%m-%d")}'
    '''
    df = perform_query(query, engine)
    return df

def select_period_aih_services(engine, start_date, final_date):
    '''
        Filter the AIH records for the period selected and considering the 
        diagnostic level of information required.

        Here, the AIH records are selected together with all medical services performed
        for each hospital admission.
    '''
    query = f'''
        SELECT 
            a.*, b.SP_ATOPROF , b.SP_QTD_ATO
        FROM (
            SELECT 
                N_AIH, CNES, MUNIC_RES, MUNIC_MOV, ANO_CMPT, MES_CMPT
            FROM aih_reduzida
            WHERE DT_INTER >= '{start_date.strftime("%Y-%m-%d")}' AND DT_INTER <= '{final_date.strftime("%Y-%m-%d")}'
        ) a
        LEFT JOIN servicos_profissionais b
        WHERE a.N_AIH = b.SP_NAIH AND a.ANO_CMPT = b.SP_AA AND a.MES_CMPT = b.SP_MM
    '''
    df = perform_query(query, engine)
    df = df.drop_duplicates(subset=["N_AIH", "SP_ATOPROF"], keep='first')
    return df

def select_cnes_equip_data(engine, reference_date):
    '''
    
    '''
    query = f'''
        SELECT
            a.*, b.CODUFMUN, b.VINC_SUS, b.TPGESTAO,
            b.ESFERA_A, b.NATUREZA, b.TP_UNID
        FROM (
            SELECT
                *
            FROM equipamentos_mes
            WHERE COMPET = '{reference_date.strftime("%Y-%m-%d 00:00:00.000000")}'
        ) a
        LEFT JOIN cnes b
        WHERE a.CNES = b.CNES
    '''
    df = perform_query(query, engine)
    df["EQUIP_KEY"] = df["TIPEQUIP"]+'-'+df["CODEQUIP"]
    return df

# -- City2City Networks (C2C)

def edgelist_c2c(engine, start_date, final_date, diag_level, mode='people'):
    '''
        Create the edgelist for the City2City networks.

        Given a period between 'start_date' and 'final_date', the function retrieves hospital admission
        data for this period and create the edgelist with pairs of cities and their corresponding weights.
        For this network, the weight can be regarded as vector $\vec{w}$ of m entries. Each entry corresponds 
        to a group of disease. The coarsing of the group depends on 'diag_level'. For instance, if 'diag_level'=1,
        then each weight $w_i$ corresponds to a group of diseases given by the first letter of the ICD-10 code.
        If 'mode'='people', then the value of a weight will correspond to the amount of flux of people carried
        in the current edge.
        If 'mode'='money', then the value of a weight will correspond to the amount of flux of money carried in
        the current edge.

        Args:
        -----
            engine:
                sqlalchemy.engine.base.Engine. SQLAlchemy engine responsible for the connection to the database
                of fluxes.
            start_date:
                datetime.datetime.
            final_date:
                datetime.datetime.
            diag_level:
                Integer. Coarsening level of the ICD-10 code associated to the hospital admission. For instance, 
                if 'diag_level'=1, any flux of admissions due to disease 'B34.2' (Covid-19) will be grouped into
                the disease group 'B'. If 'diag_level'=2, the example will be grouped as 'B3'. If 'diag_level'=0,
                then the weight vector will have only one entry corresponding to the total sum of fluxes with no
                discrimination of disease.
            mode:
                String.
                Options=['people', 'money']. See description.

        Returns:
        --------
            edgelist:
                pandas.DataFrame.
    '''
    df = select_period_aih(engine, start_date, final_date, diag_level=diag_level)
    if mode == 'people':
        edgelist = df.groupby(["MUNIC_RES", "MUNIC_MOV"])["DIAG_CATEG"].value_counts().reset_index()
        edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "MUNIC_MOV"], columns="DIAG_CATEG", values="count").fillna(0)
    elif mode == 'money':
        edgelist = df.groupby(["MUNIC_RES", "MUNIC_MOV", "DIAG_CATEG"])["VAL_TOT"].sum().reset_index()
        edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "MUNIC_MOV"], columns="DIAG_CATEG", values="VAL_TOT").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    return edgelist

def edgelist_services_c2c(engine, start_date, final_date):
    '''
        Create the edgelist for the City2City networks where the weight vector will divided into
        medical services performed for each hospital admission.

        The expression "city i has a set of directed edges towards city j" in this case refers to all medical services
        that were exported from i to j through the flux of people.
    '''
    df = select_period_aih_services(engine, start_date, final_date)
    edgelist = df.groupby(["MUNIC_RES", "MUNIC_MOV"])["SP_ATOPROF"].value_counts().reset_index()
    edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "MUNIC_MOV"], columns="SP_ATOPROF", values="count").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    return edgelist

# -- City2Hospital Networks (C2H)

def edgelist_c2h(engine, start_date, final_date, diag_level, mode='people'):
    '''
        ...
    '''
    df = select_period_aih(engine, start_date, final_date, diag_level=diag_level)
    if mode == 'people':
        edgelist = df.groupby(["MUNIC_RES", "CNES"])["DIAG_CATEG"].value_counts().reset_index()
        edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "CNES"], columns="DIAG_CATEG", values="count").fillna(0)
    elif mode == 'money':
        edgelist = df.groupby(["MUNIC_RES", "CNES", "DIAG_CATEG"])["VAL_TOT"].sum().reset_index()
        edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "CNES"], columns="DIAG_CATEG", values="VAL_TOT").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    return edgelist

def edgelist_services_c2h(engine, start_date, final_date):
    '''
        ...
    '''
    df = select_period_aih_services(engine, start_date, final_date)
    edgelist = df.groupby(["MUNIC_RES", "CNES"])["SP_ATOPROF"].value_counts().reset_index()
    edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "CNES"], columns="SP_ATOPROF", values="count").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    return edgelist

# -- Hospital2Diseases Networks (H2D)
def edgelist_h2d(engine, start_date, final_date):
    df = select_period_aih(engine, start_date, final_date, diag_level=3)
    edgelist = df.groupby(["CNES"])["DIAG_CATEG"].value_counts().reset_index().rename({"count": "TOTAL"}, axis=1)
    edgelist_ = df.groupby(["CNES", "DIAG_CATEG"])["VAL_TOT"].sum().reset_index().rename({"VAL_TOT": "VALOR_TOTAL"}, axis=1)
    edgelist = edgelist.merge(edgelist_, on=["CNES", "DIAG_CATEG"], how="inner")
    return edgelist

# -- Hospital2HealthServices (H2HS)
def edgelist_h2hs(engine, start_date, final_date):
    df = select_period_aih_services(engine, start_date, final_date)
    edgelist = df.groupby(["CNES"])["SP_ATOPROF"].value_counts().reset_index().rename({"count": "TOTAL"}, axis=1)
    return edgelist

# -- Health equipaments (maybe also include physicians) to Hospital/Municipality (E2H and E2M)
def edgelist_equip2_h_m(engine, reference_date, mode="hospital"):
    '''
        ...

        Args:
        -----
            reference_date:
                datetime.datetime. Date for extracting year and month of the data point to create the network.
            mode:
                String. Options = ['hospital', 'municip']
    '''
    cnes_df = select_cnes_equip_data(engine, reference_date)
    cnes_df["QT_EXIST"] = cnes_df["QT_EXIST"].astype(int)
    cnes_df["QT_USO"] = cnes_df["QT_USO"].astype(int)
    if mode=='hospital':
        edgelist = cnes_df.groupby(["EQUIP_KEY", "CNES"])[["QT_EXIST", "QT_USO"]].sum().reset_index().rename({"count": "TOTAL"}, axis=1)
    elif mode=="municip":
        edgelist = cnes_df.groupby(["EQUIP_KEY", "CODUFMUN"])[["QT_EXIST", "QT_USO"]].sum().reset_index().rename({"count": "TOTAL"}, axis=1)
    else:
        raise Exception("'hospital' or 'municip' are the only available options.")
    return edgelist

def create_network(edgelist_df, net_type="c2c"):
    '''
    
    '''
    # -- default (c2c)
    if net_type=="c2c":
        source_nm, target_nm = "MUNIC_RES", "MUNIC_MOV" 
        graph = nx.DiGraph()
    elif net_type=="c2h":
        source_nm, target_nm = "MUNIC_RES", "CNES"
        graph = nx.DiGraph()
    elif net_type=="h2d":
        source_nm, target_nm = "CNES", "DIAG_CATEG"
        graph = nx.Graph() 
    elif net_type=="h2hs":
        source_nm, target_nm = "CNES", "SP_ATOPROF"
        graph = nx.Graph()
    elif net_type=="equip2hospital":
        source_nm, target_nm = "EQUIP_KEY", "CNES"
        graph = nx.Graph()
    elif net_type=="equip2mun":
        source_nm, target_nm = "EQUIP_KEY", "MUNIC_MOV"
        graph = nx.Graph()
    else:
        raise Exception("type not included.")  
        
    # Iterate through the dataframe and add edges with all edge attributes
    for idx, row in edgelist_df.iterrows():
        node_res = row[source_nm]
        node_mov = row[target_nm]
    
        # Get the edge attributes (all columns except the first two)
        edge_attributes = row.drop([source_nm, target_nm]).to_dict()
    
        # Add the edge with its attributes
        graph.add_edge(node_res, node_mov, **edge_attributes)
    return graph