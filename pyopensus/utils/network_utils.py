'''

'''
import pandas as pd
import datetime as dt

def select_period_aih(engine, start_date, final_date, diag_level=0):
    '''
        Perform the query over the aih database for a selected period.

        Args:
        -----
            engine
    '''
    if diag_level>4:
        diag_level = 4
    elif diag_level<0:
        diag_level = 0
    
    query = f'''
        SELECT 
            N_AIH, CNES, MUNIC_RES, MUNIC_MOV,
            SUBSTR(DIAG_PRINC,1,{diag_level}) as DIAG_CATEG 
        FROM aih_reduzida
        WHERE DT_INTER >= '{start_date.strftime("%Y-%m-%d")}' AND DT_INTER <= '{final_date.strftime("%Y-%m-%d")}'
    '''
    df = query_data(query, engine)
    return df

def select_period_servicos(engine, start_date, final_date, proc_level=6):
    '''
        Perform the query over the medical procedures database for a selected period.
    '''
    if proc_level>10:
        proc_level = 10
    elif proc_level<6:
        proc_level = 6

    query = f'''
        SELECT
            *
        FROM (
            SELECT 
                a.SP_NAIH, a.SP_CNES, a.SP_ATOPROF, 
                b.MUNIC_RES, b.MUNIC_MOV, b.DT_INTER
            FROM servicos_profissionais a
            LEFT JOIN aih_reduzida b
            ON a.SP_NAIH = b.N_AIH
        )
        WHERE DT_INTER >= '{start_date.strftime("%Y-%m-%d")}' AND DT_INTER <= '{final_date.strftime("%Y-%m-%d")}'
    '''
    df = query_data(query, engine)
    return df

def edgelist_citytocity(engine, start_date, final_date, diag_level):
    '''
        Create an edgelist for hospital flux between brazilian municipalities.

        The final edgelist is stored in a dataframe with indexes representing the
        municipalities (nodes) and the columns representing the edge weights.

        Weights refer to the number of people that came from a given municipality and
        was admitted in a hospital of another municipality (or the same if it is a self-edge).

        Args:
        -----
            engine:
                sqlalchemy.engine.base.Engine. engine used for database connection.
            start_date:
                datetime.datetime. Begininning of the period selected to build the network.
            final_date:
                datetime.datetime. Ending of the period selected to build the network.
            diag_level:
                integer. Hierarchical level of the ICD-10 code associated with the admission.
                If 'diag_level' is zero, then it will group the amount of fluxes between two 
                municipalities into a single value. If 'diag_level'=1, then it will group the
                fluxes into first-level ICD-10 codes, such as 'C' group (subgroup of neoplasm) 
                and 'I' (group of cardiovascular diseases). A weight called 'SOMA' will be 
                created to represent the sum of fluxes.

        Return:
        ------- 
            edgelist:
                pandas.DataFrame. 
    '''
    df = select_period_aih(engine, start_date, final_date, diag_level=diag_level)
    edgelist = df.groupby(["MUNIC_RES", "MUNIC_MOV"])["DIAG_CATEG"].value_counts().reset_index()
    edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "MUNIC_MOV"], columns="DIAG_CATEG", values="count").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    # -- when 'diag_level' is zero, a dummy column appears. remove it. 
    if '' in edgelist.columns:
        edgelist = edgelist.drop(columns='')
    return edgelist

def edgelist_citytocity_servicos(engine, start_date, final_date, proc_level):
    '''
        Create an edgelist for hospital flux between brazilian municipalities.

        The final edgelist is stored in a dataframe with indexes representing the
        municipalities (nodes) and the columns representing the edge weights.

        Weights refer to the number of people that came from a given municipality and
        was admitted in a hospital of another municipality (or the same if it is a self-edge).

        Args:
        -----
            engine:
                sqlalchemy.engine.base.Engine. engine used for database connection.
            start_date:
                datetime.datetime. Begininning of the period selected to build the network.
            final_date:
                datetime.datetime. Ending of the period selected to build the network.
            proc_level:
                integer. Hierarchical level of the SIGTAP code associated with the medical procedures
                done during admission. If 'proc_level' is 6 or lower, then it will group the amount 
                of fluxes between two municipalities into groups of procedure codes up to 6 digits.
                This can be done up to 10 digits, which include the most detailed information about
                the procedure.
        Return:
        ------- 
            edgelist:
                pandas.DataFrame. 
    '''
    df = select_period_servicos(engine, start_date, final_date, proc_level=proc_level)
    edgelist = df.groupby(["MUNIC_RES", "MUNIC_MOV"])["SP_ATOPROF"].value_counts().reset_index()
    edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "MUNIC_MOV"], columns="SP_ATOPROF", values="count").fillna(0)
    return edgelist


# ----------------------------------------------------------------------------------------------------
def edgelist_for_citytohospital(engine, start_date, final_date, diag_level):
    '''
        ...
    '''
    df = select_period_aih(engine, start_date, final_date, diag_level=diag_level)
    edgelist = df.groupby(["MUNIC_RES", "CNES"])["DIAG_CATEG"].value_counts().reset_index()
    edgelist = pd.pivot_table(edgelist, index=["MUNIC_RES", "CNES"], columns="DIAG_CATEG", values="count").fillna(0)
    edgelist["SOMA"] = edgelist.apply(sum, axis=1)
    return edgelist