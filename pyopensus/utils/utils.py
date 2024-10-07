import os
import subprocess
import datetime as dt
from sqlalchemy import text
from dateutil.relativedelta import relativedelta

# -- set path for R (need to make it more consistent)
# -- more consistent option so far it is using the R binary from conda

# -- still not working (check)
#if 'linux' in platform.system().lower(): # -- linux
#    os.environ["R_HOME"] = os.path.join(os.environ["CONDA_PREFIX"])
#else: # -- windows
#    os.environ["R_HOME"] = os.path.join(os.environ["CONDA_PREFIX"], "Lib", "R")

os.environ["R_HOME"] = os.path.join(os.environ["PROGRAMFILES"], "R", "R-4.3.1")
# -- sesa pc
#os.environ["R_HOME"] = os.path.join(os.environ["LOCALAPPDATA"], "Programs", "R", "R-4.3.1")
#os.environ["R_HOME"] = os.path.join(os.environ["LOCALAPPDATA"], "Programs", "R", "R-4.4.1")

import rpy2.robjects as robjects
from rpy2.robjects.packages import importr
from rpy2.robjects.packages import PackageNotInstalledError

# -- preffix dictionary
def preffix_dictionary():
    sih_hash = {
        'ER': 'AIH Rejeitadas com código de erro',
        'RD': 'AID Reduzida',
        'RJ': 'AIH Rejeitadas',
        'SP': 'Serviços Profissionais'
    }

    sia_hash = {
        'AB': 'APAC de acompanhamento a cirurgia bariátrica',
        'ABO': 'APAC de acompanhamento pós cirurgia bariátrica',
        'ACF': 'APAC confecção de fístula arteriovenose',
        'AD': 'APAC de laudos diversos',
        'AM': 'APAC de medicamentos',
        'AN': 'APAC de nefrologia',
        'AQ': 'APAC de quimioterapia',
        'AR': 'APAC de radioterapia',
        'TD': 'APAC Tratamento Dialítico - A Partir de Jun/2014',
        'PA': 'Produção Ambulatorial - A Partir de Jul/1994',
        'PS': 'Psicossocial - A Partir de Jan/2013',
        'SAD': 'Atenção Domiciliar - A Partir de Nov/2012',
    }

    cnes_hash = {
        'DC': 'Dados complementares',
        'EE': 'Estabelecimento de ensino',
        'EF': 'Estabelecimento filantrópico',
        'EP': 'Equipes',
        'EQ': 'Equipamentos',
        'GM': 'Gestão e Metas',
        'HB': 'Habilitação',
        'IN': 'Incentivos',
        'LT': 'Leitos',
        'PF': 'Profissional',
        'RC': 'Regra contratual',
        'SR': 'Serviço especializado',
        'ST': 'Estabelecimentos',
    }

    sinan_hash = {
        "DENG": "Dengue", 'CHIK': 'Chikungunya', "ACBI": "Acidentes Biológicos",
        "CHAG": "Chagas", "DIFT": "Difteria",
    }

    dictionaries = {'sihsus': sih_hash, 'siasus': sia_hash, 'cnes': cnes_hash, 'sinan': sinan_hash}
    return dictionaries

# -- functions

def find_r_executable():
    # Check if an environment variable is set
    r_path = os.getenv('R_HOME')
    if r_path:
        r_executable = os.path.join(r_path, 'bin', 'R')
        if os.path.exists(r_executable):
            return r_executable
    
    # Search common installation paths
    common_paths = [
        '/usr/bin/R',
        '/usr/local/bin/R',
        'C:\\Program Files\\R\\R-4.0.0\\bin\\R',  # Example for Windows
        'C:\\Program Files\\R\\R-4.1.0\\bin\\R'   # Update this for the expected versions
    ]
    for path in common_paths:
        if os.path.exists(path):
            return path
    
    # Check if R is in the system PATH
    try:
        r_executable = subprocess.check_output(['which', 'R']).decode('utf-8').strip()
        if os.path.exists(r_executable):
            return r_executable
    except subprocess.CalledProcessError:
        pass
    
    raise RuntimeError("R executable not found. Please install R or set the R_HOME environment variable.")


def dbc2dbf(path_to_dbc, path_to_dbf):
    try:
        read_dbc = importr('read.dbc')
    except PackageNotInstalledError as err:
        raise Exception("Need to install package from rpy2")

    read_dbc.dbc2dbf(path_to_dbc, path_to_dbf)


def perform_query(query_str, engine, batchsize=1000):
    '''
        Given a SQL query and the sqlalchemy engine corresponding to a
        database, performs and return the result of the query.
    '''

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


# -------------------------------------------
# --------- specifics for retrieval ---------
# -------------------------------------------

# ----------------- SIASUS/SIHSUS -----------------
def retrieve_siasih(baseftp, dest, uf, year, preffix, to_dbf, verbose):
    '''
    
    '''
    # -- validate year
    this_year = dt.date.today().year
    if year < 2008 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')

    year_str = f'{year}'[2:]
    filename = f"{preffix}{uf.upper()}{year_str}"
    month_lst = ['01', '02', '03', '04', '05', '06', 
                 '07', '08', '09', '10', '11', '12']

    if year==this_year:
        processed_date = dt.datetime.today() - relativedelta(months=2)
        if processed_date.year == this_year:
            month_lst = [ f'{n+1:2.0f}'.replace(" ", "0") for n in range(processed_date.month+1) ]

    for cur_month in month_lst:
        filename_dbc = f'{filename}{cur_month}.dbc'
        filename_dbf = f'{filename}{cur_month}.dbf'
        if verbose:
            print(f'Download do arquivo {filename_dbc} ...', end='')
            
        with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
            baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)
                
        # -- conversion to DBF
        if to_dbf:
            path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
            path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
            dbc2dbf(path_to_dbc, path_to_dbf)
            
        # --
        if verbose:
            print(' Feito.')

# ----------------- SIM/SINASC -----------------
def retrieve_vital(baseftp, dest, origin, uf, year, to_dbf, verbose):
    '''
    
    '''
    # -- validate year
    this_year = dt.date.today().year
    if year < 1996 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')
    
    year_str = f'{year}'
    if origin.lower()=='sim':
        filename = f"DO{uf.upper()}{year_str}"
    elif origin.lower()=='sinasc':
        filename = f"DN{uf.upper()}{year_str}"

    filename_dbc = f'{filename}.dbc'
    filename_dbf = f'{filename}.dbf'

    if verbose:
        print(f'Download do arquivo {filename_dbc} ...', end='')
    
    with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
        baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)

    # -- conversion to DBF
    if to_dbf:
        path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
        path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
        dbc2dbf(path_to_dbc, path_to_dbf)

    # --
    if verbose:
        print(' Feito.')


def retrieve_cnes(baseftp, dest, uf, year, preffix, to_dbf, verbose):
    '''
        ...
    '''
    # -- validate preffix
    preffixes = preffix_dictionary()["cnes"].keys()
    if preffix not in preffixes:
        raise Exception('Preffix does not match any available source.')
    else:
        baseftp.cwd(preffix)

    # -- validate year
    this_year = dt.date.today().year
    if year < 2005 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')
    
    year_str = f'{year}'[2:]
    filename = f"{preffix}{uf.upper()}{year_str}"
    month_lst = ['01', '02', '03', '04', '05', '06', 
                 '07', '08', '09', '10', '11', '12']
    
    if year==2005: month_lst = ['08', '09', '10', '11', '12']

    for cur_month in month_lst:
        filename_dbc = f'{filename}{cur_month}.dbc'
        filename_dbf = f'{filename}{cur_month}.dbf'
        if verbose:
            print(f'Download do arquivo {filename_dbc} ...', end='')
            
        with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
            baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)
                
        # -- conversion to DBF
        if to_dbf:
            path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
            path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
            dbc2dbf(path_to_dbc, path_to_dbf)
            
        if verbose:
            print(' Feito.')

def retrieve_sinan(baseftp, dest, year, preffix, to_dbf, verbose):
    '''
        Retrieve national SINAN data regarding specific disorder.

        Args:
        -----
            dest:
                String.
            year:
                Integer.
            preffix:
                String.
            to_dbf:
                Bool.
            verbose:
                Bool.
    '''
    # -- validate preffix
    preffixes = preffix_dictionary()["sinan"].keys()
    if preffix not in preffixes:
        raise Exception('Preffix does not match any available source.')
    else:
        pass
        #baseftp.cwd(preffix)

    # -- validate year
    this_year = dt.date.today().year
    if year < 2006 or year > this_year:
        raise Exception('Not able to retrieve parsed year.')
    
    year_str = f'{year}'[2:]
    filename = f"{preffix}BR{year_str}"
    filename_dbc = f'{filename}.dbc'
    filename_dbf = f'{filename}.dbf'

    if verbose:
        print(f'Download do arquivo {filename_dbc} ...', end='')
        
    with open(os.path.join(dest, "DBC", filename_dbc), 'wb') as fp:
        baseftp.retrbinary(f'RETR {filename_dbc}', fp.write)
            
    # -- conversion to DBF
    if to_dbf:
        path_to_dbc = os.path.join(dest, "DBC", filename_dbc)
        path_to_dbf = os.path.join(dest, "DBF", filename_dbf)
        dbc2dbf(path_to_dbc, path_to_dbf)
        
    if verbose:
        print(' Feito.')
