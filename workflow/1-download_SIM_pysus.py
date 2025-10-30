# %%
from pathlib import Path
from pysus import SIM
import pandas as pd
import sys
import logging

sys.path.append("..")
from pyopensus.storage.whandler_sus import HandlerSIM

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Add handler if missing
if not logger.handlers:
    handler = logging.StreamHandler()  # prints to console
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# definitions
brazil_regions = {
    "North": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Northeast": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Central-West": ["DF", "GO", "MT", "MS"],
    "Southeast": ["ES", "MG", "RJ", "SP"],
    "South": ["PR", "RS", "SC"],
}
brazil_regions_names = {
    "North": ["Acre", "Amapá", "Amazonas", "Pará", "Rondônia", "Roraima", "Tocantins"],
    "Northeast": [
        "Alagoas",
        "Bahia",
        "Ceará",
        "Maranhão",
        "Paraíba",
        "Pernambuco",
        "Piauí",
        "Rio Grande do Norte",
        "Sergipe",
    ],
    "Central-West": ["Distrito Federal", "Goiás", "Mato Grosso", "Mato Grosso do Sul"],
    "Southeast": ["Espírito Santo", "Minas Gerais", "Rio de Janeiro", "São Paulo"],
    "South": ["Paraná", "Rio Grande do Sul", "Santa Catarina"],
}


def load_SIM_file(parque_filename):
    date_columns = [
        "DTOBITO",
        "DTNASC",
        "DTATESTADO",
        "DTINVESTIG",
        "DTCADASTRO",
        "DTRECEBIM",
        "DTRECORIGA",
        "DTCADINV",
        "DTCONINV",
        "DTCADINF",
        "DTCONCASO",
    ]

    hour_columns = ["HORAOBITO"]

    df = pd.read_parquet(parque_filename, engine="fastparquet")

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")

    for col in hour_columns:
        df[col] = df[col].astype(str).str.strip()
        mask = df[col].str.match(r"^\d{3,4}$")
        df[col] = pd.to_datetime(
            df.loc[mask, col].str.zfill(4), format="%H%M", errors="coerce"
        ).dt.time
    return df


def get_files(group, year=[], uf=[], local_dir=""):
    files = sim.get_files(group, year=year, uf=uf)
    parquet_files = [
        local_dir.joinpath(str(f).replace("dbc", "parquet")) for f in files
    ]
    parquet_files = [
        local_dir.joinpath(str(f).replace("DBC", "parquet")) for f in parquet_files
    ]
    if not all([f.exists() for f in parquet_files]):
        sim.download(files, local_dir=local_dir)
        parquet_files = [
            local_dir.joinpath(str(f).replace("dbc", "parquet")) for f in files
        ]
        parquet_files = [
            local_dir.joinpath(str(f).replace("DBC", "parquet")) for f in parquet_files
        ]
    for f in parquet_files:
        logger.info(f"file= {f.name}")
    return parquet_files


# %%
"""
- 'CID9'	ICD-9 cause-of-death group variable (before 1996) (DOR files)
- 'CID10'	ICD-10 cause-of-death group variable (1996 onward) (DO files)
"""
sim = SIM().load()
sim.groups
# %%

year = [2023]
uf = ["CE"]
data_dir = Path.home().joinpath("Workspace", "pyopensus", "data")
warehouse_location = data_dir.joinpath("opendatasus")
warehouse_name = f"SIMSUS_{year[0]}_{uf[0]}.db"
warehouse_injector = HandlerSIM(warehouse_location, warehouse_name)

local_dir = data_dir.joinpath("simsus")
local_dir.mkdir(exist_ok=True)

files = sim.get_files(["CID10"], year=year, uf=uf)

files


# %%
def inject_parque_files(year, uf, injector):
    group = ["CID10"]
    parquet_files = get_files(group, year, uf,local_dir)
    # inject into database
    for filename in parquet_files:
        df = load_SIM_file(filename)
        injector.insert_sim(df,filename.stem)

    logger.info(f"injected in  {injector.warehouse_name.stem}")
    return 1


# %%
inject_parque_files(year=year, uf=uf, injector=warehouse_injector)
# %%
