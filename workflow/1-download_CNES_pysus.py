# %%
from pysus import CNES
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
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

# %%
cnes = CNES().load()
print(cnes.groups)

# %%
data_folder =  Path.home().joinpath("Workspace", "pyopensus", "data")
local_dir = data_folder.joinpath("cnessus")
prefixes = ["EP", "EQ", "LT", "ST", "PF", "SR"]

files = cnes.get_files(prefixes, uf=['CE'], year=[2023], month=['01'])
cnes.download(files, local_dir=local_dir)
# %%
