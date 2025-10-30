# usa pysus para fazer downloads do SIH
from pysus import SIH
from pathlib import Path
import logging

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

# carrega o sih
sih = SIH().load()
print(sih.groups)

# baixa os arquivos por tipo, uf e ano e salva localmente
# não vai baixar os arquivos (pastas) que já existem em local_dir
data_folder =  Path.home().joinpath("Workspace", "pyopensus", "data")
local_dir = data_folder.joinpath("sihsus")

prefix_list = ["RD", "RJ", "SP"]
uf_list = ["CE"]
year_list = [2008,2009,2010,2011,2012,2013,2014,2015]
year_list = [2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]

files = sih.get_files(prefix_list, uf=uf_list, year=year_list);
sih.download(files,local_dir=local_dir)

)