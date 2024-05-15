import sys
from pathlib import Path
sys.path.append("..")
from pyopensus import Opensus

base = "SIHSUS"
inicial = '2024'
final = '2024'
to_dbf = True
preffix = "RD"
verbose = True
output = Path.home().joinpath("Documents", "data", "opendatasus", "sihsus")

# -------------- connect --------------
opensus = Opensus()

uf_ne = ['AL', 'BA', 'MA', 'CE', 'RN', 'SE', 'PI', 'PB', 'PE']
uf_sul = ['SC', 'PR', 'RS']
uf_sudeste = ['ES', 'MG', 'RJ', 'SP']
uf_centro = ['GO', 'MS', 'MT', "DF"]
uf_norte = ['AM', 'PA', 'AC', 'RR', 'RO', 'AP', 'TO']
uf_all = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
           "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
           "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
ano_range = range(int(inicial), int(final)+1)

for uf in uf_all:
    for ano in ano_range:
        try:
            opensus.retrieve_year(output, base, uf, ano, preffix=preffix, to_dbf=to_dbf, verbose=verbose)
        except:
            pass