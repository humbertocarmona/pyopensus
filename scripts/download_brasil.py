import sys
from pathlib import Path
sys.path.append("..")
from pyopensus import Opensus

base = "SIHSUS"
inicial = '2008'
final = '2023'
to_dbf = True
preffix = "RD"
verbose = True
output = Path.home().joinpath("Documents", "data", "opendatasus", "sihsus")

# -------------- connect --------------
opensus = Opensus()

uf_all = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
           "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
           "RS", "RO", "RR", "SC", "SP", "SE", "TO"]
uf_ne = ['AL', 'BA', 'MA', 'CE', 'RN', 'SE', 'PI', 'PB', 'PE']
uf_sul = ['SC', 'PR', 'RS']
ano_range = range(int(inicial), int(final)+1)

for uf in uf_ne:
    for ano in ano_range:
        opensus.retrieve_year(output, base, uf, ano, preffix=preffix, to_dbf=to_dbf, verbose=verbose)