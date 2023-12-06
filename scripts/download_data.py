import os
import sys
sys.path.append("..")

import argparse
from pyopensus import Opensus

# -------------- interface --------------

parser = argparse.ArgumentParser(
    description="Download das bases anonimizadas disponíveis pelo DATASUS.",
    add_help=True
)

parser.add_argument('--base')
parser.add_argument('--uf')
parser.add_argument('--ano-inicial')
parser.add_argument('--ano-final')
parser.add_argument('--dbf', type=bool, default=False)
parser.add_argument('--output')
parser.add_argument('--preffix', default=None)
parser.add_argument('--verbose', type=bool, default=False)

args = parser.parse_args()

base = args.base
uf = args.uf
inicial = args.ano_inicial
final = args.ano_final
to_dbf = args.dbf
output = args.output
preffix = args.preffix
verbose = args.verbose

# -------------- connect --------------
opensus = Opensus()

ano_range = range(int(inicial), int(final)+1)
for ano in ano_range:
    opensus.retrieve_year(output, base, uf, ano, preffix=preffix, to_dbf=to_dbf, verbose=verbose)

