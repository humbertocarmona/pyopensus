# %%
import sqlite3
import pandas as pd
import geopandas as gpd

# %%
conn = sqlite3.connect('../data/opendatasus/SIHSUS_NORDESTE_NO_SERVICE.db')

# %%
query = """
    SELECT * FROM aih_reduzida LIMIT 100
"""

df = pd.read_sql_query(query,conn)


# %%

br = gpd.read_file("../data/municipios_BR_2022.geojson")
city_names = dict(zip(br['CD_MUN'].str[:6],br['NM_MUN']))


# %%
"""
MUNIC_RES = código do município de residência
MUNIC_MOV = código do município de internação

TODO: preciso relacionar município com ADS
"""
print([city_names[i] for i in df['MUNIC_RES']])

# %%
