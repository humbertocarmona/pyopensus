# %%
import sqlite3
import pandas as pd
import geopandas as gpd
import networkx as nx
import numpy as np

import nxviz as nv
from nxviz import annotate

import matplotlib.pyplot as plt

# pio.renderers.default = "browser"   # open in your system browser
pal = [
    "#1f77b4",
    "#ff7f0e",
    "#279e68",
    "#d62728",
    "#aa40fc",
    "#8c564b",
]


palette = {'Cariri':"#1f77b4", 
 'Norte':"#aa40fc", 
 'Fortaleza':"#ff7f0e", 
 'Sertão Central':"#279e68", 
 'Litoral Leste':"#d62728"}

# %%
conn = sqlite3.connect("../data/opendatasus/SIHSUS_CE_2019-2019.db")

# %%
query = """
    SELECT * FROM aih_reduzida
"""

sih_df = pd.read_sql_query(query, conn)


# %%
br = gpd.read_file("../data/municipios_BR_2022.geojson")
city_names = dict(zip(br["CD_MUN"].astype(str).str[:6], br["NM_MUN"]))
city_state = dict(zip(br["CD_MUN"].astype(str).str[:6], br["SIGLA_UF"]))

# %%

ce = gpd.read_file("../data/municipios_CE_2022_Macros.geojson")
ce["MACRO_NOME"] = ce["MACRO_NOME"].str.replace("Superintendência Regional de Saúde ","")

ce_macro = dict(zip(ce["CD_MUN"].astype(str).str[:6], ce["MACRO_ID"]))
ce_macro_name = dict(zip(ce["CD_MUN"].astype(str).str[:6], ce["MACRO_NOME"]))

# %%
"""
MUNIC_RES = código do município de residência
MUNIC_MOV = código do município de internação
"""

G = nx.DiGraph()
# CE only
sih_df = sih_df[sih_df['MUNIC_RES'].str.startswith('23') & sih_df['MUNIC_MOV'].str.startswith('23')]

nodes = set(sih_df["MUNIC_RES"])
nodes = nodes | set(sih_df["MUNIC_MOV"])
nodes = sorted(list(nodes))



macro_ids = []
macro_names = []
for n in nodes:
    if ce_macro.get(n) is not None:
        macro_ids.append(ce_macro[n])
        macro_names.append(ce_macro_name[n])
    else:
        macro_ids.append(9)
        macro_names.append("UNK")
        # macro_ids.append(city_state.get(n, "UNK"))
        # macro_names.append(city_state.get(n, "UNK"))


city_macro_ids = dict(zip(nodes, macro_ids))
city_macro_names = dict(zip(nodes, macro_names))


for n in nodes:
    G.add_node(
        n,
        weight=0,
        mun_name=city_names[n],
        macro_id=city_macro_ids[n],
        macro_name=city_macro_names[n],
    )



for i, row in sih_df.iterrows():
    s = row["MUNIC_RES"]
    t = row["MUNIC_MOV"]
    G.nodes[t]["weight"] = 1  # sum of in-patients
    if G.has_edge(s, t):
        G[s][t]["weight"] += 1  # sum of patients in this edge
        G[s][t]["source_macro"] = G.nodes[s]["macro_name"]
    else:
        G.add_edge(s, t, weight=1, alpha=1, source_macro=G.nodes[s]["macro_name"] )


nx.set_node_attributes(G, dict(G.in_degree()), name="in_degree")
nx.set_node_attributes(G, dict(G.out_degree()), name="out_degree")

for n in G.nodes:
    G.nodes[n]["scaled_in_degree"]  = 1
    if G.in_degree(n) > 0:
        G.nodes[n]["scaled_in_degree"] = max(1,np.log(G.in_degree(n)))
    
for e in G.edges:
    G[e[0]][e[1]]["log_weight"] = 1+np.log(G[e[0]][e[1]]["weight"])
    
mapping = {n: d["mun_name"] for n, d in G.nodes(data=True)}
nx.relabel_nodes(G, mapping, copy=False)
# G = nx.relabel_nodes(G, {i: "long name #" + str(i) for i in range(len(G))})

# edges = nx.to_pandas_edgelist(G)  # source, target, weight
# nodes_df = pd.DataFrame.from_dict(dict(G.nodes(data=True)), orient="index")
# nodes_df.reset_index(inplace=True)
legend_kwargs = {
        "ncol": 1,
        "bbox_to_anchor": (1, 0.5),
        "frameon": False,
        "loc": "center left",
    }

ax = nv.circos(
    G,
    group_by="macro_name",
    sort_by="weight",
    node_color_by="macro_name",
    node_size_by="scaled_in_degree",
    node_palette=pal,
    edge_color_by="source_macro",
    edge_lw_by = "log_weight",
    edge_palette=pal,
    # edge_alpha_by="weight",
)
# annotate.node_colormapping(G, 
#                            color_by="macro_name", 
#                            legend_kwargs=legend_kwargs,
#                            palette=palette[:5])

annotate.circos_group(G,group_by="macro_name")
# annotate.circos_labels(G, group_by="macro_name", layout="rotate")
plt.tight_layout(rect=(0.05, 0.05, 0.95, 0.95))

plt.show()
# %%

# %%
