for i, row in sih_df.iterrows():
    s = row["MUNIC_RES"]
    t = row["MUNIC_MOV"]
    G.nodes[t]["weight"] = 1  # sum of in-patients
    if G.has_edge(s, t):
        G[s][t]["weight"] += 1  # sum of patients in this edge
        G[s][t]["source_macro"] = G.nodes[s]["macro_name"]
    else:
        G.add_edge(s, t, weight=1, alpha=1, source_macro=G.nodes[s]["macro_name"])


nx.set_node_attributes(G, dict(G.in_degree()), name="in_degree")
nx.set_node_attributes(G, dict(G.out_degree()), name="out_degree")

for n in G.nodes:
    G.nodes[n]["scaled_in_degree"] = 1
    if G.in_degree(n) > 0:
        G.nodes[n]["scaled_in_degree"] = max(1, np.log(G.in_degree(n)))

for e in G.edges:
    G[e[0]][e[1]]["log_weight"] = 1 + np.log(G[e[0]][e[1]]["weight"])

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
    edge_lw_by="log_weight",
    edge_palette=pal,
    # edge_alpha_by="weight",
)
# annotate.node_colormapping(G,
#                            color_by="macro_name",
#                            legend_kwargs=legend_kwargs,
#                            palette=palette[:5])

annotate.circos_group(G, group_by="macro_name")
# annotate.circos_labels(G, group_by="macro_name", layout="rotate")
plt.tight_layout(rect=(0.05, 0.05, 0.95, 0.95))

plt.show()
# %%

# %%
