# scripts/02_build_global_network.py
import pandas as pd
import glob
import os
import networkx as nx

os.makedirs("data_processed", exist_ok=True)
edge_files = sorted(glob.glob("data_processed/*episode*-interactions-allCharacters_edges.csv"))

# si el patrón no matchea, intenta otro patrón:
if not edge_files:
    edge_files = sorted(glob.glob("data_processed/*episode-*-interactions-allCharacters_edges.csv"))

edges_all = []
for f in edge_files:
    df = pd.read_csv(f)
    # ensure columns
    if 'source' not in df.columns or 'target' not in df.columns:
        continue
    edges_all.append(df[['source','target','weight']])

if not edges_all:
    raise SystemExit("No edge files found in data_processed/. Revisa nombres.")

df_all = pd.concat(edges_all, ignore_index=True)
# agrupar y sumar pesos (red no dirigida)
df_agg = df_all.groupby(['source','target'], as_index=False)['weight'].sum()
# eliminar loops por si
df_agg = df_agg[df_agg['source'] != df_agg['target']]

OUT_CSV = "data_processed/starwars_edges_epi1-6.csv"
OUT_GEXF = "data_processed/starwars_network_epi1-6.gexf"
df_agg.to_csv(OUT_CSV, index=False)

G = nx.Graph()
for _, r in df_agg.iterrows():
    G.add_edge(r['source'], r['target'], weight=float(r['weight']))
nx.write_gexf(G, OUT_GEXF)

print("Global network exported:", OUT_CSV, OUT_GEXF)
print("N nodes:", G.number_of_nodes(), "L edges:", G.number_of_edges())
