# scripts/06_build_networks_per_episode.py

import os
import json
import networkx as nx
import pandas as pd

os.makedirs("data_processed/episodes", exist_ok=True)

def normaliza_name(s):
    if s is None: return ""
    s = str(s).strip()
    return " ".join(s.split())

episode_files = {
    1: "data_raw/starwars-episode-1-interactions-allCharacters.json",
    2: "data_raw/starwars-episode-2-interactions-allCharacters.json",
    3: "data_raw/starwars-episode-3-interactions-allCharacters.json",
    4: "data_raw/starwars-episode-4-interactions-allCharacters.json",
    5: "data_raw/starwars-episode-5-interactions-allCharacters.json",
    6: "data_raw/starwars-episode-6-interactions-allCharacters.json",
}

for epi, path in episode_files.items():

    print(f"\nProcesando episodio {epi}...")

    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # map index -> name
    nodes = data["nodes"]
    idx_to_name = {}
    for i, n in enumerate(nodes):
        name = n["name"] if isinstance(n, dict) else str(n)
        idx_to_name[i] = normaliza_name(name)

    # extract edges
    rows = []
    for link in data["links"]:
        s = idx_to_name[link["source"]]
        t = idx_to_name[link["target"]]
        w = link.get("value", 1)
        if s == t:
            continue
        # undirected normalization
        a, b = sorted([s, t])
        rows.append((a, b, w))

    df = pd.DataFrame(rows, columns=["source", "target", "weight"])
    df = df.groupby(["source", "target"], as_index=False)["weight"].sum()

    # filenames
    out_csv = f"data_processed/episodes/episode{epi}_edges.csv"
    out_gexf = f"data_processed/episodes/episode{epi}.gexf"

    # save CSV
    df.to_csv(out_csv, index=False)

    # create graph and save GEXF
    G = nx.Graph()
    for _, r in df.iterrows():
        G.add_edge(r["source"], r["target"], weight=float(r["weight"]))

    nx.write_gexf(G, out_gexf)

    print(f"Episodio {epi}: nodos = {G.number_of_nodes()} | enlaces = {G.number_of_edges()}")
    print(f"- CSV  → {out_csv}")
    print(f"- GEXF → {out_gexf}")

print("\n✓ Redes individuales por episodio generadas correctamente.")
