# scripts/01_parse_jsons.py
import json, os, glob
import pandas as pd
import networkx as nx

os.makedirs("data_processed", exist_ok=True)

# Ajusta si tienes otro patrón
files = sorted(glob.glob("data_raw/starwars-episode-*-interactions-allCharacters.json"))

def normaliza_name(s):
    if s is None: return ""
    s = str(s).strip()
    s = " ".join(s.split())
    return s

for f in files:
    with open(f, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    # nodes list: convert index -> name
    nodes = data.get("nodes", [])
    index_to_name = {}
    for i, n in enumerate(nodes):
        name = n.get("name") if isinstance(n, dict) else str(n)
        name = normaliza_name(name)
        index_to_name[i] = name

    # links: use source/target indices -> names
    links = data.get("links", [])
    rows = []
    for link in links:
        s_idx = link.get("source")
        t_idx = link.get("target")
        weight = link.get("value", 1)
        s_name = index_to_name.get(s_idx, f"unknown_{s_idx}")
        t_name = index_to_name.get(t_idx, f"unknown_{t_idx}")
        # ignore self-loops
        if s_name == t_name:
            continue
        # keep undirected ordering (min,max) to aggregate later easily
        if s_name <= t_name:
            rows.append((s_name, t_name, weight))
        else:
            rows.append((t_name, s_name, weight))

    df = pd.DataFrame(rows, columns=["source","target","weight"])
    # aggregate possible duplicate pairs within same file
    df = df.groupby(["source","target"], as_index=False)["weight"].sum()

    base = os.path.basename(f).replace(".json","")
    out_csv = f"data_processed/{base}_edges.csv"
    out_gexf = f"data_processed/{base}.gexf"

    df.to_csv(out_csv, index=False)

    # build graph and export gexf
    G = nx.Graph()
    for _, r in df.iterrows():
        G.add_edge(r['source'], r['target'], weight=float(r['weight']))
    nx.write_gexf(G, out_gexf)

    print("Processed:", f, "->", out_csv, "nodes:", G.number_of_nodes(), "edges:", G.number_of_edges())
