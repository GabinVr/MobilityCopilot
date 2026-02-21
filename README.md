# MobilityCopilot


# Run the app
```bash
python -m venv .venv
source .venv/bin/activate 
pip install -r requirements.txt
python main.py
```


# File structure
```
mobilitycopilot/
├── .env                    # Clés API, configurations, etc.
├── requirements.txt        
├── main.py                 # FastAPI serveur et routes API UNIQUEMENT ! pas de logique métier ici
├── core/
│   ├── state.py            # La logique de gestion d'état
│   ├── graph.py            # Le workflow de raisonnement (le graphe de nœuds)
│   ├── nodes.py            # La logique métier de chaque nœud (ex: exécution SQL...)
│   └── tools.py            # Execution SQL/pandas
├── data/
│   ├── ingest.py           # Script qui ingère les données (CSV, API, etc.) et les stocke dans db.sqlite 
│   └── db.sqlite           # DB
└── rag/
    └── corpus_builder.py   # Script qui construit le corpus de RAG à partir de db.sqlite
```

# Contributing
For each new feature or bug fix:
1. Create a new branch from `main` (e.g., `feature/new-node`).
2. Implement the feature or fix the bug in the appropriate files (e.g., `core/nodes.py` for new nodes).
3. Test your changes locally.
4. Commit your changes with a clear message (e.g., "Add new node for database querying").
5. Push your branch to the remote repository.
6. Open a pull request against the `main` branch, describing the changes and their purpose.

