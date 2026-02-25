# MobilityCopilot

[![MobilityCopilot](https://github.com/GabinVr/MobilityCopilot/actions/workflows/python-app.yml/badge.svg)](https://github.com/GabinVr/MobilityCopilot/actions/workflows/python-app.yml)
[![codecov](https://codecov.io/gh/GabinVr/MobilityCopilot/branch/main/graph/badge.svg?token=K78L5W5CKX)](https://codecov.io/gh/GabinVr/MobilityCopilot)

# Run the app
## With a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate 
pip install -r requirements.txt
uvicorn main:api --reload
```
In the view directory, you can also run the frontend with:
```bash
cd view
bun install
bun run dev
```
## With Docker
```bash
docker compose up --build
```

### Monitor CSV 311 (10 checks/day)
```bash
docker compose up -d requetes311-watcher
docker compose logs -f requetes311-watcher
```
The watcher stores:
- Local CSV at `data/csv/requetes311.csv`
- State file at `data/db/requetes311_monitor_state.json`
You can tune frequency with `REQUETES311_CHECKS_PER_DAY` (default: `10`).

## ENJOY !
The frontend should be available at
`http://127.0.0.1:3000`

# Run tests
```bash
pytest
```

# File structure
```
mobilitycopilot/
├── .env                    # Clés API, configurations, etc.
├── requirements.txt        
├── main.py                 # FastAPI serveur et routes API UNIQUEMENT ! pas de logique métier ici
├── routes/                  # Fichiers de routes FastAPI, organisés par domaine fonctionnel (ex: dashboard.py, chat.py...)
├── services/                # Fichiers de services comme report_service.py
├── scheduler.py             # Script qui lance les tâches planifiées 
├── core/
│   ├── state.py            # La logique de gestion d'état
│   ├── graph.py            # Le workflow de raisonnement (le graphe de nœuds)
│   ├── nodes/              # Les fichier avec la logique métier de chaque nœud (ex: exécution SQL...)
│   │   └── generate.py     # Exemple de nœud qui génère du texte avec un LLM
│   └──tools/               # Execution SQL/pandas, requête API, etc.
│      └── sql_executor.py     # Exemple de tool pour exécuter des requêtes SQL sur sqlite
├── data/
│   ├── ingest.py           # Script qui ingère les données (CSV, API, etc.) et les stocke dans db.sqlite 
│   └── db.sqlite           # DB
├── tests/                  # Tests unitaires et d'intégration
├── view/                   # Frontend avec Bun.js (BETH stack)
└── rag/
    └── corpus_builder.py   # Script qui construit le corpus de RAG
```

# Contributing
For each new feature or bug fix:
1. Create a new branch from `main` (e.g., `feature/new-node`).
2. Implement the feature or fix the bug in the appropriate files (e.g., `core/nodes.py` for new nodes).
3. Test your changes locally.
4. Commit your changes with a clear message (e.g., "Add new node for database querying").
5. Push your branch to the remote repository.
6. Open a pull request against the `main` branch, describing the changes and their purpose.

```sh
git checkout -b feature/new-node
# Make changes
git add .
git commit -m "Add new node for database querying"
git push origin feature/new-node
# You will be prompted to open a pull request on GitHub
# Click on the link to open the PR and provide a description of your changes
```
