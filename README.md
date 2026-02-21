# MobilityCopilot

[![MobilityCopilot](https://github.com/GabinVr/MobilityCopilot/actions/workflows/python-app.yml/badge.svg)](https://github.com/GabinVr/MobilityCopilot/actions/workflows/python-app.yml)
[![codecov](https://codecov.io/gh/GabinVr/MobilityCopilot/branch/main/graph/badge.svg?token=YOUR_TOKEN)](https://codecov.io/gh/GabinVr/MobilityCopilot)

# Run the app
## With a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate 
pip install -r requirements.txt
uvicorn main:api --reload
```
## With Docker
```bash
docker build -t mobilitycopilot:latest .
docker run -p 8000:8000 mobilitycopilot:latest
```
## ENJOY !
Watch the current API endpoint once the server is running:
`http://127.0.0.1:8000/docs`

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
├── core/
│   ├── state.py            # La logique de gestion d'état
│   ├── graph.py            # Le workflow de raisonnement (le graphe de nœuds)
│   ├── nodes/              # Les fichier avec la logique métier de chaque nœud (ex: exécution SQL...)
│   │   ├── generate.py     # Exemple de nœud qui génère du texte avec un LLM
│   └── tools.py            # Execution SQL/pandas
├── data/
│   ├── ingest.py           # Script qui ingère les données (CSV, API, etc.) et les stocke dans db.sqlite 
│   └── db.sqlite           # DB
├── tests/                  # Tests unitaires et d'intégration
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

```sh
git checkout -b feature/new-node
# Make changes
git add .
git commit -m "Add new node for database querying"
git push origin feature/new-node
# You will be prompted to open a pull request on GitHub
# Click on the link to open the PR and provide a description of your changes
```