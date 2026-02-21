import os
import pandas as pd
import numpy as np
import sqlite3
import re



def ingest_collisions():

    # Nettoyage des données (CSV -> SQL)
    # 1) On lit le CSV en texte pour garder les codes tels quels.
    # 2) On transforme les "vides" ("" , espaces, "Non précisé", NA, etc.) en NULL.
    # 3) On garde seulement les lignes avec NO_SEQ_COLL (sinon pas d’identifiant).
    # 4) On convertit DT_ACCDN en date (YYYY-MM-DD).
    # 5) On remplit AN et le jour de semaine à partir de DT_ACCDN quand c’est possible.
    # 6) On convertit les compteurs (NB_*) en entiers et on met 0 si c’est vide.
    # 7) Si GRAVITE est vide, on la déduit à partir des compteurs (morts/graves/légers).
    # 8) On charge le résultat dans une table SQL (SQLite ici).

    CSV_PATH = CSV_PATH_COLLISION
    DB_PATH  = DB_PATH_COLLISION
    TABLE    = TABLE_COLLISION

    df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False)

    NULL_TOKENS = {"", " ", "NA", "N/A", "nan", "NaN", "None", "NULL", "Non précisé"}
    def to_null(x):
        if x is None: return None
        if isinstance(x, float) and np.isnan(x): return None
        s = str(x).strip()
        return None if s in NULL_TOKENS else s

    for c in df.columns:
        df[c] = df[c].map(to_null)

    df = df[df["NO_SEQ_COLL"].notna()].copy()

    dt = pd.to_datetime(df["DT_ACCDN"], errors="coerce", format="%Y/%m/%d")
    dt = dt.fillna(pd.to_datetime(df["DT_ACCDN"], errors="coerce"))
    df["DT_ACCDN"] = dt.dt.strftime("%Y-%m-%d")

    df["AN"] = pd.to_numeric(df["AN"], errors="coerce")
    df.loc[df["AN"].isna() & dt.notna(), "AN"] = dt.dt.year
    df["AN"] = df["AN"].astype("Int64")

    for c in ["NB_MORTS","NB_BLESSES_GRAVES","NB_BLESSES_LEGERS"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")

    if "GRAVITE" in df.columns:
        miss = df["GRAVITE"].isna()
        df.loc[miss & (df["NB_MORTS"] > 0), "GRAVITE"] = "Mortel"
        df.loc[miss & (df["NB_MORTS"] == 0) & (df["NB_BLESSES_GRAVES"] > 0), "GRAVITE"] = "Grave"
        df.loc[miss & (df["NB_MORTS"] == 0) & (df["NB_BLESSES_GRAVES"] == 0) & (df["NB_BLESSES_LEGERS"] > 0), "GRAVITE"] = "Léger"
        df.loc[miss & (df["NB_MORTS"] == 0) & (df["NB_BLESSES_GRAVES"] == 0) & (df["NB_BLESSES_LEGERS"] == 0), "GRAVITE"] = "Dommages matériels seulement"

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
    cur.execute(f"""
    CREATE TABLE {TABLE} (
        NO_SEQ_COLL TEXT NOT NULL,
        DT_ACCDN TEXT,
        AN INTEGER,
        JR_SEMN_ACCDN TEXT,
        HEURE_ACCDN TEXT,
        GRAVITE TEXT,
        NB_MORTS INTEGER,
        NB_BLESSES_GRAVES INTEGER,
        NB_BLESSES_LEGERS INTEGER
    );
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_no_seq ON {TABLE}(NO_SEQ_COLL);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_dt ON {TABLE}(DT_ACCDN);")

    wanted = [c for c in [
        "NO_SEQ_COLL","DT_ACCDN","AN","JR_SEMN_ACCDN","HEURE_ACCDN","GRAVITE",
        "NB_MORTS","NB_BLESSES_GRAVES","NB_BLESSES_LEGERS"
    ] if c in df.columns]

    df[wanted].to_sql(TABLE, con, if_exists="append", index=False)

    con.commit()
    con.close()

    print("OK:", len(df), "lignes ->", DB_PATH, "table:", TABLE)


def ingest_311():
    # - Standardise les valeurs manquantes → NULL
    # - Convertit DDS_DATE_CREATION et DATE_DERNIER_STATUT → datetime ISO (SQL-friendly)
    # - Valide NATURE (Information / Commentaire / Requête / Plainte) ; sinon → NULL
    # - Valide DERNIER_STATUT selon la liste autorisée ; sinon → NULL
    # - Applique la règle ID_UNIQUE :
    #   - autorisé NULL si NATURE == "Information"
    #   - sinon, si ID_UNIQUE est NULL → ligne supprimée
    # - Convertit toutes les colonnes PROVENANCE_* en entiers ; vides → 0
    # - Nettoie/valide LIN_CODE_POSTAL (format canadien) ; invalide → NULL
    # - Convertit LOC_X/LOC_Y/LOC_LAT/LOC_LONG en numériques ; invalide → NULL
    # - Force LOC_ERREUR_GDT ∈ {0,1} ; sinon → NULL
    # - Déduplique sur ID_UNIQUE en gardant l’enregistrement le plus récent
    #   (DATE_DERNIER_STATUT sinon DDS_DATE_CREATION)
    # - Crée/alimente une table SQLite + index (ID_UNIQUE, ARRONDISSEMENT, DDS_DATE_CREATION)


    CSV_PATH = CSV_PATH_311
    DB_PATH  = DB_PATH_311
    TABLE    = TABLE_311

    NATURE_OK = {"Information", "Commentaire", "Requête", "Plainte"}
    STATUT_OK = {
        "Acceptée", "Annulée", "Prise en charge", "Réactivée", "Refusée",
        "Supprimée", "Terminée", "Transmise pour traitement", "Urgente"
    }

    PROV_COLS = [
        "PROVENANCE_TELEPHONE","PROVENANCE_COURRIEL","PROVENANCE_PERSONNE","PROVENANCE_COURRIER",
        "PROVENANCE_TELECOPIEUR","PROVENANCE_INSTANCE","PROVENANCE_MOBILE","PROVENANCE_MEDIASOCIAUX",
        "PROVENANCE_SITEINTERNET"
    ]

    DATE_COLS = ["DDS_DATE_CREATION","DATE_DERNIER_STATUT"]
    NULL_TOKENS = {"", " ", "NA", "N/A", "nan", "NaN", "None", "NULL", "Non précisé", "non précisé"}

    POSTAL_RE = re.compile(r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$")

    def to_null(x):
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        s = str(x).strip()
        return None if s in NULL_TOKENS else s

    def clean_postal(s):
        if s is None:
            return None
        s = re.sub(r"\s+", "", s.upper())
        if not POSTAL_RE.match(s):
            return None
        return s[:3] + " " + s[3:]

    def parse_dt(series):
        return pd.to_datetime(series, errors="coerce", utc=False)

    df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False)

    for c in df.columns:
        df[c] = df[c].map(to_null)

    for c in DATE_COLS:
        if c in df.columns:
            dt = parse_dt(df[c])
            df[c] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    if "NATURE" in df.columns:
        df["NATURE"] = df["NATURE"].map(lambda x: x.strip() if x else None)
        df.loc[~df["NATURE"].isin(NATURE_OK), "NATURE"] = None

    if "DERNIER_STATUT" in df.columns:
        df["DERNIER_STATUT"] = df["DERNIER_STATUT"].map(lambda x: x.strip() if x else None)
        df.loc[~df["DERNIER_STATUT"].isin(STATUT_OK), "DERNIER_STATUT"] = None

    if "ID_UNIQUE" in df.columns and "NATURE" in df.columns:
        df["ID_UNIQUE"] = df["ID_UNIQUE"].map(lambda x: x.strip() if x else None)
        bad_id = df["NATURE"].notna() & (df["NATURE"] != "Information") & df["ID_UNIQUE"].isna()
        df = df.loc[~bad_id].copy()

    for c in PROV_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")

    if "LIN_CODE_POSTAL" in df.columns:
        df["LIN_CODE_POSTAL"] = df["LIN_CODE_POSTAL"].map(clean_postal)

    for c in ["LOC_X","LOC_Y","LOC_LAT","LOC_LONG"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "LOC_ERREUR_GDT" in df.columns:
        df["LOC_ERREUR_GDT"] = pd.to_numeric(df["LOC_ERREUR_GDT"], errors="coerce")
        df.loc[~df["LOC_ERREUR_GDT"].isin([0, 1]), "LOC_ERREUR_GDT"] = np.nan

    if "ID_UNIQUE" in df.columns:
        with_id = df[df["ID_UNIQUE"].notna()].copy()
        without_id = df[df["ID_UNIQUE"].isna()].copy()

        sort_dt = pd.to_datetime(with_id["DATE_DERNIER_STATUT"], errors="coerce")
        sort_dt2 = pd.to_datetime(with_id["DDS_DATE_CREATION"], errors="coerce")
        with_id["_sort_dt"] = sort_dt.fillna(sort_dt2)

        with_id = with_id.sort_values(["ID_UNIQUE", "_sort_dt"]).drop_duplicates("ID_UNIQUE", keep="last")
        with_id = with_id.drop(columns=["_sort_dt"])

        df = pd.concat([with_id, without_id], ignore_index=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {TABLE};")
    cur.execute(f"""
    CREATE TABLE {TABLE} (
        ID_UNIQUE TEXT,
        NATURE TEXT,
        ACTI_NOM TEXT,
        TYPE_LIEU_INTERV TEXT,
        ARRONDISSEMENT TEXT,
        ARRONDISSEMENT_GEO TEXT,
        UNITE_RESP_PARENT TEXT,
        DDS_DATE_CREATION TEXT,
        PROVENANCE_ORIGINALE TEXT,
        PROVENANCE_TELEPHONE INTEGER,
        PROVENANCE_COURRIEL INTEGER,
        PROVENANCE_PERSONNE INTEGER,
        PROVENANCE_COURRIER INTEGER,
        PROVENANCE_TELECOPIEUR INTEGER,
        PROVENANCE_INSTANCE INTEGER,
        PROVENANCE_MOBILE INTEGER,
        PROVENANCE_MEDIASOCIAUX INTEGER,
        PROVENANCE_SITEINTERNET INTEGER,
        RUE TEXT,
        RUE_INTERSECTION1 TEXT,
        RUE_INTERSECTION2 TEXT,
        LIN_CODE_POSTAL TEXT,
        LOC_X REAL,
        LOC_Y REAL,
        LOC_LAT REAL,
        LOC_LONG REAL,
        LOC_ERREUR_GDT INTEGER,
        DERNIER_STATUT TEXT,
        DATE_DERNIER_STATUT TEXT
    );
    """)

    wanted_cols = [col for col in [
        "ID_UNIQUE","NATURE","ACTI_NOM","TYPE_LIEU_INTERV","ARRONDISSEMENT","ARRONDISSEMENT_GEO",
        "UNITE_RESP_PARENT","DDS_DATE_CREATION","PROVENANCE_ORIGINALE", *PROV_COLS,
        "RUE","RUE_INTERSECTION1","RUE_INTERSECTION2","LIN_CODE_POSTAL",
        "LOC_X","LOC_Y","LOC_LAT","LOC_LONG","LOC_ERREUR_GDT",
        "DERNIER_STATUT","DATE_DERNIER_STATUT"
    ] if col in df.columns]

    df[wanted_cols].to_sql(TABLE, con, if_exists="append", index=False)

    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_id_unique ON {TABLE}(ID_UNIQUE);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_arr ON {TABLE}(ARRONDISSEMENT);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_date_creation ON {TABLE}(DDS_DATE_CREATION);")

    con.commit()
    con.close()

    print(f"OK: {len(df)} lignes chargées dans {DB_PATH}, table '{TABLE}'.")
   

def ingest_gtfs():
    DATA_DIR = DATA_PATH_GTFS
    DB_PATH  = DB_PATH_GTFS

    NULL_TOKENS = {"", " ", "NA", "N/A", "nan", "NaN", "None", "NULL", "null"}

    RULES = {
        "routes":        {"int": ["route_type"]},
        "stops":         {"int": ["location_type","wheelchair_boarding"], "float": ["stop_lat","stop_lon"]},
        "trips":         {"int": ["direction_id","wheelchair_accessible"]},
        "stop_times":    {"int": ["stop_sequence","pickup_type","drop_off_type","timepoint","continuous_pickup","continuous_drop_off"]},
        "calendar":      {"int": ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"], "date": ["start_date","end_date"]},
        "calendar_dates":{"int": ["exception_type"], "date": ["date"]},
        "shapes":        {"int": ["shape_pt_sequence"], "float": ["shape_pt_lat","shape_pt_lon"]},
        "feed_info":     {"date": ["feed_start_date","feed_end_date"]},
    }
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # ordre "logique" (si absent, ignoré)
    preferred = ["feed_info","agency","routes","stops","calendar","calendar_dates","shapes","trips","stop_times","translations"]

    # discover all txt
    all_txt = {os.path.splitext(fn)[0]: os.path.join(DATA_DIR, fn)
               for fn in os.listdir(DATA_DIR) if fn.lower().endswith(".txt")}

    order = [t for t in preferred if t in all_txt] + [t for t in all_txt if t not in preferred]

    for table in order:
        path = all_txt[table]
        df = pd.read_csv(path, dtype=str, low_memory=False)

        # NULL + trim
        for c in df.columns:
            df[c] = df[c].apply(lambda x: None if x is None or (isinstance(x, float) and np.isnan(x))
                                else (None if str(x).strip() in NULL_TOKENS else str(x).strip()))

        # dates YYYYMMDD -> YYYY-MM-DD
        if table in RULES and "date" in RULES[table]:
            for c in RULES[table]["date"]:
                if c in df.columns:
                    df[c] = df[c].apply(lambda s: (f"{s[:4]}-{s[4:6]}-{s[6:]}"
                                                   if isinstance(s, str) and len(s)==8 and s.isdigit()
                                                   else None))

        # ints
        if table in RULES and "int" in RULES[table]:
            for c in RULES[table]["int"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

        # floats
        if table in RULES and "float" in RULES[table]:
            for c in RULES[table]["float"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

        # CREATE TABLE simple (types via dtype)
        cur.execute(f'DROP TABLE IF EXISTS "{table}";')
        cols_sql = []
        for c in df.columns:
            if pd.api.types.is_integer_dtype(df[c].dtype):
                t = "INTEGER"
            elif pd.api.types.is_float_dtype(df[c].dtype):
                t = "REAL"
            else:
                t = "TEXT"
            cols_sql.append(f'"{c}" {t}')
        cur.execute(f'CREATE TABLE "{table}" ({", ".join(cols_sql)});')

        df.to_sql(table, con, if_exists="append", index=False)
        print(f"{table}: {len(df)} lignes")

    con.commit()
    con.close()
    print("OK ->", DB_PATH)


if __name__ == "__main__":

    CSV_PATH_COLLISION = "data/csv/collisions_routieres.csv"
    DB_PATH_COLLISION = "data/db/collisions.db"
    TABLE_COLLISION = "collisions"


    CSV_PATH_311 = "data/csv/requetes311.csv"
    DB_PATH_311 = "data/db/demandes.db"
    TABLE_311 = "demandes"

    DATA_PATH_GTFS = "data/csv/gtfs_stm/"
    DB_PATH_GTFS  = "data/db/gtfs_all.db"

    # 1. Création du dossier pour les bases de données (obligatoire pour SQLite)
    # Si le dossier data/db n'existe pas, SQLite lèvera une erreur.
    print("--- Préparation de l'environnement ---")
    os.makedirs("data/db", exist_ok=True)

    # 2. Lancement séquentiel des ingestions
    print("\n[1/3] Ingestion des Collisions Routières...")
    try:
        ingest_collisions()
    except Exception as e:
        print(f"Erreur lors de l'ingestion des collisions : {e}")

    print("\n[2/3] Ingestion des Requêtes 311...")
    try:
        ingest_311()
    except Exception as e:
        print(f"Erreur lors de l'ingestion du 311 : {e}")

    print("\n[3/3] Ingestion des fichiers GTFS...")
    try:
        # Assure-toi que DATA_PATH_GTFS est bien "data/csv/gtfs_stm/"
        ingest_gtfs()
    except Exception as e:
        print(f"Erreur lors de l'ingestion GTFS : {e}")

    print("\n--- Processus terminé. Vos bases .db sont prêtes dans data/db/ ---")

