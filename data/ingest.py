"""
Ingesteur de données - Implémentation propre et SOLID
Respecte les principes:
- Single Responsibility: chaque classe a une seule raison de changer
- Open/Closed: ouvert à l'extension, fermé à la modification  
- Liskov Substitution: DataLoader peut être étendu
- Interface Segregation: interfaces minimales et spécifiques
- Dependency Inversion: dépend des abstractions
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Tuple
from pathlib import Path
from dataclasses import dataclass


# ============================================================================
# TYPES & CONSTANTES
# ============================================================================

@dataclass
class LoadResult:
    """Résultat du chargement d'un fichier"""
    name: str
    path: str
    rows_before: int
    rows_after: int
    columns: int


# ============================================================================
# INTERFACES (SOLID principles)
# ============================================================================

class DataLoader(ABC):
    """Interface pour charger des données depuis une source"""
    
    @abstractmethod
    def load(self, path: str) -> Tuple[str, pd.DataFrame]:
        """Charge les données et retourne (nom_table, dataframe)"""
        pass


class DataCleaner(ABC):
    """Interface pour nettoyer des données"""
    
    @abstractmethod
    def clean(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Nettoie le dataframe"""
        pass


class DatabaseWriter(ABC):
    """Interface pour écrire dans une base de données"""
    
    @abstractmethod
    def write(self, table_name: str, df: pd.DataFrame) -> None:
        """Écrit le dataframe dans la base de données"""
        pass


# ============================================================================
# IMPLEMENTATIONS - LOADERS
# ============================================================================

class CSVLoader(DataLoader):
    """Charge les fichiers CSV"""
    
    def load(self, path: str) -> Tuple[str, pd.DataFrame]:
        table_name = Path(path).stem
        df = pd.read_csv(path, dtype=str, low_memory=False)
        return table_name, df


class TXTLoader(DataLoader):
    """Charge les fichiers TXT (format GTFS)"""
    
    def load(self, path: str) -> Tuple[str, pd.DataFrame]:
        table_name = Path(path).stem
        df = pd.read_csv(path, dtype=str, low_memory=False)
        return table_name, df


class DataLoaderFactory:
    """Factory pour créer les loaders appropriés"""
    
    _loaders: Dict[str, DataLoader] = {
        '.csv': CSVLoader(),
        '.txt': TXTLoader(),
    }
    
    @classmethod
    def get_loader(cls, file_path: str) -> Optional[DataLoader]:
        ext = Path(file_path).suffix.lower()
        return cls._loaders.get(ext)


# ============================================================================
# IMPLEMENTATIONS - CLEANER (Soft cleaning)
# ============================================================================

class SoftDataCleaner(DataCleaner):
    """Nettoyage minimal et doux des données"""
    
    # Tokens considérés comme "vides"
    EMPTY_TOKENS = {"", " ", "NA", "N/A", "nan", "NaN", "None", "NULL", 
                    "null", "Non précisé", "non précisé"}
    
    # Règles de conversion par table
    CONVERSION_RULES: Dict[str, Dict[str, List[str]]] = {
        "collisions_routieres": {
            "int": ["NO_CIVIQ_ACCDN", "NB_METRE_DIST_ACCD", "NB_VEH_IMPLIQUES_ACCDN",
                   "NB_MORTS", "NB_BLESSES_GRAVES", "NB_BLESSES_LEGERS", "AN", "NB_VICTIMES_TOTAL",
                   "NB_DECES_PIETON", "NB_BLESSES_PIETON", "NB_VICTIMES_PIETON",
                   "NB_DECES_MOTO", "NB_BLESSES_MOTO", "NB_VICTIMES_MOTO",
                   "NB_DECES_VELO", "NB_BLESSES_VELO", "NB_VICTIMES_VELO",
                   "VITESSE_AUTOR", "nb_automobile_camion_leger", "nb_camionLourd_tractRoutier",
                   "nb_outil_equipement", "nb_tous_autobus_minibus", "nb_bicyclette",
                   "nb_cyclomoteur", "nb_motocyclette", "nb_taxi", "nb_urgence",
                   "nb_motoneige", "nb_VHR", "nb_autres_types", "nb_veh_non_precise"],
            "float": ["LOC_X", "LOC_Y", "LOC_LONG", "LOC_LAT"],
            "date": ["DT_ACCDN"],
        },
        "requetes311": {
            "int": ["PROVENANCE_TELEPHONE", "PROVENANCE_COURRIEL", "PROVENANCE_PERSONNE",
                   "PROVENANCE_COURRIER", "PROVENANCE_TELECOPIEUR", "PROVENANCE_INSTANCE",
                   "PROVENANCE_MOBILE", "PROVENANCE_MEDIASOCIAUX", "PROVENANCE_SITEINTERNET",
                   "LOC_ERREUR_GDT"],
            "float": ["LOC_LONG", "LOC_LAT", "LOC_X", "LOC_Y"],
            "datetime": ["DDS_DATE_CREATION", "DATE_DERNIER_STATUT"],
        },
        # Tables GTFS
        "routes": {
            "int": ["route_type"],
        },
        "stops": {
            "int": ["location_type", "wheelchair_boarding"],
            "float": ["stop_lat", "stop_lon"],
        },
        "trips": {
            "int": ["direction_id", "wheelchair_accessible"],
        },
        "stop_times": {
            "int": ["stop_sequence", "pickup_type", "drop_off_type", "timepoint",
                   "continuous_pickup", "continuous_drop_off"],
        },
        "calendar": {
            "int": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
            "date": ["start_date", "end_date"],
        },
        "calendar_dates": {
            "int": ["exception_type"],
            "date": ["date"],
        },
        "shapes": {
            "int": ["shape_pt_sequence"],
            "float": ["shape_pt_lat", "shape_pt_lon"],
        },
        "feed_info": {
            "date": ["feed_start_date", "feed_end_date"],
        },
    }
    
    def clean(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Nettoyage en deux phases: nullification et conversion de type"""
        
        # Phase 1: Nettoyage des valeurs vides (minimal)
        df = self._nullify_empty_values(df)
        
        # Phase 2: Conversion de type selon la table
        df = self._convert_types(df, table_name)
        
        # Phase 3: Règles métier spécifiques par table
        df = self._apply_business_rules(df, table_name)
        
        return df
    
    def _nullify_empty_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convertit les valeurs "vides" en None, avec strip des espaces"""
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(lambda x: self._clean_value(x))
        return df
    
    def _clean_value(self, x: Any) -> Any:
        """Nettoie une valeur unique"""
        if pd.isna(x):
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        s = str(x).strip()
        return None if s in self.EMPTY_TOKENS else s
    
    def _convert_types(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Convertit les types selon les règles de la table"""
        
        rules = self.CONVERSION_RULES.get(table_name, {})
        
        # Conversion en entiers
        for col in rules.get("int", []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Garder NaN au lieu de les convertir en 0 (c'est plus doux)
        
        # Conversion en floats
        for col in rules.get("float", []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Conversion en dates (format YYYY-MM-DD)
        for col in rules.get("date", []):
            if col in df.columns:
                df[col] = self._parse_date(df[col])
        
        # Conversion en datetime (format ISO)
        for col in rules.get("datetime", []):
            if col in df.columns:
                df[col] = self._parse_datetime(df[col])
        
        return df
    
    def _parse_date(self, series: pd.Series) -> pd.Series:
        """Parse une date dans multiple formats et retourne YYYY-MM-DD"""
        formats = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y%m%d"]
        result = pd.Series([None] * len(series), dtype=object)
        
        for fmt in formats:
            mask = result.isna() & series.notna()
            if mask.any():
                try:
                    parsed = pd.to_datetime(series[mask], format=fmt, errors="coerce")
                    result[mask] = parsed.dt.strftime("%Y-%m-%d")
                except:
                    pass
        
        return result
    
    def _parse_datetime(self, series: pd.Series) -> pd.Series:
        """Parse un datetime et retourne format ISO"""
        result = pd.to_datetime(series, errors="coerce")
        return result.dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def _apply_business_rules(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Applique des règles métier spécifiques à chaque table"""
        
        if table_name == "demandes":
            df = self._apply_311_rules(df)
        
        return df
    
    def _apply_311_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Règles spécifiques pour la table 311"""
        
        # Valider NATURE si la colonne existe
        if "NATURE" in df.columns:
            nature_ok = {"Information", "Commentaire", "Requête", "Plainte"}
            mask = df["NATURE"].notna() & ~df["NATURE"].isin(nature_ok)
            df.loc[mask, "NATURE"] = None
        
        # Valider DERNIER_STATUT si la colonne existe
        if "DERNIER_STATUT" in df.columns:
            statut_ok = {"Acceptée", "Annulée", "Prise en charge", "Réactivée", "Refusée",
                        "Supprimée", "Terminée", "Transmise pour traitement", "Urgente"}
            mask = df["DERNIER_STATUT"].notna() & ~df["DERNIER_STATUT"].isin(statut_ok)
            df.loc[mask, "DERNIER_STATUT"] = None
        
        # NE PAS supprimer les lignes avec ID_UNIQUE NULL (règle trop restrictive!)
        # Garder toutes les données, c'est plus doux
        
        return df


# ============================================================================
# IMPLEMENTATIONS - DATABASE WRITER
# ============================================================================

class SQLiteDatabaseWriter(DatabaseWriter):
    """Écrit les données dans une base SQLite"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = None
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self
    
    def __exit__(self, *args):
        if self.conn:
            self.conn.close()
    
    def write(self, table_name: str, df: pd.DataFrame) -> None:
        """Écrit le dataframe dans la base"""
        if self.conn is None:
            raise RuntimeError("DatabaseWriter not opened with context manager")
        
        # Créer la table avec les types appropriés
        self._create_table(table_name, df)
        
        # Insérer les données
        df.to_sql(table_name, self.conn, if_exists="append", index=False)
        self.conn.commit()
    
    def _create_table(self, table_name: str, df: pd.DataFrame) -> None:
        """Crée la table avec les types de colonnes appropriés"""
        cursor = self.conn.cursor()
        
        # Supprimer la table si elle existe
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        
        # Construire les colonnes SQL
        columns = []
        for col in df.columns:
            dtype = "INTEGER" if pd.api.types.is_integer_dtype(df[col].dtype) else \
                    "REAL" if pd.api.types.is_float_dtype(df[col].dtype) else \
                    "TEXT"
            columns.append(f'"{col}" {dtype}')
        
        # Créer la table
        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
        cursor.execute(create_sql)
        
        # Créer les index sur les colonnes clés
        self._create_indices(table_name, df.columns)
        
        self.conn.commit()
    
    def _create_indices(self, table_name: str, columns: List[str]) -> None:
        """Crée les index sur les colonnes utiles"""
        cursor = self.conn.cursor()
        
        # Index sur les colonnes d'ID ou de date
        index_candidates = [
            "NO_SEQ_COLL", "ID_UNIQUE", "DT_ACCDN", "DDS_DATE_CREATION",
            "DATE_DERNIER_STATUT", "ARRONDISSEMENT", "AN"
        ]
        
        for col in index_candidates:
            if col in columns:
                index_name = f"idx_{table_name}_{col.lower()}"
                try:
                    cursor.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}"("{col}")')
                except sqlite3.OperationalError:
                    pass  # Index peut déjà exister
        
        self.conn.commit()


# ==========================================================================
# APPEND WRITER (Incremental updates)
# ==========================================================================

class SQLiteAppendWriter:
    """Écrit les données en append sans supprimer la table existante."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, *args):
        if self.conn:
            self.conn.close()

    def write(self, table_name: str, df: pd.DataFrame) -> None:
        if self.conn is None:
            raise RuntimeError("SQLiteAppendWriter not opened with context manager")

        self._ensure_table(table_name, df)
        df.to_sql(table_name, self.conn, if_exists="append", index=False)
        self.conn.commit()

    def _ensure_table(self, table_name: str, df: pd.DataFrame) -> None:
        if not table_exists(self.conn, table_name):
            self._create_table(table_name, df)
            return

        existing_cols = get_table_columns(self.conn, table_name)
        missing = [col for col in df.columns if col not in existing_cols]
        if not missing:
            return

        cursor = self.conn.cursor()
        for col in missing:
            dtype = map_dtype(df[col].dtype)
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype}')
        self.conn.commit()

    def _create_table(self, table_name: str, df: pd.DataFrame) -> None:
        cursor = self.conn.cursor()
        columns_sql = []
        for col in df.columns:
            dtype = map_dtype(df[col].dtype)
            columns_sql.append(f'"{col}" {dtype}')
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_sql)})'
        cursor.execute(create_sql)
        self.conn.commit()


def map_dtype(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    return "TEXT"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]


def get_default_db_path() -> str:
    return os.getenv("MOBILITY_DB_PATH", "data/db/mobility.db")


class Requetes311Store:
    """Persist and de-duplicate 311 requests data in SQLite."""

    def __init__(self, db_path: Optional[str] = None, table_name: str = "requetes311"):
        self.db_path = db_path or get_default_db_path()
        self.table_name = table_name

    def append_new_rows(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        with SQLiteAppendWriter(self.db_path) as writer:
            existing_ids = self._fetch_existing_ids(writer.conn)
            if existing_ids is not None and "ID_UNIQUE" in df.columns:
                df = df[~df["ID_UNIQUE"].isin(existing_ids)]

            if df.empty:
                return 0

            writer.write(self.table_name, df)
            return len(df)

    def _fetch_existing_ids(self, conn: sqlite3.Connection) -> Optional[set]:
        if not table_exists(conn, self.table_name):
            return None

        columns = get_table_columns(conn, self.table_name)
        if "ID_UNIQUE" not in columns:
            return None

        cursor = conn.cursor()
        cursor.execute(f'SELECT ID_UNIQUE FROM "{self.table_name}"')
        return {row[0] for row in cursor.fetchall() if row[0] is not None}


# ============================================================================
# ORCHESTRATOR
# ============================================================================

class DataIngestionService:
    """Service principal d'ingestion de données"""
    
    def __init__(self, loader_factory: DataLoaderFactory, cleaner: DataCleaner, 
                 writer: DatabaseWriter):
        self.loader_factory = loader_factory
        self.cleaner = cleaner
        self.writer = writer
        self.results: List[LoadResult] = []
    
    def ingest_directory(self, source_dir: str) -> List[LoadResult]:
        """Ingère tous les fichiers CSV et TXT d'un répertoire"""
        
        for file_path in Path(source_dir).glob("**/*"):
            if not file_path.is_file():
                continue
            
            loader = self.loader_factory.get_loader(str(file_path))
            if not loader:
                continue
            
            try:
                print(f"  Chargement: {file_path.name}...", end=" ")
                
                # Charger
                table_name, df = loader.load(str(file_path))
                rows_before = len(df)
                cols = len(df.columns)
                
                # Nettoyer
                df = self.cleaner.clean(df, table_name)
                
                # Écrire
                self.writer.write(table_name, df)
                
                result = LoadResult(
                    name=table_name,
                    path=str(file_path),
                    rows_before=rows_before,
                    rows_after=len(df),
                    columns=cols
                )
                self.results.append(result)
                
                print(f"✓ {rows_before} lignes → {cols} colonnes")
                
            except Exception as e:
                print(f"✗ Erreur: {type(e).__name__}: {str(e)[:80]}")
    
    def print_summary(self) -> None:
        """Affiche un résumé de l'ingestion"""
        print("\n" + "="*70)
        print("RÉSUMÉ DE L'INGESTION")
        print("="*70)
        
        total_rows = sum(r.rows_after for r in self.results)
        total_tables = len(self.results)
        
        print(f"\nTableles créées: {total_tables}")
        print(f"Lignes totales: {total_rows}")
        print(f"\nDétails par table:")
        print(f"  {'Table':<20} {'Lignes':<10} {'Colonnes':<10}")
        print("-" * 40)
        
        for result in self.results:
            print(f"  {result.name:<20} {result.rows_after:<10} {result.columns:<10}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Point d'entrée principal"""
    
    # Configuration
    CSV_DIR = "data/csv"
    DB_PATH = "data/db/mobility.db"
    
    print("="*70)
    print("INGESTION DE DONNÉES - MobilityCopilot")
    print("="*70)
    
    # Créer le répertoire de la base de données
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Supprimer l'ancienne base si elle existe
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"\nAncienne base supprimée: {DB_PATH}")
    
    # Initialiser le service
    clean_dry = False  # Mode dry-run (test sans écrire)
    
    print(f"\nChargement depuis: {CSV_DIR}")
    print(f"Cible: {DB_PATH}\n")
    
    with SQLiteDatabaseWriter(DB_PATH) as writer:
        service = DataIngestionService(
            loader_factory=DataLoaderFactory(),
            cleaner=SoftDataCleaner(),
            writer=writer
        )
        
        service.ingest_directory(CSV_DIR)
        service.print_summary()
    
    print(f"\n✓ Base de données créée: {DB_PATH}")
    print("="*70)


if __name__ == "__main__":
    main()
