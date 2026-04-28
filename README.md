# Pokemon Ingestion — Airflow DAG

Ingestion des données Pokémon depuis [PokeAPI](https://pokeapi.co) avec séparation extraction / transformation.

---

## Architecture du projet

```
Airflow/
├── dags/
│   ├── pokemon_ingestion_dag.py     # DAG principal
│   └── utils/
│       ├── pokemon_extractor.py     # Appels API (extraction brute)
│       └── pokemon_transformer.py  # Sélection et mise en forme des champs
└── README.md
```

---

## Pipeline — schéma du DAG

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│                     │     │                      │     │                     │
│   extract_pokemons  │────▶│  transform_pokemons  │────▶│   display_summary   │
│                     │     │                      │     │                     │
│  Appelle PokeAPI    │     │  Sélectionne les     │     │  Affiche un aperçu  │
│  pour 5 Pokémon     │     │  champs utiles et    │     │  tabulaire + JSON   │
│  Stocke JSON brut   │     │  reformate en dict   │     │  du premier record  │
│  dans XCom          │     │  propre via XCom     │     │                     │
└─────────────────────┘     └──────────────────────┘     └─────────────────────┘
         │                           │
         │    XCom : raw_pokemons    │    XCom : pokemon_records
         └───────────────────────────┘
```

**Planification :** `@daily` — pas de rattrapage historique (`catchup=False`)  
**Pokémon ciblés :** Pikachu, Bulbasaur, Charmander, Squirtle, Mewtwo

---

## Séparation des responsabilités

| Fichier | Rôle | Ce qu'il NE fait PAS |
|---|---|---|
| `pokemon_extractor.py` | Appel HTTP brut, retourne le JSON tel quel | Aucune transformation |
| `pokemon_transformer.py` | Sélection des champs, calculs, formatage | Aucun appel réseau |
| `pokemon_ingestion_dag.py` | Orchestre les tâches via XCom | Contient aucune logique métier |

---

## Flux de données

```
PokeAPI (JSON brut ~150 champs)
        │
        ▼
┌───────────────────────────────────────┐
│           EXTRACTION                  │
│  GET /api/v2/pokemon/{name}           │
│  → réponse JSON brute stockée XCom   │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│           TRANSFORMATION              │
│  Sélection de 11 champs utiles        │
│  Aplatissement des stats              │
│  Ajout du timestamp d'ingestion       │
└───────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────┐
│  Enregistrement prêt pour chargement  │
│  (base de données, CSV, entrepôt...)  │
└───────────────────────────────────────┘
```

---

## Champs retenus

### Schéma de l'enregistrement de sortie

```
pokemon_record
├── id              (int)       Identifiant unique PokeAPI
├── name            (str)       Nom canonique en minuscules
├── height_dm       (int)       Taille en décimètres
├── weight_hg       (int)       Poids en hectogrammes
├── base_experience (int|null)  Expérience de base (indicateur de puissance)
├── types           (list[str]) Liste des types   ex: ["fire", "flying"]
├── hp              (int)       Points de vie
├── attack          (int)       Statistique d'attaque
├── defense         (int)       Statistique de défense
├── speed           (int)       Statistique de vitesse
├── sprite_url      (str|null)  URL de l'image officielle front
└── ingested_at     (str)       Timestamp ISO 8601 UTC d'ingestion
```

### Justification des champs retenus

| Champ | Source API | Pourquoi retenu |
|---|---|---|
| `id` | `id` | Clé primaire naturelle pour la future table |
| `name` | `name` | Identifiant métier lisible |
| `height_dm` | `height` | Donnée physique exploitable (analyses morphologiques) |
| `weight_hg` | `weight` | Donnée physique exploitable (analyses morphologiques) |
| `base_experience` | `base_experience` | Indicateur synthétique de puissance globale |
| `types` | `types[].type.name` | Axe de catégorisation clé pour tout dashboard |
| `hp` | `stats[name=hp].base_stat` | Stat de survie, fondamentale en analyse de combat |
| `attack` | `stats[name=attack].base_stat` | Puissance offensive |
| `defense` | `stats[name=defense].base_stat` | Résistance défensive |
| `speed` | `stats[name=speed].base_stat` | Détermine l'ordre d'action en combat |
| `sprite_url` | `sprites.front_default` | Image officielle pour affichage dashboard |
| `ingested_at` | *(généré)* | Traçabilité pipeline, essentiel pour audits |

### Champs écartés et pourquoi

| Champ API | Raison de l'exclusion |
|---|---|
| `moves` | Liste de ~80 entrées, détail trop fin pour un pipeline d'analyse |
| `abilities` | Donnée contextuelle sans valeur agrégée immédiate |
| `forms` / `game_indices` | Données de version de jeu non pertinentes pour l'analyse |
| `held_items` | Presque toujours vide pour les Pokémon de base |
| `stats` autres (`special-attack`, `special-defense`) | Retrait pour simplifier — ajout facile si besoin |

---

## Exemple de sortie

```json
{
  "id": 25,
  "name": "pikachu",
  "height_dm": 4,
  "weight_hg": 60,
  "base_experience": 112,
  "types": ["electric"],
  "hp": 35,
  "attack": 55,
  "defense": 40,
  "speed": 90,
  "sprite_url": "https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/25.png",
  "ingested_at": "2026-04-28T09:00:00+00:00"
}
```

---

## Lancer le DAG

```bash
# Démarrer Airflow (Docker Compose)
docker compose up -d

# Déclencher manuellement
airflow dags trigger pokemon_ingestion

# Consulter les logs de la dernière exécution
airflow dags list-runs -d pokemon_ingestion
```
