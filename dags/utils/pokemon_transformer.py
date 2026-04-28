"""
Transformation des données brutes PokeAPI en structure exploitable.

Champs retenus et justification :
- id              : identifiant unique du Pokémon (clé primaire future table)
- name            : nom canonique
- height_dm       : taille en décimètres (donnée physique métier)
- weight_hg       : poids en hectogrammes (donnée physique métier)
- base_experience : expérience de base (indicateur de puissance)
- types           : liste des types (ex. "fire", "flying") — catégorisation métier
- hp / attack / defense / speed : stats de combat essentielles
- sprite_url      : URL de l'image officielle (utile pour un dashboard)
"""

import json
from datetime import datetime, timezone


def _extract_stats(stats: list[dict]) -> dict:
    """Convertit la liste de stats en dict plat {nom_stat: valeur}."""
    kept = {"hp", "attack", "defense", "speed"}
    return {
        s["stat"]["name"]: s["base_stat"]
        for s in stats
        if s["stat"]["name"] in kept
    }


def transform_pokemon(raw: dict) -> dict:
    """Transforme un Pokémon brut en enregistrement propre pour le pipeline."""
    stats = _extract_stats(raw.get("stats", []))
    return {
        "id": raw["id"],
        "name": raw["name"],
        "height_dm": raw["height"],
        "weight_hg": raw["weight"],
        "base_experience": raw.get("base_experience"),
        "types": [t["type"]["name"] for t in raw.get("types", [])],
        "hp": stats.get("hp"),
        "attack": stats.get("attack"),
        "defense": stats.get("defense"),
        "speed": stats.get("speed"),
        "sprite_url": raw.get("sprites", {}).get("front_default"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def transform_all_pokemons(raw_list: list[dict]) -> list[dict]:
    """Transforme une liste de Pokémon bruts."""
    transformed = []
    for raw in raw_list:
        print(f"[TRANSFORM] Processing: {raw['name']}")
        transformed.append(transform_pokemon(raw))
    return transformed


def print_summary(records: list[dict]) -> None:
    """Affiche un aperçu lisible des données préparées."""
    print("\n" + "=" * 60)
    print(f"  Aperçu des données préparées ({len(records)} Pokémon)")
    print("=" * 60)
    for r in records:
        print(
            f"  #{r['id']:04d} {r['name'].capitalize():<12} "
            f"types={r['types']} "
            f"hp={r['hp']} atk={r['attack']} def={r['defense']} spd={r['speed']}"
        )
    print("=" * 60 + "\n")
    print(json.dumps(records[0], indent=2, ensure_ascii=False))
