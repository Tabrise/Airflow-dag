"""
Récupération des données brutes depuis l'API PokeAPI.
Séparé du pipeline de transformation pour respecter le principe de séparation des responsabilités.
"""

import requests


POKEAPI_BASE_URL = "https://pokeapi.co/api/v2/pokemon"

POKEMONS = ["pikachu", "bulbasaur", "charmander", "squirtle", "mewtwo"]


def fetch_pokemon_raw(name: str) -> dict:
    """Appelle l'API PokeAPI et retourne la réponse JSON brute."""
    url = f"{POKEAPI_BASE_URL}/{name.lower()}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def fetch_all_pokemons_raw() -> list[dict]:
    """Récupère les données brutes pour tous les Pokémon de la liste."""
    results = []
    for name in POKEMONS:
        print(f"[EXTRACT] Fetching: {name}")
        raw = fetch_pokemon_raw(name)
        results.append(raw)
    return results
