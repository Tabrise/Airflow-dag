"""
DAG : pokemon_ingestion
Objectif : ingérer les données Pokémon depuis PokeAPI, les transformer
           en structure exploitable et afficher un aperçu des résultats.

Structure :
    extract_pokemons >> transform_pokemons >> display_summary
"""

import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.pokemon_extractor import fetch_all_pokemons_raw
from utils.pokemon_transformer import transform_all_pokemons, print_summary


default_args = {
    "owner": "ipssi",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract(**context) -> None:
    """Récupère les données brutes et les pousse dans XCom."""
    raw_data = fetch_all_pokemons_raw()
    context["ti"].xcom_push(key="raw_pokemons", value=raw_data)
    print(f"[EXTRACT] {len(raw_data)} Pokémon récupérés depuis PokeAPI")


def transform(**context) -> None:
    """Lit les données brutes depuis XCom, transforme et repousse le résultat."""
    raw_data = context["ti"].xcom_pull(key="raw_pokemons", task_ids="extract_pokemons")
    records = transform_all_pokemons(raw_data)
    context["ti"].xcom_push(key="pokemon_records", value=records)
    print(f"[TRANSFORM] {len(records)} enregistrements prêts pour chargement")


def display_summary(**context) -> None:
    """Affiche un aperçu des données préparées (livrable pédagogique)."""
    records = context["ti"].xcom_pull(key="pokemon_records", task_ids="transform_pokemons")
    print_summary(records)


with DAG(
    dag_id="pokemon_ingestion",
    description="Ingestion PokeAPI — extraction, transformation, aperçu",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["pokemon", "api", "ingestion", "ipssi"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_pokemons",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_pokemons",
        python_callable=transform,
    )

    summary_task = PythonOperator(
        task_id="display_summary",
        python_callable=display_summary,
    )

    extract_task >> transform_task >> summary_task
