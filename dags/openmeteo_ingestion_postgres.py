import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from modules.fetch import archive_raw, fetch_meteo
from modules.load import load_data, log_ingestion
from modules.quality import check_quality
from modules.transform import transform_data

DEFAULT_ARGS = {
    "owner": "oukhemanou",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=5),
}


def get_villes() -> dict:
    default = json.dumps({
        "Paris": [48.8566, 2.3522],
        "Lyon": [45.7640, 4.8357],
        "Marseille": [43.2965, 5.3698],
    })
    raw = Variable.get("meteo_villes", default_var=default)
    return json.loads(raw)


def alert_on_failure(**context):
    logging.error("[alert] ÉCHEC détecté dans le pipeline météo.")
    logging.error(f"[alert] Date : {context['ds']}")


def log_execution(**context):
    villes = list(get_villes().keys())
    logging.info("=" * 60)
    logging.info("[log] openmeteo_ingestion_postgres — bilan d'exécution")
    logging.info(f"[log] Date : {context['ds']} | Villes : {villes}")
    logging.info("=" * 60)


with DAG(
    dag_id="openmeteo_ingestion_postgres",
    description="Ingestion Open-Meteo PostgreSQL — fetch_archive / transform / quality / load par ville",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["open-meteo", "tp5"],
) as dag:

    toutes_taches = []
    VILLES = get_villes()

    for ville, coords in VILLES.items():
        latitude, longitude = coords[0], coords[1]

        fetch = PythonOperator(
            task_id=f"fetch_meteo_{ville}",
            python_callable=fetch_meteo,
            op_kwargs={"ville": ville, "latitude": latitude, "longitude": longitude},
        )

        archive = PythonOperator(
            task_id=f"archive_raw_{ville}",
            python_callable=archive_raw,
            op_kwargs={"ville": ville},
        )

        transform = PythonOperator(
            task_id=f"transform_data_{ville}",
            python_callable=transform_data,
            op_kwargs={"ville": ville},
        )

        quality = BranchPythonOperator(
            task_id=f"check_quality_{ville}",
            python_callable=check_quality,
            op_kwargs={"ville": ville},
        )

        load = PythonOperator(
            task_id=f"load_data_{ville}",
            python_callable=load_data,
            op_kwargs={"ville": ville},
        )

        skip = EmptyOperator(
            task_id=f"skip_load_{ville}",
        )

        log_ing = PythonOperator(
            task_id=f"log_ingestion_{ville}",
            python_callable=log_ingestion,
            op_kwargs={"ville": ville},
            trigger_rule="none_failed_min_one_success",
        )

        fetch >> archive >> transform >> quality
        quality >> load >> log_ing
        quality >> skip >> log_ing

        toutes_taches += [fetch, archive, transform, quality, load, skip, log_ing]

    alerte = PythonOperator(
        task_id="alert_on_failure",
        python_callable=alert_on_failure,
        trigger_rule="one_failed",
    )

    log_fin = PythonOperator(
        task_id="log_execution",
        python_callable=log_execution,
        trigger_rule="all_done",
    )

    toutes_taches >> alerte
    toutes_taches >> log_fin
