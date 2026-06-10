import logging

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_table_cible() -> str:
    return Variable.get("meteo_table", default_var="observations_meteo")


def load_data(ville: str, **context):
    ti = context["ti"]
    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )

    if not transformed:
        raise ValueError(f"[load] Aucune donnée transformée pour {ville}.")

    table = get_table_cible()
    hook = PostgresHook(postgres_conn_id="postgres_meteo")

    sql = f"""
        INSERT INTO {table}
            (ville, heure, temperature_c, vent_kmh, precipitation_mm, date_execution)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ville, heure) DO NOTHING;
    """

    hook.run(sql, parameters=(
        transformed["ville"],
        transformed["heure"],
        transformed["temperature_c"],
        transformed["vent_kmh"],
        transformed["precipitation_mm"],
        transformed["date_execution"],
    ))

    logging.info(f"[load] {ville} inséré dans {table}.")


def log_ingestion(ville: str, **context):
    ti = context["ti"]
    hook = PostgresHook(postgres_conn_id="postgres_meteo")

    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )
    anomalies = ti.xcom_pull(
        key=f"anomalies_{ville}", task_ids=f"check_quality_{ville}"
    )

    if not transformed:
        statut = "failure"
        message = f"Aucune donnée transformée pour {ville}."
    elif anomalies:
        statut = "anomalie"
        message = f"Anomalies détectées : {'; '.join(anomalies)}"
    else:
        statut = "success"
        message = None

    sql = """
        INSERT INTO log_ingestion
            (dag_id, task_id, ville, statut, message, date_execution)
        VALUES
            (%s, %s, %s, %s, %s, %s);
    """

    hook.run(sql, parameters=(
        context["dag"].dag_id,
        f"log_ingestion_{ville}",
        ville,
        statut,
        message,
        context["ds"],
    ))

    logging.info(f"[log_ingestion] {ville} → statut={statut}")
