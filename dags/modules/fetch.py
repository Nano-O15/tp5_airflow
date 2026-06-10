import json
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook

MINIO_BUCKET = "meteo-raw"
MINIO_CONN_ID = "minio_s3"


def fetch_meteo(ville: str, latitude: float, longitude: float, **context):
    hook = HttpHook(method="GET", http_conn_id="open_meteo_api")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,wind_speed_10m,precipitation",
        "timezone": "Europe/Paris",
    }

    logging.info(f"[fetch] Appel API Open-Meteo pour {ville}")
    response = hook.run(
        endpoint="/v1/forecast",
        data=params,
        extra_options={"timeout": 10},
    )
    raw_data = response.json()
    logging.info(f"[fetch] Réponse brute {ville} : {json.dumps(raw_data, indent=2)}")

    context["ti"].xcom_push(key=f"raw_{ville}", value=raw_data)


def archive_raw(ville: str, **context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(key=f"raw_{ville}", task_ids=f"fetch_meteo_{ville}")

    if not raw_data:
        raise ValueError(f"[archive] Aucune donnée brute à archiver pour {ville}.")

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_key = f"{context['ds']}/{ville}_raw.json"

    raw_bytes = json.dumps(raw_data, ensure_ascii=False, indent=2).encode("utf-8")

    s3_hook.load_bytes(
        bytes_data=raw_bytes,
        key=s3_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )

    logging.info(f"[archive] {ville} archivé dans MinIO : s3://{MINIO_BUCKET}/{s3_key}")
