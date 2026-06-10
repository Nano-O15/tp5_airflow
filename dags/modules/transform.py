import logging

CHAMPS_REQUIS = ["temperature_2m", "wind_speed_10m", "precipitation", "time"]


def transform_data(ville: str, **context):
    ti = context["ti"]
    raw_data = ti.xcom_pull(key=f"raw_{ville}", task_ids=f"fetch_meteo_{ville}")

    if not raw_data:
        raise ValueError(f"[transform] Aucune donnée brute pour {ville}.")

    current = raw_data.get("current")
    if not current:
        raise ValueError(f"[transform] Clé 'current' absente pour {ville}.")

    manquants = [c for c in CHAMPS_REQUIS if c not in current]
    if manquants:
        raise ValueError(f"[transform] Champs manquants pour {ville} : {manquants}")

    transformed = {
        "ville": ville,
        "heure": current["time"],
        "temperature_c": current["temperature_2m"],
        "vent_kmh": current["wind_speed_10m"],
        "precipitation_mm": current["precipitation"],
        "date_execution": context["ds"],
    }

    logging.info(f"[transform] {ville} → {transformed}")
    ti.xcom_push(key=f"transformed_{ville}", value=transformed)
