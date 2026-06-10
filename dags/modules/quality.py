import logging

TEMP_MIN = -50
TEMP_MAX = 60


def check_quality(ville: str, **context):
    ti = context["ti"]
    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )

    # transformed["temperature_c"] = 999

    anomalies = []

    if not transformed:
        anomalies.append("Aucune donnée transformée disponible.")
    else:
        temp = transformed.get("temperature_c")
        vent = transformed.get("vent_kmh")
        precip = transformed.get("precipitation_mm")
        heure = transformed.get("heure")

        if temp is None or not (TEMP_MIN <= temp <= TEMP_MAX):
            anomalies.append(f"Température hors plage : {temp}°C (attendu : {TEMP_MIN} à {TEMP_MAX})")

        if vent is None or vent < 0:
            anomalies.append(f"Vitesse du vent invalide : {vent} km/h")

        if precip is None or precip < 0:
            anomalies.append(f"Précipitations invalides : {precip} mm")

        if not heure:
            anomalies.append("Heure de mesure manquante.")

    if anomalies:
        for anomalie in anomalies:
            logging.warning(f"[quality] ANOMALIE {ville} — {anomalie}")
        context["ti"].xcom_push(key=f"anomalies_{ville}", value=anomalies)
        return f"skip_load_{ville}"

    logging.info(f"[quality] {ville} — contrôle qualité OK")
    return f"load_data_{ville}"


def simulate_anomaly(ville: str, **context):
    ti = context["ti"]
    transformed = ti.xcom_pull(
        key=f"transformed_{ville}", task_ids=f"transform_data_{ville}"
    )
    if transformed:
        transformed["temperature_c"] = 999
        ti.xcom_push(key=f"transformed_{ville}", value=transformed)
        logging.warning(f"[simulate_anomaly] Température forcée à 999°C pour {ville}")
