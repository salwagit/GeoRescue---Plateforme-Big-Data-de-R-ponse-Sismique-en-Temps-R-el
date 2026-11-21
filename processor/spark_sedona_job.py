"""Consommateur Kafka -> PostGIS.

Rôle : lit les alertes du topic « seismes », les valide et les insère dans
`alertes_sismiques`. Tout le traitement spatial (zones d'impact, population
exposée, affectation des hôpitaux, notification temps réel) est fait par le
TRIGGER SQL `trg_alerte_apres_insert` défini dans init.sql : la logique reste
dans la base de données, le processeur ne fait que transporter l'événement.

NOTE : malgré son nom historique, ce script n'utilise ni Spark ni Sedona.
"""
import json
import os
import time

import psycopg2
from kafka import KafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

password = os.environ.get("POSTGRES_PASSWORD")
if not password:
    raise RuntimeError("La variable d'environnement POSTGRES_PASSWORD est obligatoire.")


def connect_db():
    conn = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB", "georescue"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=password,
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5435")),
    )
    conn.autocommit = True
    return conn


def parse(raw):
    """Désérialise sans jamais lever : un message illisible ne doit pas tuer le consumer."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def valide(m):
    try:
        lat, lon, mag = float(m["lat"]), float(m["lon"]), float(m["magnitude"])
    except (TypeError, KeyError, ValueError):
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180 and 0 <= mag <= 10):
        return None
    return lat, lon, mag


print("⚙️  Démarrage du processeur...")
conn = connect_db()
consumer = KafkaConsumer(
    "seismes",
    bootstrap_servers=[BOOTSTRAP],
    group_id="georescue-processor",
    auto_offset_reset="latest",
    value_deserializer=parse,
)
print("🎧 En attente des alertes sismiques...")

for message in consumer:
    donnees = valide(message.value) if isinstance(message.value, dict) else None
    if donnees is None:
        print(f"⚠️  Message ignoré (invalide) : {message.value!r}")
        continue
    lat, lon, mag = donnees

    for tentative in range(2):
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO alertes_sismiques (magnitude, geom, source)
                   VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), 'SIMULATEUR')
                   RETURNING id, population_severe, victimes_estimees, deficit_lits""",
                (mag, lon, lat),
            )
            alerte_id = cur.fetchone()[0]
            # Le trigger (AFTER INSERT) a calculé l'impact : on relit le résultat.
            cur.execute(
                "SELECT population_severe, victimes_estimees, deficit_lits "
                "FROM alertes_sismiques WHERE id = %s", (alerte_id,))
            pop, victimes, deficit = cur.fetchone()
            cur.execute(
                """SELECT h.nom, d.distance_km, d.patients_assignes, d.dans_zone_severe
                   FROM dispatches d JOIN hopitaux h ON h.id = d.hopital_id
                   WHERE d.alerte_id = %s ORDER BY d.rang""", (alerte_id,))
            lignes = cur.fetchall()
            cur.close()

            print(f"\n🚨 [ALERTE #{alerte_id}] M{mag} en ({lat:.3f}, {lon:.3f}) "
                  f"| pop. zone sévère : {pop} | victimes estimées : {victimes} | lits manquants : {deficit}")
            for nom, dist, patients, touche in lignes:
                etat = "HORS SERVICE (zone sévère)" if touche else f"{patients} patients"
                print(f"   - {nom} ({dist:.1f} km) : {etat}")
            break
        except psycopg2.Error as e:
            print(f"❌ Erreur base de données : {e}")
            if tentative == 0:
                time.sleep(2)
                try:
                    conn = connect_db()
                except psycopg2.Error:
                    pass