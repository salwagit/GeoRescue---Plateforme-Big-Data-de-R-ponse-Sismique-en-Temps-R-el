import json
import os

import psycopg2
from kafka import KafkaConsumer

# NOTE: Pour la simplicite de l'execution locale sans cluster Spark complet,
# ce script utilise un Consumer Python classique couple a PostGIS pour executer
# la logique spatiale. Dans un vrai cluster Big Data, on utiliserait PySpark + Apache Sedona.

print("⚙️ Demarrage du processeur de donnees...")

password = os.environ.get("POSTGRES_PASSWORD")
if not password:
    raise RuntimeError("La variable d'environnement POSTGRES_PASSWORD est obligatoire.")

conn = psycopg2.connect(
    dbname=os.environ.get("POSTGRES_DB", "georescue"),
    user=os.environ.get("POSTGRES_USER", "user"),
    password=password,
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5435")),
)
conn.autocommit = True
cursor = conn.cursor()

consumer = KafkaConsumer(
    "seismes",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
)

print("🎧 En attente des alertes sismiques...")

for message in consumer:
    alerte = message.value
    lon, lat, mag = alerte["lon"], alerte["lat"], alerte["magnitude"]

    insert_query = """
        INSERT INTO alertes_sismiques (magnitude, geom)
        VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        RETURNING id;
    """
    cursor.execute(insert_query, (mag, lon, lat))
    alerte_id = cursor.fetchone()[0]

    print(f"\n🚨 [ALERTE #{alerte_id}] Seisme Mag {mag} detecte aux coords ({lat}, {lon})")

    danger_query = """
        SELECT nom, ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000 AS dist_km
        FROM hopitaux
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 20000)
    """
    cursor.execute(danger_query, (lon, lat, lon, lat))
    hopitaux_touches = cursor.fetchall()

    if hopitaux_touches:
        print(f"⚠️  DANGER : {len(hopitaux_touches)} hôpitaux dans la zone d'impact (20km) :")
        for hopital in hopitaux_touches:
            print(f"   - {hopital[0]} (a {hopital[1]:.1f} km)")
    else:
        print("✅ Aucun hôpital dans la zone critique immediate.")
