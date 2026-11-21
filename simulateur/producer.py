"""Simulateur de séismes (Producer Kafka).

Plus réaliste que des tirages uniformes :
  - magnitudes selon la loi de Gutenberg-Richter (beaucoup de petits séismes,
    très peu de gros) ;
  - épicentres concentrés autour d'une faille (fictive) ;
  - les séismes forts (M >= 5) déclenchent des répliques proches et plus faibles
    (décroissance type loi d'Omori) -> permet le regroupement ST_ClusterDBSCAN.
"""
import argparse
import json
import math
import os
import random
import time

from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
FAILLE = [(-7.95, 33.35), (-7.75, 33.45), (-7.55, 33.60), (-7.30, 33.72)]  # (lon, lat)
M_MIN, M_MAX, B = 3.8, 7.5, 1.0


def magnitude_gutenberg_richter():
    u = random.random()
    m = M_MIN - math.log10(1 - u * (1 - 10 ** (-B * (M_MAX - M_MIN)))) / B
    return round(min(m, M_MAX), 1)


def epicentre():
    if random.random() < 0.7:  # près de la faille
        i = random.randrange(len(FAILLE) - 1)
        t = random.random()
        lon = FAILLE[i][0] + t * (FAILLE[i + 1][0] - FAILLE[i][0])
        lat = FAILLE[i][1] + t * (FAILLE[i + 1][1] - FAILLE[i][1])
        return lat + random.gauss(0, 0.03), lon + random.gauss(0, 0.03)
    return 33.5 + random.uniform(-0.2, 0.2), -7.6 + random.uniform(-0.2, 0.2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=5.0, help="secondes entre deux événements")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("🚀 Démarrage du simulateur de séismes (Producer)...")

    def envoyer(lat, lon, mag, etiquette):
        alerte = {"lat": round(lat, 5), "lon": round(lon, 5), "magnitude": mag}
        producer.send("seismes", alerte)
        producer.flush()
        print(f"📡 {etiquette} : {alerte}")

    try:
        while True:
            lat, lon = epicentre()
            mag = magnitude_gutenberg_richter()
            envoyer(lat, lon, mag, "Séisme")
            if mag >= 5.0:
                for k in range(random.randint(2, 3 + int(mag - 4))):
                    time.sleep(args.interval * (0.4 + 0.5 * k))  # répliques de plus en plus espacées
                    m2 = round(max(M_MIN - 0.3, mag - random.uniform(1.0, 2.0)), 1)
                    envoyer(lat + random.gauss(0, 0.03), lon + random.gauss(0, 0.03), m2, "  ↳ réplique")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Arrêt du simulateur.")


if __name__ == "__main__":
    main()