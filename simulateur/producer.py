import time
import json
import random
from kafka import KafkaProducer

# Configuration Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Démarrage du simulateur de séismes (Producer)...")

try:
    while True:
        # Simulation d'un épicentre aléatoire autour de Casablanca
        alerte = {
            "lat": 33.5 + random.uniform(-0.2, 0.2),
            "lon": -7.6 + random.uniform(-0.2, 0.2),
            "magnitude": round(random.uniform(4.0, 7.5), 1)
        }
        
        producer.send('seismes', alerte)
        print(f"📡 Alerte envoyée : {alerte}")
        
        time.sleep(5) # Une alerte toutes les 5 secondes
except KeyboardInterrupt:
    print("Arrêt du simulateur.")
