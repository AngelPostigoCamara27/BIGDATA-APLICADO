import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    acks="all",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

camiones = ["CAM-01", "CAM-02", "CAM-03", "CAM-04"]
coords = {
    "CAM-01": (37.3891, -5.9845),
    "CAM-02": (37.2829, -5.9209),
    "CAM-03": (37.4100, -6.0050),
    "CAM-04": (37.3550, -5.9500),
}

posiciones_anomalias = random.sample(range(60), 8)
anomalias = 0

for i in range(60):
    camion = camiones[i % 4]
    lat, lon = coords[camion]

    if i in posiciones_anomalias:
        temp = round(random.uniform(-17.5, -14.0), 1)
        anomalias += 1
    else:
        temp = round(random.uniform(-22.0, -18.5), 1)

    evento = {
        "vehiculo": camion,
        "temp_c": temp,
        "lat": round(lat + random.uniform(-0.005, 0.005), 4),
        "lon": round(lon + random.uniform(-0.005, 0.005), 4),
        "timestamp_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    res = producer.send("telemetria-frio", key=camion, value=evento).get(timeout=10)
    print(f"{camion} | {temp} C | particion {res.partition} | offset {res.offset}")
    time.sleep(0.5)

producer.flush()
producer.close()
print(f"\nTotal anomalias enviadas: {anomalias}")