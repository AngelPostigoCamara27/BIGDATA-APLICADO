import json
import os
from kafka import KafkaConsumer

os.makedirs("evidencias", exist_ok=True)

consumer = KafkaConsumer(
    "telemetria-frio",
    bootstrap_servers="localhost:9092",
    group_id="grupo-alertas-frio",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Esperando mensajes...")

with open("evidencias/alertas_detectadas.log", "a", encoding="utf-8") as log:
    try:
        for msg in consumer:
            datos = msg.value
            temp = datos["temp_c"]
            print(f"particion {msg.partition} | offset {msg.offset} | {msg.key} | {temp} C")

            if temp > -18.0:
                print(f"*** ALERTA FRIO: {msg.key} a {temp} C (particion {msg.partition}, offset {msg.offset}) ***")
                linea = f"particion={msg.partition} | offset={msg.offset} | vehiculo={datos['vehiculo']} | temp_c={temp} | lat={datos['lat']} | lon={datos['lon']} | timestamp={datos['timestamp_iso']}\n"
                log.write(linea)
                log.flush()

            consumer.commit()
    except KeyboardInterrupt:
        print("Consumidor detenido.")
    finally:
        consumer.close()