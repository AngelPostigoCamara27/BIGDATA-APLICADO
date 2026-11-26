# Migración de ingestión batch a Event Streaming con Apache Kafka 4.0 (KRaft)

Repositorio de la práctica de laboratorio del módulo **5074 - Sistemas de Big Data** (Curso de Especialización en Inteligencia Artificial y Big Data).

El proyecto implementa un prototipo de ingestión de telemetría en tiempo real para la flota frigorífica de **FríoSur Logística S. L.**, sustituyendo el envío de pequeños ficheros CSV a HDFS por un flujo desacoplado sobre Apache Kafka 4.0 en modo KRaft, con garantías de orden por vehículo y procesamiento *at-least-once*.

---

## Estructura del repositorio

```text
kafka-cadena-frio-5074/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── productor_frio.py
│   └── consumidor_alertas.py
└── evidencias/
    ├── E1_topic_describe.txt
    ├── E2_productor_salida.txt
    ├── E3A_group_antes_fallo.txt
    ├── E3B_group_despues_fallo.txt
    └── alertas_detectadas.log
```

---

## Prerrequisitos

- Docker Desktop (o Docker Engine con Docker Compose v2).
- Python 3.10 o superior.
- Cliente Python `kafka-python-ng` (definido en `requirements.txt`).

---

## Guía de despliegue y ejecución

### 1. Arrancar Apache Kafka 4.0 (KRaft) y crear el topic

Desde la raíz del proyecto:

```bash
docker compose up -d
docker logs kafka-kraft-5074
```

Una vez iniciado el broker, se crea el topic `telemetria-frio` con 3 particiones y factor de replicación 1, y se verifican sus metadatos (Evidencia `E1`):

```bash
docker exec kafka-kraft-5074 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic telemetria-frio \
  --partitions 3 --replication-factor 1

docker exec kafka-kraft-5074 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic telemetria-frio
```

### 2. Configurar el entorno Python

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Prueba de consumo paralelo y reequilibrio en caliente

1. Abrir dos terminales (Consumidor A y Consumidor B), activar el entorno virtual y arrancar el consumidor en ambas:
   ```powershell
   python src/consumidor_alertas.py
   ```
2. En una tercera terminal, comprobar que el grupo `grupo-alertas-frio` ha repartido las 3 particiones entre las dos instancias (2 particiones en una y 1 en la otra, Evidencia `E3-A`):
   ```powershell
   docker exec kafka-kraft-5074 /opt/kafka/bin/kafka-consumer-groups.sh `
     --bootstrap-server localhost:9092 `
     --describe --group grupo-alertas-frio
   ```
3. Lanzar el productor desde la tercera terminal (Evidencia `E2`):
   ```powershell
   python src/productor_frio.py
   ```
4. Cuando se hayan emitido aproximadamente 25-30 eventos, detener abruptamente uno de los dos consumidores con `Ctrl + C`.
5. Al finalizar la emisión del productor, volver a inspeccionar el grupo de consumidores (Evidencia `E3-B`) y comprobar el fichero `evidencias/alertas_detectadas.log` (Evidencia `E4`).

### Interpretación de offsets y validación de integridad

En la salida de `kafka-consumer-groups.sh --describe` se monitorizan tres métricas clave por cada partición:

- **CURRENT-OFFSET:** posición del último mensaje procesado y confirmado explícitamente (`consumer.commit()`) por el grupo de consumidores.
- **LOG-END-OFFSET:** posición del último mensaje escrito por el productor en el log de la partición.
- **LAG:** diferencia entre `LOG-END-OFFSET` y `CURRENT-OFFSET`, es decir, el número de mensajes pendientes de consumir.

En la evidencia `E3-A` se observa el reparto inicial de las particiones 0, 1 y 2 entre dos `CONSUMER-ID` distintos. Tras forzar la caída de una instancia a mitad de la emisión, Kafka dispara un *rebalance* automático: en `E3-B` un único `CONSUMER-ID` asume las 3 particiones y finaliza con `LAG = 0` en todas ellas. Al confirmar el offset manualmente solo después de escribir y volcar a disco (`flush()`) en `evidencias/alertas_detectadas.log`, se cumple la semántica *at-least-once* y se verifica en `E4` que el total de alertas registradas coincide con las anomalías emitidas por el productor (cero pérdida de datos).

---

## Análisis arquitectónico

### 1. HDFS frente a Kafka: solución al *Small Files Problem*

En Hadoop HDFS, el NameNode mantiene en memoria RAM el árbol completo de metadatos del sistema de ficheros (inodos, directorios y mapeo de bloques), consumiendo en torno a 150 bytes por cada objeto almacenado [2]. En la arquitectura anterior de FríoSur Logística, cada camión generaba un CSV de pocos kilobytes cada minuto; esto provocaba que se acumularan miles de ficheros minúsculos muy por debajo del tamaño de bloque estándar de HDFS (128 MB), saturando la memoria del NameNode y ralentizando todo el clúster. Además, el procesamiento *batch* introducía retrasos de horas para detectar roturas de la cadena de frío.

Apache Kafka soluciona ambos problemas al actuar como capa de ingestión desacoplada. En lugar de abrir un fichero individual por cada lectura, el broker añade los eventos de forma secuencial (*append-only*) a ficheros de segmento continuos dentro de cada partición [1]. Así, el sistema de alertas consume los mensajes en tiempo real con latencias de milisegundos, mientras que el almacenamiento histórico en HDFS queda desacoplado para realizarse posteriormente en lotes compactos (por ejemplo, ficheros Parquet de gran tamaño generados con Spark), eliminando la sobrecarga sobre el NameNode.

### 2. Claves de partición y paralelismo en el Consumer Group

- **Efecto de eliminar el parámetro `key`:** Si se omite `key=vehiculo` en `producer.send(...)`, Kafka distribuye los eventos entre las 3 particiones mediante un particionador equilibrado (*sticky partitioner* / *round-robin*) [1]. Dado que Kafka solo garantiza el orden estricto de lectura dentro de una misma partición y no a nivel global del topic, las lecturas de un mismo camión se repartirían entre distintas particiones y serían leídas en paralelo por consumidores diferentes, perdiendo el orden cronológico de su temperatura y ruta GPS. Mantener `vehiculo` como clave garantiza mediante hash determinista que todos los eventos de un mismo camión entren siempre en la misma partición.
- **Papel de una 4.ª instancia con 3 particiones:** Dentro de un mismo Consumer Group, una partición solo puede estar asignada a un único consumidor al mismo tiempo [1]. El grado máximo de paralelismo viene limitado por el número de particiones del topic. Por tanto, si se conectan 4 instancias a `grupo-alertas-frio` manteniendo 3 particiones, tres instancias recibirán una partición cada una y la cuarta permanecerá inactiva (*idle*) sin recibir mensajes, funcionando únicamente como nodo en espera (*standby*) listo para asumir una partición en caso de que caiga alguno de los consumidores activos.

### 3. Puente hacia Apache Spark (configuración de listeners en Docker)

Al establecer conexión con el clúster, el broker responde al cliente con las direcciones anunciadas en `KAFKA_ADVERTISED_LISTENERS` para el tráfico de datos posterior [1]. Si el broker solo anunciara `localhost:9092`, un contenedor de Apache Spark Structured Streaming desplegado en la misma red Docker intentaría conectarse a su propia interfaz local (`localhost`) y la conexión fallaría.

Para que convivan el acceso desde el host local (`localhost:9092`) y el acceso interno desde Spark dentro de la red puente de Docker (`kafka:9092`), es necesario declarar dos listeners separados en `docker-compose.yml` (uno interno y otro externo), mapeando el puerto externo hacia el anfitrión para evitar colisiones dentro del contenedor:

```yaml
ports:
  - "9092:29092"
environment:
  KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
  KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:9092
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
  KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

De este modo, los contenedores en la red Docker (como Spark) se conectan a `kafka:9092` usando el listener `INTERNAL`, mientras que los productores o consumidores ejecutados en la máquina anfitriona siguen accediendo a `localhost:9092` a través del puerto mapeado hacia el listener `EXTERNAL`.

---

## Referencias

[1] Apache Software Foundation, "Apache Kafka Documentation," Apache Kafka. [En línea]. Disponible en: https://kafka.apache.org/documentation/

[2] Apache Software Foundation, "HDFS Architecture Guide," Apache Hadoop Documentation. [En línea]. Disponible en: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html