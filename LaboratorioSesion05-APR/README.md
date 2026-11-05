# Laboratorio 1: Ingesta de Fuentes Heterogéneas (Batch y Streaming)

* **Módulo:** Sistemas de Big Data (IABD)
* **Alumno:** Ángel Postigo Rodríguez
* **Entorno:** Docker sobre WSL2, Apache Hadoop 3.4.0, MySQL 8.0, Apache Flume 1.9.0

---

## 1. Descripción del Proyecto

Este repositorio contiene el despliegue, configuración y scripts para los procesos de extracción e ingesta de datos del Laboratorio 1:

* **Parte A (Batch):** Migración masiva desde MySQL hacia HDFS utilizando Apache Sqoop.
* **Parte B (Streaming):** Ingesta continua de logs y eventos hacia HDFS utilizando Apache Flume con inyección dinámica del Classpath de Hadoop.

La memoria técnica completa con explicaciones detalladas, respuestas a las cuestiones teóricas y evidencias gráficas de ejecución se encuentra en:
👉 `Memoria_Laboratorio_1_Postigo_Rodríguez_Ángel.pdf`

---

## 2. Estructura Principal del Repositorio

El proyecto está organizado de la siguiente manera:

```text
LaboratorioSesion05-APR/
├── CAPTURAS/                                  # Evidencias gráficas de ejecución
│   ├── Evidencias de Ejecución (Flume).png    # Capturas de ingesta en streaming
│   ├── Evidencias de Ejecución (Sqoop)1.png   # Capturas de ingesta batch con Sqoop (Parte 1)
│   └── Evidencias de Ejecución (Sqoop)2.png   # Capturas de ingesta batch con Sqoop (Parte 2)
│
├── Docker/
│   ├── docker-compose.yml                     # Orquestación del ecosistema de contenedores
│   ├── hadoop.env                             # Variables de entorno de Hadoop
│   ├── .env.example                           # Plantilla de variables de entorno para Docker Compose
│   │
│   ├── flume/
│   │   ├── flume-conf.properties              # Configuración del agente Flume (SpoolDir -> Memory -> HDFS)
│   │   └── instalar_flume.sh                  # Script automatizado para descargar y desplegar Flume
│   │
│   ├── Jupyter/
│   │   └── Dockerfile                         # Imagen personalizada de Jupyter para el clúster
│   │
│   └── work/                                  # Espacio de trabajo y notebooks de apoyo
│       └── Procesamiento_Analitico_Spark.ipynb
│
├── Memoria_Laboratorio_1_Postigo_Rodríguez_Ángel.pdf  # Memoria técnica del laboratorio
└── README.md                                  # Documentación del proyecto
```

---

## 3. Instrucciones de Despliegue y Ejecución

### Requisitos Previos
* Docker y Docker Compose instalados.
* Git.

### Paso 1: Levantar el ecosistema Docker
Desde el directorio `LaboratorioSesion05-APR/Docker`:
```bash
cp .env.example .env
docker compose up -d
```

### Paso 2: Ingesta Batch con Apache Sqoop (Parte A)
Ejecutar la extracción desde MySQL e ingesta en HDFS mediante el cliente Sqoop provisto en los contenedores.

### Paso 3: Ingesta Streaming con Apache Flume (Parte B)
1. Instalar y preparar Apache Flume utilizando el script automatizado:
   ```bash
   bash flume/instalar_flume.sh
   ```
2. Iniciar el agente de Flume con la configuración definida en `flume/flume-conf.properties`:
   ```bash
   /opt/flume/bin/flume-ng agent \
     --conf /opt/flume/conf \
     --conf-file flume/flume-conf.properties \
     --name a1 \
     -Dflume.root.logger=INFO,console
   ```
3. Depositar archivos en el directorio spooling (`/data/flume/spool`) para verificar la ingesta automática en HDFS en `hdfs://namenode:8020/user/iabd_admin/ingesta/streaming/ventas`.
