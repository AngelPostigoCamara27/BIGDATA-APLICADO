# Laboratorio 1: Ingesta de Fuentes Heterogéneas (Batch y Streaming)

* **Modulo:** Sistemas de Big Data (IABD)
* **Alumno:** Angel Postigo Rodriguez
* **Entorno:** Docker sobre WSL2, Apache Hadoop 3.4.0, MySQL 8.0, Apache Flume 1.9.0

---

## 1. Descripcion del Proyecto
Este repositorio contiene el despliegue y configuracion para los procesos de extraccion e ingesta de datos del Laboratorio 1:
* **Parte A (Batch):** Migracion masiva desde MySQL hacia HDFS utilizando Apache Sqoop.
* **Parte B (Streaming):** Ingesta continua de logs y eventos hacia HDFS utilizando Apache Flume con inyeccion dinamica del Classpath de Hadoop.

La memoria completa con las evidencias graficas de ejecucion y respuestas a las cuestiones teoricas se encuentra en el archivo:
`Memoria_Laboratorio_1_Postigo_Rodriguez_Angel.pdf`

---

## 2. Instrucciones de Despliegue

### Requisitos Previos
* Docker y Docker Compose instalados.
* Git.

### Paso 1: Levantar el ecosistema
```bash
cp .env.example .env
docker compose up -d