# Laboratorio Apache Hadoop Multi-Nodo con Docker

Despliegue y auditoria de un cluster distribuido Apache Hadoop sobre contenedores Docker, verificando el almacenamiento resiliente en HDFS, la tolerancia a fallos ante caida de nodos y el computo distribuido MapReduce sobre YARN.

## Datos del proyecto

- **Modulo:** 5075 - Big Data aplicado (RA 2)
- **Centro:** Campus Camara de Comercio de Sevilla
- **Autor:** Ángel Postigo Rodríguez

## Arquitectura desplegada

El entorno se ejecuta sobre una red puente aislada (`hadoop-net`) utilizando la imagen oficial `apache/hadoop:3` con 6 servicios:

| Servicio | Rol | Puerto expuesto |
| :--- | :--- | :--- |
| `namenode` | Maestro HDFS (gestion de metadatos y bloques) | `9870` (Web UI) |
| `datanode1` | Trabajador HDFS (almacenamiento de bloques) | Interno |
| `datanode2` | Trabajador HDFS (almacenamiento de bloques) | Interno |
| `datanode3` | Trabajador HDFS (almacenamiento de bloques) | Interno |
| `resourcemanager` | Maestro YARN (planificacion de recursos) | `8088` (Web UI) |
| `nodemanager` | Trabajador YARN (ejecucion de contenedores) | Interno |

### Parametros clave de HDFS

- **Tamano de bloque (`dfs.blocksize`):** 64 MiB (`67108864` bytes)
- **Factor de replicacion (`dfs.replication`):** 2

## Instrucciones de arranque

1. Clonar el repositorio y acceder al directorio de infraestructura:
   ```bash
   git clone [https://github.com/AngelCamara26/IABD-5075-LabHadoop-Postigo-Rodriguez.git](https://github.com/AngelCamara26/IABD-5075-LabHadoop-Postigo-Rodriguez.git)
   cd IABD-5075-LabHadoop-Postigo-Rodriguez/docker