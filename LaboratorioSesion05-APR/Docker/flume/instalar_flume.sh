#!/bin/bash
set -e

echo "Instalando Apache Flume 1.9.0..."
wget -qO /tmp/flume.tar.gz https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz
tar -zxf /tmp/flume.tar.gz -C /opt/
rm -rf /opt/flume
mv /opt/apache-flume-1.9.0-bin /opt/flume

echo "Configurando directorios de trabajo..."
mkdir -p /opt/flume/conf /data/flume/logs /data/flume/spool

echo "Despliegue de Flume completado con exito."