# Reporte de Perfilado y Calidad de Datos (KDD)

## 1. Validación de Integridad (Fase 1)
- **Tasa de Integridad Criptográfica**: Se procesaron el 100% de los lotes. El fichero `transacciones_tienda_sur.csv` fue segregado exitosamente a la zona de cuarentena al no coincidir su firma SHA-256 con el manifiesto oficial. Esto previene el envenenamiento de los modelos (Data Poisoning) por archivos incompletos o manipulados en tránsito.

## 2. Decisiones de Limpieza e Imputación (Fase 2)
Se establecieron las siguientes políticas de calidad basadas en el estándar ISO/IEC 25012:
- **Exactitud y Coherencia Lógica:** Se descartaron registros con importes negativos o iguales a cero, así como transacciones con unidades inferiores a 1.
- **Trazabilidad (Nulos):** Los registros carentes de `id_transaccion` fueron aislados en `quarantine/bad_records.csv`, ya que un ID es obligatorio para el cruce en bases de datos relacionales del almacén BI.
- **Consistencia Temporal:** Se forzó la conversión al estándar ISO 8601. Fechas futuras o fuera del ejercicio fiscal actual fueron descartadas por considerarse anomalías de los sensores de caja.

## 3. Métricas de Distribución Post-Saneamiento (Fase 3)
{
    "calidad_iso_25012": {
        "tasa_completitud_porcentaje": 90.0,
        "integridad_criptografica": {
            "archivos_validados": 2,
            "archivos_rechazados": 1
        }
    },
    "distribucion_importe_eur": {
        "media": 161.41,
        "mediana": 74.48,
        "desviacion_estandar": 528.78,
        "percentil_25": 55.22,
        "percentil_75": 88.57
    }
}

**Conclusión técnica:** El conjunto de datos alojado en `processed/dataset_kdd_listo.csv` se encuentra libre de artefactos y valores extremos ilógicos (outliers de dominio), garantizando una convergencia más rápida y precisa para los algoritmos de Machine Learning que se apliquen en la fase analítica del proceso KDD.