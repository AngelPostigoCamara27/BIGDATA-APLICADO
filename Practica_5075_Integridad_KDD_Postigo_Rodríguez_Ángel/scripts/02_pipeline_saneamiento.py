import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime

# Configuración de logs
logging.basicConfig(
    filename='../logs/ejecucion_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def ejecutar_pipeline_saneamiento():
    base_dir = Path("..")
    staging_dir = base_dir / "staging" / "verified"
    processed_dir = base_dir / "processed"
    quarantine_dir = base_dir / "quarantine"
    processed_dir.mkdir(exist_ok=True)
    
    archivos_csv = list(staging_dir.glob("*.csv"))
    if not archivos_csv:
        logging.error("No hay archivos verificados en staging.")
        return

    # 1. Carga y concatenación
    df_list = [pd.read_csv(f) for f in archivos_csv]
    df = pd.concat(df_list, ignore_index=True)
    total_registros_iniciales = len(df)
    logging.info(f"Dataset cargado. Registros iniciales: {total_registros_iniciales}")

    # 2. Gestión de Nulos
    # Segregar transacciones sin ID
    mascara_sin_id = df['id_transaccion'].isnull()
    df_malos = df[mascara_sin_id].copy()
    df_validos = df[~mascara_sin_id].copy()

    # 3. Coherencia Temporal (ISO 8601) y filtrado de años anómalos
    df_validos['timestamp'] = pd.to_datetime(df_validos['timestamp'], errors='coerce')
    anyo_actual = datetime.now().year
    
    mascara_fecha_invalida = (
        df_validos['timestamp'].isnull() | 
        (df_validos['timestamp'].dt.year > anyo_actual) | 
        (df_validos['timestamp'].dt.year < anyo_actual) # Asumiendo que solo queremos datos del año en curso
    )
    df_malos = pd.concat([df_malos, df_validos[mascara_fecha_invalida]])
    df_validos = df_validos[~mascara_fecha_invalida]

    # 4. Reglas de Dominio (importe > 0, unidades >= 1)
    df_validos['importe_eur'] = pd.to_numeric(df_validos['importe_eur'], errors='coerce')
    df_validos['unidades'] = pd.to_numeric(df_validos['unidades'], errors='coerce')
    
    mascara_dominio = (df_validos['importe_eur'] > 0) & (df_validos['unidades'] >= 1)
    df_malos = pd.concat([df_malos, df_validos[~mascara_dominio]])
    df_validos = df_validos[mascara_dominio]

    # Imputar nulos en variables secundarias (ej. importe_eur si fuera secundario, aunque aquí es clave)
    # df_validos['columna_secundaria'].fillna(df_validos['columna_secundaria'].median(), inplace=True)

    # 5. Exportación
    df_malos.to_csv(quarantine_dir / "bad_records.csv", index=False)
    df_validos.to_csv(processed_dir / "dataset_kdd_listo.csv", index=False)
    logging.info(f"Limpieza finalizada. Registros válidos: {len(df_validos)}, Registros en cuarentena: {len(df_malos)}")

    # 6. Generación de Métricas de Perfilado (ISO/IEC 25012)
    tasa_completitud = (len(df_validos) / total_registros_iniciales) * 100
    
    # Leer métricas de criptografía de la Fase 1
    archivos_validados, archivos_rechazados = 0, 0
    try:
        with open(base_dir / "output_samples" / "quarantine_manifest.txt", "r") as f:
            lineas = f.readlines()
            archivos_validados = int(lineas[0].split(":")[1].strip())
            archivos_rechazados = int(lineas[1].split(":")[1].strip())
    except Exception:
        pass

    metricas = {
        "calidad_iso_25012": {
            "tasa_completitud_porcentaje": round(tasa_completitud, 2),
            "integridad_criptografica": {
                "archivos_validados": archivos_validados,
                "archivos_rechazados": archivos_rechazados
            }
        },
        "distribucion_importe_eur": {
            "media": round(df_validos['importe_eur'].mean(), 2),
            "mediana": round(df_validos['importe_eur'].median(), 2),
            "desviacion_estandar": round(df_validos['importe_eur'].std(), 2),
            "percentil_25": round(df_validos['importe_eur'].quantile(0.25), 2),
            "percentil_75": round(df_validos['importe_eur'].quantile(0.75), 2)
        }
    }

    ruta_metricas = base_dir / "output_samples" / "metricas_perfilado.json"
    with open(ruta_metricas, 'w') as json_file:
        json.dump(metricas, json_file, indent=4)
    logging.info("Métricas de perfilado exportadas a JSON.")

if __name__ == "__main__":
    logging.info("Iniciando pipeline de saneamiento (Fase 2 y 3)...")
    ejecutar_pipeline_saneamiento()