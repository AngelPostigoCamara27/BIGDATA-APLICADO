import hashlib
import shutil
import logging
from pathlib import Path

# Configuración del sistema de logs
Path("../logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename='../logs/ejecucion_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def calcular_sha256(ruta_fichero):
    """Calcula el hash SHA-256 de un fichero leyendo en bloques de 64KB."""
    hasher = hashlib.sha256()
    with open(ruta_fichero, 'rb') as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def validar_lote():
    base_dir = Path("..")
    raw_dir = base_dir / "raw_batch_202611"
    manifest_path = raw_dir / "manifest_checksums.sha256"
    
    # Directorios de destino
    staging_dir = base_dir / "staging" / "verified"
    quarantine_dir = base_dir / "quarantine" / "tampered"
    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    if not manifest_path.exists():
        logging.error("No se encuentra el manifiesto de control.")
        return

    # Leer el manifiesto (asume formato: HASH  nombre_fichero.csv)
    firmas_esperadas = {}
    with open(manifest_path, 'r') as m:
        for linea in m:
            partes = linea.strip().split()
            if len(partes) >= 2:
                firmas_esperadas[partes[1]] = partes[0]

    archivos_procesados = 0
    archivos_rechazados = 0

    for archivo in raw_dir.glob("*.csv"):
        nombre_fichero = archivo.name
        if nombre_fichero in firmas_esperadas:
            hash_calculado = calcular_sha256(archivo)
            hash_esperado = firmas_esperadas[nombre_fichero]

            if hash_calculado == hash_esperado:
                # Fichero íntegro: mover a staging
                shutil.copy(archivo, staging_dir / nombre_fichero)
                logging.info(f"INTEGRIDAD OK: {nombre_fichero} movido a staging/verified/")
                archivos_procesados += 1
            else:
                # Fichero corrupto: aislar en cuarentena
                shutil.copy(archivo, quarantine_dir / nombre_fichero)
                logging.warning(f"CORRUPCIÓN DETECTADA: {nombre_fichero} movido a quarantine/tampered/")
                archivos_rechazados += 1

    # Guardar métricas temporales para la Fase 3
    with open(base_dir / "output_samples" / "quarantine_manifest.txt", "w") as f:
        f.write(f"Archivos Validados: {archivos_procesados}\n")
        f.write(f"Archivos Rechazados: {archivos_rechazados}\n")

if __name__ == "__main__":
    logging.info("Iniciando validación criptográfica (Fase 1)...")
    validar_lote()
    logging.info("Validación finalizada.")