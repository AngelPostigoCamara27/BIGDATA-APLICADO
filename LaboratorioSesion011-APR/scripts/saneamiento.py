import hashlib
import pandas as pd


def calcular_sha256(ruta_archivo):
  sha256 = hashlib.sha256()
  with open(ruta_archivo, "rb") as f:
    for bloque in iter(lambda: f.read(4096), b""):
      sha256.update(bloque)
  return sha256.hexdigest()


ruta_original = "datos/telemetria_envios.csv"
ruta_limpia = "datos/telemetria_limpia.csv"

# 1. Auditoría inicial
hash_original = calcular_sha256(ruta_original)
print("=== AUDITORÍA CRIPTOGRÁFICA INICIAL ===")
print(f"SHA-256 Original: {hash_original}\n")

# 2. Carga del dataset y normalización de columnas
df = pd.read_csv(ruta_original)
df.columns = df.columns.str.strip()  # Elimina posibles espacios en cabeceras
registros_iniciales = len(df)
print(f"Total registros iniciales: {registros_iniciales}")
print(f"Columnas detectadas: {list(df.columns)}")

# 3. Limpieza de anomalías
# A. Eliminar duplicados en el identificador de envío
col_id = "id_envio" if "id_envio" in df.columns else df.columns[0]
df_limpio = df.drop_duplicates(subset=[col_id]).copy()

# B. Eliminar nulos en campos obligatorios
cols_nulos = [
    c for c in ["temp_celsius", "peso_kg", "destino"] if c in df_limpio.columns
]
df_limpio = df_limpio.dropna(subset=cols_nulos)

# C. Filtros de coherencia física
if "peso_kg" in df_limpio.columns:
  df_limpio = df_limpio[df_limpio["peso_kg"] > 0]

if "temp_celsius" in df_limpio.columns:
  df_limpio = df_limpio[
      (df_limpio["temp_celsius"] >= -30.0) & (df_limpio["temp_celsius"] <= 50.0)
  ]

# D. Filtro de estado no tipado (detecta 'estado_entrega' o 'estado')
col_estado = "estado_entrega" if "estado_entrega" in df_limpio.columns else None
if not col_estado and "estado" in df_limpio.columns:
  col_estado = "estado"

if col_estado:
  df_limpio = df_limpio[df_limpio[col_estado] != "DESCONOCIDO_ERROR"]

registros_finales = len(df_limpio)
print(f"\nTotal registros tras saneamiento: {registros_finales}")
print(f"Registros eliminados: {registros_iniciales - registros_finales}")

# 4. Guardar archivo depurado
df_limpio.to_csv(ruta_limpia, index=False)
print(f"Archivo depurado generado en: {ruta_limpia}")

# 5. Auditoría post-saneamiento
hash_limpio = calcular_sha256(ruta_limpia)
print("\n=== AUDITORÍA CRIPTOGRÁFICA TRAS SANEAMIENTO ===")
print(f"SHA-256 Limpio:   {hash_limpio}")