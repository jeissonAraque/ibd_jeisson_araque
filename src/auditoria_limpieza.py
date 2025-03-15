import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Definir la ruta del archivo
base_path = Path(__file__).resolve().parent
input_file = base_path / "static/csv/ingestion.csv"
output_file = base_path / "static/csv/ingestion_cleaned.csv"
audit_file = base_path / "static/auditoria/cleaning_report.txt"

# Cargar los datos originales
df_original = pd.read_csv(input_file)

# Crear una copia para trabajar sin modificar el original
df = df_original.copy()

# Inicializar auditoría
audit_log = []
audit_log.append(f"Auditoría realizada el: {datetime.now()}\n")
audit_log.append(f"Archivo original: {input_file}\n")
audit_log.append(f"Total de filas antes de limpiar: {df.shape[0]}\n")
audit_log.append(f"Total de columnas: {df.shape[1]}\n\n")

# 1. Identificar valores nulos antes de limpiar
missing_before = df.isnull().sum()

# 2. Incluir df.info() antes de la limpieza
df.info()

# 3. Reemplazar valores vacíos con NaN
df.replace("", np.nan, inplace=True)

# 4. Convertir a numérico y registrar errores
cols_numericas = ["high", "low", "vol", "last", "buy", "sell", "open", "date"]
for col in cols_numericas:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# 5. Contar valores nulos después de la conversión
missing_after = df.isnull().sum()

# 6. Filtrar valores extremos y erróneos
df = df[df["vol"] < 1000]  # Filtrar volúmenes excesivamente altos
df = df[df["last"] >= 0]  # Eliminar valores negativos de precios

# 7. Rellenar valores nulos con la mediana
df.fillna(df.median(numeric_only=True), inplace=True)

# 8. Corregir formato de fechas
df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")

# 9. Guardar el DataFrame limpio
df.to_csv(output_file, index=False)

# 10. Incluir df.info() después de la limpieza
df.info()

# Comparar cambios y registrar en la auditoría
audit_log.append("Comparación de valores nulos antes y después:\n")
audit_log.append(missing_before.to_string() + "\n\n")
audit_log.append("Valores nulos después de la limpieza:\n")
audit_log.append(missing_after.to_string() + "\n\n")

audit_log.append(f"Total de filas después de limpiar: {df.shape[0]}\n")
audit_log.append(f"Archivo limpio guardado en: {output_file}\n")

# Guardar el archivo de auditoría
with open(audit_file, "w", encoding="utf-8") as f:
    f.writelines("\n".join(audit_log))

print(f"Auditoría guardada en: {audit_file}")
