import pandas as pd
import numpy as np
import os

# Definir la ruta del archivo CSV
file_path = "src/static/csv/ingestion.csv"
file_path_clean = "src/static/csv/cleaned_data.csv"


# ############### Llenar el CSV con datos para hacer la limpieza de datos ################

def generar_datos_de_prueba():
    # Crear datos adicionales con valores nulos e inconsistencias
    nuevos_datos = pd.DataFrame({
        "high": np.random.uniform(500000, 550000, 50),  
        "low": np.random.uniform(480000, 520000, 50),
        "vol": np.random.uniform(10, 100, 50), 
        "last": np.random.uniform(490000, 530000, 50),
        "buy": np.random.uniform(490000, 530000, 50),
        "sell": np.random.uniform(490000, 530000, 50),
        "open": np.random.uniform(490000, 530000, 50),
        "date": np.random.randint(1741220000, 1741229999, 50),  
        "pair": ["BRLBTC"] * 50  
    })

    # Introducir valores nulos en algunas filas
    nuevos_datos.loc[5, "high"] = np.nan
    nuevos_datos.loc[10, "vol"] = np.nan
    nuevos_datos.loc[15, "low"] = np.nan
    nuevos_datos.loc[20, "pair"] = None  
    nuevos_datos.loc[25, "date"] = ""  

    # Agregar valores extremos o inconsistentes
    nuevos_datos.loc[30, "vol"] = 5000
    nuevos_datos.loc[35, "last"] = -10000
    nuevos_datos.loc[40, "sell"] = "error"

    return nuevos_datos


def cargar_datos_csv():
    # Intentar leer el CSV existente, si existe
    if os.path.exists(file_path):
        df_existente = pd.read_csv(file_path)
        return df_existente
    else:
        print(f"El archivo {file_path} no existe.")
        return pd.DataFrame()


def guardar_csv(df, path):
    # Guardar los datos en el archivo CSV
    df.to_csv(path, index=False)
    print(f"Archivo guardado en: {path}")


def limpiar_datos(df):
    # 1Reemplazar valores vacíos o nulos en cada columna
    df.replace("", np.nan, inplace=True)  
    df.dropna(subset=["pair"], inplace=True)  

    # 2️Eliminar filas con errores en datos numéricos
    # Convertir a numérico, forzando errores a NaN
    cols_numericas = ["high", "low", "vol", "last", "buy", "sell", "open", "date"]
    for col in cols_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce") 

    # 3️Manejar valores extremos
    df = df[df["vol"] < 1000]  
    df = df[df["last"] >= 0]  

    # 4️Rellenar valores nulos con la media (o mediana si hay sesgo)
    df.fillna({
        "high": df["high"].median(),
        "low": df["low"].median(),
        "vol": df["vol"].median(),
        "last": df["last"].median(),
        "buy": df["buy"].median(),
        "sell": df["sell"].median(),
        "open": df["open"].median(),
        "date": df["date"].median()
    }, inplace=True)

    # 5️Corregir formato de fechas
    df["date"] = pd.to_datetime(df["date"], unit="s", errors="coerce")

    return df


def main():
    # Generar datos de prueba con valores nulos e inconsistentes
    nuevos_datos = generar_datos_de_prueba()

    # Cargar los datos existentes
    df_existente = cargar_datos_csv()

    # Concatenar los datos nuevos con los existentes (si ya hay datos)
    if not df_existente.empty:
        df_final = pd.concat([df_existente, nuevos_datos], ignore_index=True)
    else:
        df_final = nuevos_datos  # Si no existen datos, usar solo los nuevos

    # Guardar el CSV con los datos combinados
    guardar_csv(df_final, file_path)

    # Limpiar los datos
    df_limpio = limpiar_datos(df_final)

    # Guardar el archivo limpio
    guardar_csv(df_limpio, file_path_clean)
    print("Datos limpios guardados con éxito.")


if __name__ == "__main__":
    main()
