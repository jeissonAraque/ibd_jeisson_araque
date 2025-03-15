import sqlite3
import requests
import json
import pandas as pd

class Ingestion:
    def __init__(self):
        self.ruta_static = "src/static/"

    def obtener_datos_api(self,url="",params={}):
        url = "{}/{}/{}/".format(url,params["coin"],params["method"])
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            print(error)
            return {}
    
    def guardar_datos(self,datos={},nombre_archivo="ingestion"):
        with open("{}db/{}.json".format(self.ruta_static,nombre_archivo), "w") as archivo:
            json.dump(datos,archivo)
        
    def guardar_db(self, datos={}, nombre_archivo="ingestion"):
        """ Guarda los datos en una base de datos SQLite """
        db_path = f"{self.ruta_static}db/{nombre_archivo}.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Asegurar que 'datos' contiene datos válidos
        if not isinstance(datos, dict) or "ticker" not in datos:
            print("Formato de datos no válido para almacenamiento en SQLite")
            return
        
        ticker = datos["ticker"]  # Extraer la información relevante
        if not isinstance(ticker, dict):
            print("Los datos del ticker no están en el formato esperado")
            return

        # Crear tabla si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mercado_bitcoin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                high REAL,
                low REAL,
                vol REAL,
                last REAL,
                buy REAL,
                sell REAL,
                date INTEGER
            )
        """)

        # Insertar datos
        cursor.execute("""
            INSERT INTO mercado_bitcoin (high, low, vol, last, buy, sell, date) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker.get("high"),
            ticker.get("low"),
            ticker.get("vol"),
            ticker.get("last"),
            ticker.get("buy"),
            ticker.get("sell"),
            ticker.get("date")
        ))

        conn.commit()
        conn.close()
        print("Datos guardados en SQLite")

    def validar_auditoria(self, datos, nombre_archivo="ingestion"):
        """
        Valida la cantidad de registros y columnas de la variable 'datos' 
        y del archivo JSON generado por guardar_datos.
        Se asume que 'datos' es una lista de diccionarios o un diccionario con 
        una clave 'results' que contiene esa lista.
        """
        # Validación de la variable 'datos'
        registros_datos = 0
        columnas_datos = 0
        
        if isinstance(datos, list) and len(datos) > 0:
            registros_datos = len(datos)
            if isinstance(datos[0], dict):
                columnas_datos = len(datos[0])
            else:
                print("El primer registro de 'datos' no es un diccionario.")
        elif isinstance(datos, dict):
            if "results" in datos and isinstance(datos["results"], list) and len(datos["results"]) > 0:
                registros_datos = len(datos["results"])
                if isinstance(datos["results"][0], dict):
                    columnas_datos = len(datos["results"][0])
            else:
                registros_datos = len(datos)
                columnas_datos = len(datos)
        else:
            print("Formato de 'datos' no reconocido.")
        
        # Lectura y validación del archivo JSON
        ruta_archivo = "{}db/{}.json".format(self.ruta_static, nombre_archivo)
        try:
            with open(ruta_archivo, "r") as archivo:
                datos_archivo = json.load(archivo)
        except Exception as e:
            print("Error al leer el archivo JSON:", e)
            return False
        
        registros_archivo = 0
        columnas_archivo = 0
        
        if isinstance(datos_archivo, list) and len(datos_archivo) > 0:
            registros_archivo = len(datos_archivo)
            if isinstance(datos_archivo[0], dict):
                columnas_archivo = len(datos_archivo[0])
            else:
                print("El primer registro del archivo no es un diccionario.")
        elif isinstance(datos_archivo, dict):
            if "results" in datos_archivo and isinstance(datos_archivo["results"], list) and len(datos_archivo["results"]) > 0:
                registros_archivo = len(datos_archivo["results"])
                if isinstance(datos_archivo["results"][0], dict):
                    columnas_archivo = len(datos_archivo["results"][0])
            else:
                registros_archivo = len(datos_archivo)
                columnas_archivo = len(datos_archivo)
        else:
            print("Formato de datos en el archivo no reconocido.")
        
        # Verificar existencia de los archivos generados
        db_path = f"{self.ruta_static}db/{nombre_archivo}.db"
        csv_path = f"{self.ruta_static}csv/{nombre_archivo}.csv"
        auditoria_txt_path = f"{self.ruta_static}auditoria/{nombre_archivo}.txt"
        
        def archivo_existe(ruta):
            try:
                with open(ruta, 'r'):
                    return "Archivo creado correctamente"
            except FileNotFoundError:
                return "Archivo no encontrado"

        auditoria_result = {
            "registros_datos": registros_datos,
            "columnas_datos": columnas_datos,
            "registros_archivo": registros_archivo,
            "columnas_archivo": columnas_archivo,
            "archivo_db_existente": archivo_existe(db_path),
            "archivo_csv_existente": archivo_existe(csv_path)
        }
        
        # Guardar auditoría en archivo .txt
        with open(auditoria_txt_path, "w") as auditoria_txt:
            auditoria_txt.write(json.dumps(auditoria_result, indent=4))
        
        print("Resultado de auditoría guardado en:", auditoria_txt_path)

        print("Variable 'datos': {} registros, {} columnas".format(registros_datos, columnas_datos))
        print("Archivo JSON: {} registros, {} columnas".format(registros_archivo, columnas_archivo))
        print("Auditoría: ")
        print(f"Archivo DB: {auditoria_result['archivo_db_existente']}")
        print(f"Archivo csv: {auditoria_result['archivo_csv_existente']}")
        
        return (registros_datos == registros_archivo) and (columnas_datos == columnas_archivo)
    
    def guardar_csv(self, datos={}, nombre_archivo="ingestion"):
        """ Guarda los datos en un archivo csv """
        
        ruta_csv = f"{self.ruta_static}csv/{nombre_archivo}.csv"
        
        if not isinstance(datos, dict) or "ticker" not in datos:
            print("Formato de datos no válido para almacenamiento en csv")
            return
        
        ticker = datos["ticker"]
        if not isinstance(ticker, dict):
            print("Los datos del ticker no están en el formato esperado")
            return

        df = pd.DataFrame([ticker])  
        
        df.to_csv(ruta_csv, index=False)

        print(f"Datos guardados en csv en la ruta: {ruta_csv}")
        
        
    
ingestion = Ingestion()
parametros = {"coin":"BTC","method":"ticker"}
url = "https://www.mercadobitcoin.net/api"
datos = ingestion.obtener_datos_api(url=url, params=parametros)
if len(datos)>0:
    print(json.dumps(datos,indent=4))
else:
    print("no se obtubo la consulta")
ingestion.guardar_datos(datos=datos,nombre_archivo="ingestion")
ingestion.guardar_db(datos=datos,nombre_archivo="ingestion")
ingestion.guardar_csv(datos=datos,nombre_archivo="ingestion")
ingestion.validar_auditoria(datos=datos,nombre_archivo="ingestion")
