import requests
import json

class Ingestion:
    def __init__(self):
        self.ruta_static = "src/ibgd/static/"

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
        
    def guardar_db(self,datos={},nombre_archivo="ingestion"):
        pass

    def validar_autoria(self, datos, nombre_archivo="ingestion"):
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
        
        print("Variable 'datos': {} registros, {} columnas".format(registros_datos, columnas_datos))
        print("Archivo JSON: {} registros, {} columnas".format(registros_archivo, columnas_archivo))
        
        return (registros_datos == registros_archivo) and (columnas_datos == columnas_archivo)
    
ingestion = Ingestion()
parametros = {"coin":"BTC","method":"ticker"}
url = "https://www.mercadobitcoin.net/api"
datos = ingestion.obtener_datos_api(url=url, params=parametros)
if len(datos)>0:
    print(json.dumps(datos,indent=4))
else:
    print("no se obtubo la consulta")
ingestion.guardar_datos(datos=datos,nombre_archivo="ingestion")
ingestion.validar_autoria(datos=datos,nombre_archivo="ingestion")
