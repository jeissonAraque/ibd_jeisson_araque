from time import sleep
import pandas as pd
import os

def enrich_data():
    
    df_clean = pd.read_csv("src/static/csv/cleaned_data.csv")
    df_new_data = pd.read_excel("src/market_data.xlsx")

    def parse_date(df, date_column):
        df[date_column] = pd.to_datetime(df[date_column])
        df["fecha"] = df[date_column].dt.date
        return df


    parse_date(df_clean, "date")
    parse_date(df_new_data, "fecha")


    df_merged = pd.merge(df_clean, df_new_data, on="fecha", how="left")

    df_merged.to_csv("src/static/enriched_data/enriched_data.csv", index=False)

    print("Archivo guardado en src/static/enriched_data/enriched_data.csv")
    print("Data enriquecida.")
    

def auditoria_enrich():
    # Definir las rutas de los archivos
    enriched_path = "src/static/enriched_data/enriched_data.csv"
    cleaned_path = "src/static/csv/cleaned_data.csv"
    audit_file_path = "src/static/auditoria/auditoria_enrich.txt"
    
    # Iniciar auditoría en el archivo de texto
    with open(audit_file_path, 'w') as f:
        # Verificar si el archivo enriched_data.csv existe en la ruta indicada
        if not os.path.exists(enriched_path):
            f.write("ERROR: El archivo enriched_data.csv no existe en la ruta src/static/enriched_data.\n")
            print(f"ERROR: El archivo enriched_data.csv no existe en la ruta {enriched_path}. Auditoría no realizada.")
            return
        else:
            f.write("El archivo enriched_data.csv se encuentra en la ruta src/static/enriched_data.\n")
        
        df_enriched = pd.read_csv(enriched_path)
        df_clean = pd.read_csv(cleaned_path)
        
        num_columns_enriched = df_enriched.shape[1]
        num_columns_clean = df_clean.shape[1]
        f.write(f"Auditoría de datos:\n\n")
        f.write(f"Archivo enriched_data.csv tiene {num_columns_enriched} columnas.\n")
        f.write(f"Archivo cleaned_data.csv tiene {num_columns_clean} columnas.\n")
        
        new_columns = set(df_enriched.columns) - set(df_clean.columns)
        if new_columns:
            f.write(f"\nNuevas columnas agregadas en enriched_data.csv:\n")
            for col in new_columns:
                f.write(f"- {col}\n")
        else:
            f.write(f"\nNo hay nuevas columnas en enriched_data.csv.\n")
        
        missing_values = df_enriched.isnull().sum()
        missing_values = missing_values[missing_values > 0]
        if not missing_values.empty:
            f.write(f"\nValores faltantes en enriched_data.csv:\n")
            for col, count in missing_values.items():
                f.write(f"- {col}: {count} valores faltantes\n")
        else:
            f.write(f"\nNo hay valores faltantes en enriched_data.csv.\n")
        
        duplicates = df_enriched.duplicated().sum()
        f.write(f"\nNúmero de filas duplicadas en enriched_data.csv: {duplicates}\n")
        
    print(f"Auditoría completada. El resultado se guardó en {audit_file_path}")


print("******* Iniciando *********")
enrich_data()
print("******* Realizando auditoria *********")
sleep(3)  # Esperar 3 segundos para asegurar que el archivo se haya guardado
auditoria_enrich()    
print("******* Finalizado *********")