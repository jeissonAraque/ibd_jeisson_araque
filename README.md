# ibd_jeisson_araque

# Ingestion de Datos API y Auditoría

## Descripción

Este proyecto implementa una solución para la obtención, almacenamiento y validación de datos desde una API de criptomonedas (MercadoBitcoin) en varios formatos: JSON, base de datos SQLite y archivo Excel. Además, se realiza un proceso de auditoría sobre los datos obtenidos, validando la cantidad de registros y columnas y verificando la existencia de los archivos generados. Los resultados de la auditoría se guardan en un archivo de texto.

## Trazabilidad del Proceso

1. **Obtención de datos desde la API**: 
   - Se realiza una solicitud a la API de MercadoBitcoin con parámetros específicos (moneda y método de consulta) para obtener información sobre el mercado de Bitcoin.
   
2. **Guardado de datos en diferentes formatos**:
   - **JSON**: Los datos obtenidos son guardados en un archivo JSON dentro del directorio `static/db/`.
   - **SQLite**: Los datos también se guardan en una base de datos SQLite dentro del directorio `static/db/`. La tabla creada se llama `mercado_bitcoin`.
   - **Excel**: Los datos se guardan en un archivo Excel en el directorio `static/xlsx/`.

3. **Validación de datos**: 
   - La función `validar_auditoria` valida la cantidad de registros y columnas de los datos obtenidos y de los archivos generados, comparando los resultados y asegurando la consistencia de los datos.
   
4. **Generación de archivo de auditoría**:
   - Los resultados de la auditoría (número de registros y columnas, existencia de archivos) se guardan en un archivo de texto `.txt` dentro del directorio `static/auditoria/`.


### Clonar el repositorio

Para clonar el repositorio, ejecuta el siguiente comando en tu terminal:

```bash
git clone https://github.com/jeissonAraque/ibd_jeisson_araque.git
```

