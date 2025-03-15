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

# EA2

## Proceso de Limpieza de Datos y Auditoría

Realizamos una limpieza exhaustiva del archivo `ingestion.csv` para preparar los datos para análisis. El proceso incluyó varios pasos clave:

### Etapas del proceso

**Nota**
Antes de realizar la manipuilación de los datos fue necesario llenar el archivo fuente con datos con mala calidad por que originalmente el archivo contaba solo con un registro, el csv se cargó con 50 filas.

1. **Carga inicial de datos**: Importamos `ingestion.csv` y documentamos sus características básicas (número de filas/columnas, tipos de datos y valores no nulos por columna).

2. **Tratamiento de valores vacíos**: Convertimos todos los campos vacíos a `NaN` para facilitar su procesamiento posterior.

3. **Normalización de datos numéricos**: Transformamos las columnas numéricas (`high`, `low`, `vol`, `last`, `buy`, `sell`, `open`, `date`) a su formato correcto, sustituyendo con `NaN` aquellos valores que no pudieron convertirse.

4. **Eliminación de outliers**: Removimos registros con volumen (`vol`) superior a 1000 y precios negativos (`last`), ya que distorsionaban el análisis.

5. **Imputación de valores faltantes**: Rellenamos los valores `NaN` restantes con la mediana de cada columna, manteniendo así la integridad del dataset sin perder registros.

6. **Estandarización de fechas**: Convertimos la columna de fechas de timestamp Unix a formato legible, marcando como `NaT` aquellas fechas inconsistentes.

7. **Documentación del proceso**: Generamos un reporte detallado (`cleaning_report.txt`) que incluye:
   - Resumen con fecha y hora del proceso
   - Estadísticas del dataset antes y después de la limpieza
   - Análisis de cambios en valores nulos
   - Acciones realizadas en cada etapa

8. **Exportación de resultados**: Guardamos el dataset procesado como `ingestion_cleaned.csv`.

