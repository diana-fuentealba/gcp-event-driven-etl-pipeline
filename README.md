# Pipeline ETL Automatizado y Basado en Eventos (Serverless) en GCP

Este repositorio contiene un pipeline de datos **ETL (Extract, Transform, Load)** robusto diseñado bajo una arquitectura orientada a eventos (*Event-Driven*) en **Google Cloud Platform (GCP)**. El sistema procesa de forma automática los archivos de inventario cargados en la nube, los limpia utilizando **Python (Pandas)** y los disponibiliza en **Google BigQuery** para su explotación analítica local o mediante dashboards.

---

## 🏗️ Arquitectura del Sistema

El flujo de datos está diseñado bajo el paradigma *Serverless*, garantizando escalabilidad automática y costos vinculados estrictamente al consumo real.

```
[ Cloud Storage ] ---> ( Notificación de Objeto )
        │
        ▼
 [ Cloud Pub/Sub ] ---> ( tema-etl-productos )
        │
        ▼
  [ Eventarc ]    ---> ( Disparador de Evento )
        │
        ▼
[ Cloud Run Functions ] ──> [ Transformación Pandas ]
        │
        ▼
  [ BigQuery ]    <─── [ Carga en reporte_productos ]
        │
   (Llave JSON)
        ▼
[ VS Code / Local Notebook ] ──> [ Análisis de Datos ]
```

### Componentes de GCP Utilizados:
1. **Google Cloud Storage (GCS):** Repositorio inicial (`diana-datos-input-2026`) donde los archivos crudos son depositados.
2. **Cloud Pub/Sub:** Sistema de mensajería asíncrona desacoplado que actúa como el puente de notificaciones de la plataforma.
3. **Eventarc:** Enrutador de eventos encargado de interceptar el mensaje de Pub/Sub y despertar la lógica computacional.
4. **Cloud Run Functions (Python 3.14):** Entorno de cómputo serverless donde corre el script de procesamiento de datos.
5. **Google BigQuery:** Data Warehouse analítico donde reside la tabla final optimizada (`reporte_productos`).

---

## 🛠️ Stack Tecnológico

* **Lenguaje:** Python
* **Librerías Clave:**
  * `pandas` (Manipulación y estructuración de matrices de datos)
  * `google-cloud-bigquery` & `pandas-gbq` (Conectores oficiales de almacenamiento masivo)
  * `pyarrow` (Motor de serialización de alto rendimiento para BigQuery)
  * `gcsfs` & `fsspec` (Acceso eficiente a sistemas de archivos en la nube)
* **Entorno Local:** Visual Studio Code & Jupyter Notebooks para la explotación y testing analítico.

---

## ⚙️ Detalle del Pipeline ETL

### 1. Extracción (Extract)
El sistema reacciona en tiempo real ante eventos en el bucket. Al detectar la subida de un archivo con extensión válida (`.csv`), la función captura los metadatos del evento (nombre del bucket y del objeto) decodificando el mensaje encapsulado en Pub/Sub, abriendo el flujo remoto sin necesidad de descargar el archivo físicamente en disco local.

### 2. Transformación (Transform)
La lógica en Python implementa reglas estrictas de calidad y formateo de datos:
* **Estandarización del Esquema:** Mapeo explícito y ordenamiento de columnas fijas (`sku`, `producto`, `precio_unitario`, `stock`).
* **Sanitización Financiera (Data Cleaning):** Remoción dinámica de caracteres especiales (como el símbolo de moneda `$`) y espacios en blanco residuales mediante procesamiento de strings, permitiendo la conversión segura a enteros (`int64`).
* **Enriquecimiento de Datos:** Inyección automatizada de la fecha de procesamiento (`fecha_proceso`).

### 3. Carga (Load)
Los datos estructurados son insertados mediante el método `load_table_from_dataframe` de BigQuery, configurado en modalidad `WRITE_APPEND`. Este paso se ejecuta a través del motor `pyarrow`, garantizando inserciones masivas de baja latencia.

---

## 📊 Código del Pipeline (`main.py`)

```python
import functions_framework
import pandas as pd
from google.cloud import bigquery
import base64

@functions_framework.cloud_event
def procesar_archivo_etl(cloud_event):
    # 1. Captura y desencapsulamiento del evento Pub/Sub
    pubsub_message = cloud_event.data.get("message", {})
    if not pubsub_message or "attributes" not in pubsub_message:
        return

    atributos = pubsub_message.get("attributes", {})
    bucket_name = atributos.get("bucketId")
    file_name = atributos.get("objectId")
    
    if not file_name or not file_name.endswith('.csv'):
        return

    path_archivo = f"gs://{bucket_name}/{file_name}"
    
    try:
        # 2. Extracción y Limpieza Indestructible con Pandas
        df = pd.read_csv(path_archivo, sep='|')
        df.columns = ['sku', 'producto', 'precio_unitario', 'stock']
        
        # Limpieza de strings y casteo numérico seguro
        df['precio_unitario'] = df['precio_unitario'].astype(str).str.replace('$', '', regex=False).str.strip().astype(int)
        df['fecha_proceso'] = pd.to_datetime('today').date()
        df = df[['sku', 'producto', 'stock', 'precio_unitario', 'fecha_proceso']]

        # 3. Carga a BigQuery
        client = bigquery.Client()
        table_id = "mi-proyecto-etl-498923.dataset_etl.reporte_productos"
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() 

        print(f"Éxito: Se cargaron {len(df)} filas en BigQuery.")

    except Exception as e:
        print(f"ERROR CRÍTICO: {str(e)}")
```

---

## 📈 Conexión Local y Análisis de Negocio (VS Code)

Para interactuar con el Data Warehouse fuera del entorno cloud, se diseñó un flujo de autenticación seguro utilizando variables de entorno y Service Accounts de GCP (`credenciales.json`). Esto permite consumir las tablas limpias directamente desde un Jupyter Notebook local:

```python
import os
import pandas as pd
from google.cloud import bigquery

# Autenticación segura mediante Service Account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciales.json"
client = bigquery.Client()

# Consulta SQL optimizada para analítica (Control de duplicados por Max Fecha)
query = """
    SELECT 
        sku, producto, stock, precio_unitario,
        (stock * precio_unitario) AS valor_inventario,
        fecha_proceso
    FROM `mi-proyecto-etl-498923.dataset_etl.reporte_productos`
    WHERE fecha_proceso = (SELECT MAX(fecha_proceso) FROM `mi-proyecto-etl-498923.dataset_etl.reporte_productos`)
"""

df = client.query(query).to_dataframe()
print(df.describe())
```

---

## 🚀 Próximos Pasos de Ingeniería (Roadmap)

Para llevar este desarrollo a un nivel empresarial aún más alto, se contemplan las siguientes integraciones:
* **Lógica Upsert (MERGE):** Reemplazar la disposición `WRITE_APPEND` por consultas SQL orientadas a actualizar el stock o precio si el SKU ya existe, mitigando duplicados de cargas repetidas del mismo día.
* **Idempotencia en Origen:** Programar comandos finales en la función de Cloud Run para mover automáticamente los archivos completamente procesados a una carpeta histórica de almacenamiento (`gs://.../procesados/`).
* **Visualización:** Conectar la tabla final de BigQuery a **Looker Studio** para automatizar un cuadro de mando ejecutivo con actualización en tiempo real.
