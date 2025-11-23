# Pipeline ETL - Capital Bikeshare Dataset

Pipeline ETL construido con Apache Airflow para analizar patrones de uso de bicicletas compartidas en Washington D.C. (2011-2012).

## 📋 Justificación del Proyecto

Los sistemas de bicicletas compartidas representan una alternativa de movilidad sostenible que reduce la congestión vehicular, las emisiones de CO2 y promueve la salud pública en áreas urbanas. El análisis de patrones de uso de Capital Bikeshare en Washington D.C., considerando variables meteorológicas y temporales, permite optimizar la distribución de bicicletas en estaciones, predecir demanda en horas pico y mejorar la planificación de mantenimiento preventivo. Los beneficiarios directos incluyen autoridades de transporte urbano que pueden asignar recursos eficientemente, ciudadanos que acceden a transporte limpio y confiable, y municipios que buscan reducir la huella de carbono mediante políticas de movilidad verde.

## 🏗️ Arquitectura del Pipeline

### Estructura del Proyecto

```
PedrozoETL/
├── dags/
│   └── bike_sharing_etl.py         # DAG principal (5 tareas)
├── data/
│   ├── raw/
│   │   └── hour.csv                # Dataset original (17,379 registros)
│   └── processed/                  # Outputs en formato Parquet
│       ├── bike_sharing_full.parquet
│       ├── daily_summary.parquet
│       └── weekly_summary.parquet
├── logs/                           # Logs de Airflow
├── plugins/                        # Custom operators (opcional)
├── docker-compose.yml              # Configuración de servicios
├── .env                            # Variables de entorno
├── requirements.txt                # Dependencias Python
└── README.md
```

### Flujo del DAG (5 Tareas)

```
extract_and_validate
        ↓
transform_clean_and_denormalize
        ↓
transform_feature_engineering
        ↓
transform_aggregations
        ↓
load_to_parquet
```

## 🔧 Tareas del Pipeline

### 1️⃣ Extract and Validate
- Lee `hour.csv` completo (17,379 registros)
- Valida 17 columnas esperadas
- Verifica rango de fechas (2011-2012)
- Logea estadísticas básicas

### 2️⃣ Transform - Clean and Denormalize
- Elimina duplicados (si existen)
- Verifica y corrige tipos de datos
- **Desnormaliza variables climáticas:**
  - `temp_celsius = temp × 41°C`
  - `atemp_celsius = atemp × 50°C`
  - `humidity_pct = hum × 100%`
  - `windspeed_kmh = windspeed × 67 km/h`

### 3️⃣ Transform - Feature Engineering
Crea nuevas features:
- `is_peak_hour`: Horas pico (7-9am, 5-7pm)
- `is_weekend`: Fin de semana (sábado/domingo)
- `season_name`: Primavera/Verano/Otoño/Invierno
- `weather_desc`: Descripción del clima
- `day_name`: Lunes-Domingo
- `month_name`: Enero-Diciembre

### 4️⃣ Transform - Aggregations
Crea dos niveles de agregación:

**Agregación Diaria:**
- Total de rentas, usuarios casuales/registrados
- Promedios de temperatura, humedad, viento
- Clima más frecuente, horas pico del día

**Agregación Semanal:**
- Totales semanales de rentas
- Promedios de métricas climáticas
- Rango de fechas de la semana

### 5️⃣ Load to Parquet
- Guarda dataset transformado completo
- Guarda agregaciones diaria y semanal
- **Formato Parquet con compresión snappy** (eficiente)

## ✅ Cumplimiento de Requisitos

| Requisito | Implementación |
|-----------|----------------|
| **Extract** | ✅ Lectura de CSV con validación completa |
| **Transform** | ✅ Limpieza + Desnormalización + Feature Engineering + Agregaciones |
| **Load** | ✅ Parquet con compresión snappy |
| **Scheduling** | ✅ `@daily` a las 00:00 |
| **Error Handling** | ✅ try/except en cada tarea + retries=2 |
| **Scaling** | ✅ **Formato Parquet eficiente** (columnar, comprimido) |
| **Failure Notifications** | ✅ Logging detallado por tarea |

---

## 🚀 Pasos de Activación

### Prerequisitos

- Docker y Docker Compose instalados
- Al menos 4GB de RAM disponible
- Puerto 8080 libre

### Paso 1: Configurar Variables de Entorno

Edita el archivo `.env` si necesitas cambiar el UID (opcional):

```bash
# Para Linux/WSL, obtén tu UID:
echo $(id -u)

# Luego edita .env y actualiza AIRFLOW_UID si es diferente a 50000
```

### Paso 2: Instalar Dependencias en Airflow

Edita `.env` para agregar las dependencias:

```bash
_PIP_ADDITIONAL_REQUIREMENTS=pandas>=2.1.0 pyarrow>=14.0.0 numpy>=1.24.0
```

**O** copia el archivo `requirements.txt` al contenedor después de iniciar (ver paso 4).

### Paso 3: Levantar Airflow con Docker Compose

```bash
# Iniciar todos los servicios en background
docker-compose up -d

# Ver logs en tiempo real (opcional)
docker-compose logs -f
```

**Servicios que se levantan:**
- PostgreSQL (metadata database)
- Airflow Webserver (UI en puerto 8080)
- Airflow Scheduler (ejecutor de DAGs)

**Tiempo de inicio:** ~2-3 minutos la primera vez

### Paso 4: Instalar Dependencias Python (si no usaste .env)

```bash
# Ejecutar dentro del contenedor webserver
docker-compose exec airflow-webserver pip install pandas pyarrow numpy
```

### Paso 5: Acceder a Airflow Web UI

1. Abre tu navegador en: **http://localhost:8080**

2. **Credenciales de acceso:**
   - **Usuario:** `airflow`
   - **Contraseña:** `airflow`

3. Deberías ver el DAG `bike_sharing_etl` en la lista

### Paso 6: Activar y Ejecutar el DAG

#### Opción A: Ejecución Manual (Recomendado para prueba)

1. En la UI, busca el DAG **`bike_sharing_etl`**
2. Activa el toggle (switch ON) en la columna izquierda
3. Click en el nombre del DAG para ver detalles
4. Click en **"Trigger DAG"** (botón de play ▶️ arriba a la derecha)
5. Confirma la ejecución

#### Opción B: Esperar Ejecución Programada

- El DAG está programado para ejecutarse **diariamente a las 00:00**
- Si activas el toggle, esperará hasta la siguiente medianoche

### Paso 7: Monitorear la Ejecución

#### En la UI de Airflow:

1. **Graph View:** Ver el flujo de tareas y su estado
   - 🟢 Verde: Completado exitosamente
   - 🔵 Azul: En ejecución
   - 🔴 Rojo: Falló
   - ⚪ Gris: No ejecutado aún

2. **Logs de cada tarea:**
   - Click en una tarea (cuadro en el grafo)
   - Click en "Log"
   - Ver output detallado de cada función

3. **Grid View:** Ver histórico de ejecuciones

#### Desde la terminal:

```bash
# Ver logs del scheduler
docker-compose logs -f airflow-scheduler

# Ver logs del webserver
docker-compose logs -f airflow-webserver
```

### Paso 8: Verificar Outputs Generados

```bash
# Listar archivos Parquet generados
ls -lh data/processed/

# Deberías ver:
# bike_sharing_full.parquet    (~100-200 KB)
# daily_summary.parquet         (~10-20 KB)
# weekly_summary.parquet        (~5-10 KB)
```

#### Leer los archivos Parquet (Python):

```python
import pandas as pd

# Dataset completo transformado
df_full = pd.read_parquet('data/processed/bike_sharing_full.parquet')
print(f"Registros: {len(df_full):,}")
print(f"Columnas: {list(df_full.columns)}")

# Agregación diaria
df_daily = pd.read_parquet('data/processed/daily_summary.parquet')
print(df_daily.head())

# Agregación semanal
df_weekly = pd.read_parquet('data/processed/weekly_summary.parquet')
print(df_weekly.head())
```

---

## 🛠️ Comandos Útiles

### Gestión de Docker Compose

```bash
# Ver estado de servicios
docker-compose ps

# Detener todos los servicios
docker-compose down

# Detener y eliminar volúmenes (CUIDADO: borra datos)
docker-compose down -v

# Reiniciar un servicio específico
docker-compose restart airflow-scheduler

# Ver logs de un servicio
docker-compose logs -f airflow-webserver
```

### Gestión de Airflow

```bash
# Listar DAGs
docker-compose exec airflow-webserver airflow dags list

# Probar una tarea específica (sin ejecutar el DAG completo)
docker-compose exec airflow-webserver airflow tasks test bike_sharing_etl extract_and_validate 2025-11-23

# Ver información del DAG
docker-compose exec airflow-webserver airflow dags show bike_sharing_etl

# Pausar/Despausar DAG
docker-compose exec airflow-webserver airflow dags pause bike_sharing_etl
docker-compose exec airflow-webserver airflow dags unpause bike_sharing_etl
```

### Debugging

```bash
# Ejecutar bash dentro del contenedor
docker-compose exec airflow-webserver bash

# Ver variables de entorno
docker-compose exec airflow-webserver env | grep AIRFLOW

# Ver conexiones configuradas
docker-compose exec airflow-webserver airflow connections list
```

---

## 🐛 Solución de Problemas

### Error: "Port 8080 already in use"

```bash
# Ver qué proceso usa el puerto 8080
sudo lsof -i :8080

# Cambiar el puerto en docker-compose.yml:
# ports:
#   - "8081:8080"  # Usa 8081 en tu máquina
```

### Error: "Permission denied" en logs o data

```bash
# Ajustar permisos
sudo chown -R $USER:$USER logs/ data/

# O en .env, cambiar AIRFLOW_UID a tu UID
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### Error: "ModuleNotFoundError: No module named 'pandas'"

```bash
# Instalar dependencias manualmente
docker-compose exec airflow-webserver pip install pandas pyarrow numpy

# O reiniciar con .env actualizado (ver Paso 2)
docker-compose down
docker-compose up -d
```

### DAG no aparece en la UI

1. Verificar que el archivo está en `dags/bike_sharing_etl.py`
2. Verificar logs del scheduler:
   ```bash
   docker-compose logs airflow-scheduler | grep bike_sharing
   ```
3. Refrescar la UI (botón circular arriba a la derecha)
4. Esperar 1-2 minutos para que Airflow detecte cambios

### Tarea falla constantemente

1. Ver logs de la tarea en la UI (click en tarea → Log)
2. Verificar que `hour.csv` existe en `data/raw/`
3. Verificar permisos de escritura en `data/processed/`

---

## 📊 Dashboard

### Acceso al Dashboard

**Opción 1: Archivo Power BI**
- Ubicación: `dashboard/Capital_Bikeshare_Dashboard.pbix`
- Instrucciones:
  1. Descargar Power BI Desktop (gratuito)
  2. Abrir el archivo .pbix
  3. Los datos están embebidos, no requiere conexión adicional

**Opción 2: Screenshots**
Ver capturas en `dashboard/screenshots/`

### Visualizaciones Incluidas

1. **KPIs:**
   - Total de Rentas: 3,292,679
   - Temperatura Promedio: 20.4°C

2. **Gráficos:**
   - Rentas por Hora del Día (columnas)
   - Distribución por Estación (donut)
   - Fin de Semana vs Entre Semana (barras)

### Insights Clave

- **Horas pico:** 8am y 5-6pm (horarios laborales)
- **Temporada alta:** Otoño (32%) y Verano (28%)
- **Uso laboral:** Días entre semana tienen 2x más rentas que fines de semana

## 🎯 Justificación del Proyecto

Los sistemas de bicicletas compartidas representan una alternativa de movilidad sostenible que reduce la congestión vehicular, las emisiones de CO2 y promueve la salud pública en áreas urbanas. El análisis de patrones de uso de Capital Bikeshare en Washington D.C., considerando variables meteorológicas y temporales, permite optimizar la gestión de la flota identificando horas pico de demanda y mejorar la planificación de mantenimiento preventivo en temporadas de baja demanda. Los beneficiarios directos incluyen autoridades de transporte urbano que pueden dimensionar recursos según patrones diarios y estacionales, ciudadanos que acceden a transporte limpio y confiable, y municipios que buscan reducir la huella de carbono mediante políticas de movilidad verde. Este dataset facilita decisiones basadas en datos para promover sistemas de transporte más sostenibles y accesibles.
---

## 📚 Información del Dataset

- **Fuente:** Capital Bikeshare, Washington D.C.
- **Período:** 2011-2012
- **Registros:** 17,379 (granularidad horaria)
- **Variables:** 17 columnas (clima, tiempo, conteo de rentas)
- **URL Original:** https://www.kaggle.com/datasets/lakshmi25npathi/bike-sharing-dataset

---

