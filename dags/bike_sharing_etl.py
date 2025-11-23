"""
ETL Pipeline para Capital Bikeshare Dataset
Procesa datos de uso de bicicletas compartidas en Washington D.C. (2011-2012)

Dataset: hour.csv (17,379 registros horarios)
Objetivo: Analizar patrones de uso para optimizar distribución de bicicletas
          y promover movilidad sostenible urbana
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
from pathlib import Path

# Configuración de paths
BASE_DIR = Path('/opt/airflow')
RAW_DATA_PATH = BASE_DIR / 'data' / 'raw' / 'hour.csv'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'

# Configuración de logging
logger = logging.getLogger(__name__)


# ============================================================================
# TAREA 1: EXTRACT AND VALIDATE
# ============================================================================

def extract_and_validate(**context):
    """
    Extrae datos del CSV y valida estructura básica
    - Lee hour.csv completo (17K registros)
    - Valida 17 columnas esperadas
    - Verifica rango de fechas
    - Logea estadísticas básicas
    """
    try:
        logger.info(f"Leyendo dataset desde {RAW_DATA_PATH}")

        # Leer CSV completo (no chunking para 17K registros)
        df = pd.read_csv(RAW_DATA_PATH)

        # Validar estructura
        expected_columns = [
            'instant', 'dteday', 'season', 'yr', 'mnth', 'hr',
            'holiday', 'weekday', 'workingday', 'weathersit',
            'temp', 'atemp', 'hum', 'windspeed',
            'casual', 'registered', 'cnt'
        ]

        if list(df.columns) != expected_columns:
            raise ValueError(f"Columnas no coinciden. Esperadas: {expected_columns}, Encontradas: {list(df.columns)}")

        # Convertir dteday a datetime
        df['dteday'] = pd.to_datetime(df['dteday'])

        # Validar rango de fechas
        min_date = df['dteday'].min()
        max_date = df['dteday'].max()

        if not (min_date.year >= 2011 and max_date.year <= 2012):
            raise ValueError(f"Rango de fechas fuera de lo esperado: {min_date} - {max_date}")

        # Estadísticas básicas
        logger.info(f"✓ Dataset cargado exitosamente")
        logger.info(f"  Registros: {len(df):,}")
        logger.info(f"  Columnas: {len(df.columns)}")
        logger.info(f"  Rango de fechas: {min_date.date()} a {max_date.date()}")
        logger.info(f"  Total de rentas: {df['cnt'].sum():,}")
        logger.info(f"  Promedio de rentas/hora: {df['cnt'].mean():.2f}")

        # Guardar en XCom para siguiente tarea
        context['task_instance'].xcom_push(key='validated_data', value=df.to_json(date_format='iso'))

        return {
            'status': 'success',
            'records': len(df),
            'date_range': f"{min_date.date()} to {max_date.date()}"
        }

    except Exception as e:
        logger.error(f"Error en extract_and_validate: {str(e)}")
        raise


# ============================================================================
# TAREA 2: TRANSFORM - CLEAN AND DENORMALIZE
# ============================================================================

def transform_clean_and_denormalize(**context):
    """
    Limpia datos y desnormaliza variables climáticas
    - Elimina duplicados
    - Verifica/corrige tipos de datos
    - Desnormaliza: temp, atemp, humidity, windspeed
    """
    try:
        # Recuperar datos de tarea anterior
        df_json = context['task_instance'].xcom_pull(key='validated_data', task_ids='extract_and_validate')
        df = pd.read_json(df_json)
        df['dteday'] = pd.to_datetime(df['dteday'])

        logger.info("Iniciando limpieza y desnormalización")

        # Eliminar duplicados (por fecha + hora)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['dteday', 'hr'], keep='first')
        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.warning(f"  Duplicados eliminados: {duplicates_removed}")
        else:
            logger.info("  ✓ No se encontraron duplicados")

        # Verificar tipos de datos
        df['season'] = df['season'].astype(int)
        df['yr'] = df['yr'].astype(int)
        df['mnth'] = df['mnth'].astype(int)
        df['hr'] = df['hr'].astype(int)
        df['holiday'] = df['holiday'].astype(int)
        df['weekday'] = df['weekday'].astype(int)
        df['workingday'] = df['workingday'].astype(int)
        df['weathersit'] = df['weathersit'].astype(int)

        # DESNORMALIZACIÓN
        # Variables originales están normalizadas (0-1)
        logger.info("Desnormalizando variables climáticas:")

        # Temperatura (normalizada: t/t_max, t_max=39°C)
        # Fórmula inversa: temp_real = temp * 41 (aproximado)
        df['temp_celsius'] = df['temp'] * 41
        logger.info(f"  ✓ Temperatura: {df['temp_celsius'].min():.1f}°C - {df['temp_celsius'].max():.1f}°C")

        # Sensación térmica (normalizada: t/t_max, t_max=50°C)
        df['atemp_celsius'] = df['atemp'] * 50
        logger.info(f"  ✓ Sensación térmica: {df['atemp_celsius'].min():.1f}°C - {df['atemp_celsius'].max():.1f}°C")

        # Humedad (normalizada: hum/100)
        df['humidity_pct'] = df['hum'] * 100
        logger.info(f"  ✓ Humedad: {df['humidity_pct'].min():.1f}% - {df['humidity_pct'].max():.1f}%")

        # Velocidad del viento (normalizada: speed/67)
        df['windspeed_kmh'] = df['windspeed'] * 67
        logger.info(f"  ✓ Viento: {df['windspeed_kmh'].min():.1f} - {df['windspeed_kmh'].max():.1f} km/h")

        logger.info(f"✓ Limpieza y desnormalización completada: {len(df):,} registros")

        # Guardar en XCom
        context['task_instance'].xcom_push(key='cleaned_data', value=df.to_json(date_format='iso'))

        return {
            'status': 'success',
            'records': len(df),
            'duplicates_removed': duplicates_removed
        }

    except Exception as e:
        logger.error(f"Error en transform_clean_and_denormalize: {str(e)}")
        raise


# ============================================================================
# TAREA 3: TRANSFORM - FEATURE ENGINEERING
# ============================================================================

def transform_feature_engineering(**context):
    """
    Crea nuevas features temporales y categorías descriptivas
    - is_peak_hour: Horas pico (7-9am, 5-7pm)
    - is_weekend: Fin de semana
    - season_name: Nombre de estación
    - weather_desc: Descripción del clima
    - day_name: Nombre del día
    """
    try:
        # Recuperar datos de tarea anterior
        df_json = context['task_instance'].xcom_pull(key='cleaned_data', task_ids='transform_clean_and_denormalize')
        df = pd.read_json(df_json)
        df['dteday'] = pd.to_datetime(df['dteday'])

        logger.info("Iniciando feature engineering")

        # 1. HORA PICO (7-9am y 5-7pm)
        df['is_peak_hour'] = df['hr'].apply(
            lambda x: (7 <= x <= 9) or (17 <= x <= 19)
        )
        peak_hours_count = df['is_peak_hour'].sum()
        logger.info(f"  ✓ is_peak_hour: {peak_hours_count:,} registros en hora pico")

        # 2. FIN DE SEMANA (weekday: 0=domingo, 6=sábado)
        df['is_weekend'] = df['weekday'].apply(lambda x: x in [0, 6])
        weekend_count = df['is_weekend'].sum()
        logger.info(f"  ✓ is_weekend: {weekend_count:,} registros de fin de semana")

        # 3. NOMBRE DE ESTACIÓN
        season_map = {
            1: 'Spring',
            2: 'Summer',
            3: 'Fall',
            4: 'Winter'
        }
        df['season_name'] = df['season'].map(season_map)
        logger.info(f"  ✓ season_name: {df['season_name'].value_counts().to_dict()}")

        # 4. DESCRIPCIÓN DEL CLIMA
        weather_map = {
            1: 'Clear/Partly Cloudy',
            2: 'Mist/Cloudy',
            3: 'Light Rain/Snow',
            4: 'Heavy Rain/Snow'
        }
        df['weather_desc'] = df['weathersit'].map(weather_map)
        logger.info(f"  ✓ weather_desc: {df['weather_desc'].value_counts().to_dict()}")

        # 5. NOMBRE DEL DÍA
        day_map = {
            0: 'Sunday',
            1: 'Monday',
            2: 'Tuesday',
            3: 'Wednesday',
            4: 'Thursday',
            5: 'Friday',
            6: 'Saturday'
        }
        df['day_name'] = df['weekday'].map(day_map)
        logger.info(f"  ✓ day_name creado")

        # 6. MES NOMBRE
        month_map = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        df['month_name'] = df['mnth'].map(month_map)

        logger.info(f"✓ Feature engineering completado: {len(df.columns)} columnas totales")

        # Guardar en XCom
        context['task_instance'].xcom_push(key='featured_data', value=df.to_json(date_format='iso'))

        return {
            'status': 'success',
            'records': len(df),
            'total_columns': len(df.columns)
        }

    except Exception as e:
        logger.error(f"Error en transform_feature_engineering: {str(e)}")
        raise


# ============================================================================
# TAREA 4: TRANSFORM - AGGREGATIONS
# ============================================================================

def transform_aggregations(**context):
    """
    Crea agregaciones a nivel diario y semanal
    - Agregación DIARIA: totales y promedios por día
    - Agregación SEMANAL: totales y promedios por semana
    """
    try:
        # Recuperar datos de tarea anterior
        df_json = context['task_instance'].xcom_pull(key='featured_data', task_ids='transform_feature_engineering')
        df = pd.read_json(df_json)
        df['dteday'] = pd.to_datetime(df['dteday'])

        logger.info("Iniciando agregaciones")

        # ========== AGREGACIÓN DIARIA ==========
        daily_agg = df.groupby('dteday').agg({
            'cnt': 'sum',               # Total de rentas
            'casual': 'sum',            # Usuarios casuales
            'registered': 'sum',        # Usuarios registrados
            'temp_celsius': 'mean',     # Temperatura promedio
            'humidity_pct': 'mean',     # Humedad promedio
            'windspeed_kmh': 'mean',    # Viento promedio
            'weathersit': lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0],  # Clima más frecuente
            'is_peak_hour': 'sum',      # Horas pico en el día
            'is_weekend': 'max'         # Si es fin de semana
        }).reset_index()

        daily_agg.columns = [
            'date', 'total_rentals', 'casual_users', 'registered_users',
            'avg_temp_celsius', 'avg_humidity_pct', 'avg_windspeed_kmh',
            'most_common_weather', 'peak_hours_count', 'is_weekend'
        ]

        logger.info(f"  ✓ Agregación diaria: {len(daily_agg)} días")
        logger.info(f"    Total rentas diarias: {daily_agg['total_rentals'].sum():,}")
        logger.info(f"    Promedio rentas/día: {daily_agg['total_rentals'].mean():.0f}")

        # ========== AGREGACIÓN SEMANAL ==========
        df['year'] = df['dteday'].dt.year
        df['week'] = df['dteday'].dt.isocalendar().week

        weekly_agg = df.groupby(['year', 'week']).agg({
            'cnt': 'sum',
            'casual': 'sum',
            'registered': 'sum',
            'temp_celsius': 'mean',
            'humidity_pct': 'mean',
            'windspeed_kmh': 'mean',
            'dteday': ['min', 'max']    # Primera y última fecha de la semana
        }).reset_index()

        # Aplanar multi-index de columnas
        weekly_agg.columns = [
            'year', 'week', 'total_rentals', 'casual_users', 'registered_users',
            'avg_temp_celsius', 'avg_humidity_pct', 'avg_windspeed_kmh',
            'week_start', 'week_end'
        ]

        logger.info(f"  ✓ Agregación semanal: {len(weekly_agg)} semanas")
        logger.info(f"    Total rentas semanales: {weekly_agg['total_rentals'].sum():,}")
        logger.info(f"    Promedio rentas/semana: {weekly_agg['total_rentals'].mean():.0f}")

        # Guardar en XCom
        context['task_instance'].xcom_push(key='final_data', value=df.to_json(date_format='iso'))
        context['task_instance'].xcom_push(key='daily_agg', value=daily_agg.to_json(date_format='iso'))
        context['task_instance'].xcom_push(key='weekly_agg', value=weekly_agg.to_json(date_format='iso'))

        return {
            'status': 'success',
            'daily_records': len(daily_agg),
            'weekly_records': len(weekly_agg)
        }

    except Exception as e:
        logger.error(f"Error en transform_aggregations: {str(e)}")
        raise


# ============================================================================
# TAREA 5: LOAD TO PARQUET
# ============================================================================

def load_to_parquet(**context):
    """
    Guarda todos los datasets transformados en formato Parquet
    - Dataset principal transformado
    - Agregación diaria
    - Agregación semanal
    Formato: Parquet con compresión snappy (eficiente)
    """
    try:
        # Crear directorio si no existe
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"Guardando datasets en {PROCESSED_DIR}")

        # Recuperar datos de tarea anterior
        df_json = context['task_instance'].xcom_pull(key='final_data', task_ids='transform_aggregations')
        daily_json = context['task_instance'].xcom_pull(key='daily_agg', task_ids='transform_aggregations')
        weekly_json = context['task_instance'].xcom_pull(key='weekly_agg', task_ids='transform_aggregations')

        # Convertir de JSON a DataFrame
        df = pd.read_json(df_json)
        daily_df = pd.read_json(daily_json)
        weekly_df = pd.read_json(weekly_json)

        # Convertir fechas
        df['dteday'] = pd.to_datetime(df['dteday'])
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        weekly_df['week_start'] = pd.to_datetime(weekly_df['week_start'])
        weekly_df['week_end'] = pd.to_datetime(weekly_df['week_end'])

        # ========== GUARDAR DATASET PRINCIPAL ==========
        full_path = PROCESSED_DIR / 'bike_sharing_full.parquet'
        df.to_parquet(full_path, engine='pyarrow', compression='snappy', index=False)

        full_size = full_path.stat().st_size / 1024  # KB
        logger.info(f"  ✓ Dataset completo: {full_path.name}")
        logger.info(f"    Registros: {len(df):,}")
        logger.info(f"    Columnas: {len(df.columns)}")
        logger.info(f"    Tamaño: {full_size:.2f} KB")

        # ========== GUARDAR AGREGACIÓN DIARIA ==========
        daily_path = PROCESSED_DIR / 'daily_summary.parquet'
        daily_df.to_parquet(daily_path, engine='pyarrow', compression='snappy', index=False)

        daily_size = daily_path.stat().st_size / 1024
        logger.info(f"  ✓ Agregación diaria: {daily_path.name}")
        logger.info(f"    Registros: {len(daily_df):,}")
        logger.info(f"    Tamaño: {daily_size:.2f} KB")

        # ========== GUARDAR AGREGACIÓN SEMANAL ==========
        weekly_path = PROCESSED_DIR / 'weekly_summary.parquet'
        weekly_df.to_parquet(weekly_path, engine='pyarrow', compression='snappy', index=False)

        weekly_size = weekly_path.stat().st_size / 1024
        logger.info(f"  ✓ Agregación semanal: {weekly_path.name}")
        logger.info(f"    Registros: {len(weekly_df):,}")
        logger.info(f"    Tamaño: {weekly_size:.2f} KB")

        total_size = full_size + daily_size + weekly_size
        logger.info(f"✓ Carga completada - Tamaño total: {total_size:.2f} KB")

        return {
            'status': 'success',
            'files_created': 3,
            'total_size_kb': total_size,
            'full_records': len(df),
            'daily_records': len(daily_df),
            'weekly_records': len(weekly_df)
        }

    except Exception as e:
        logger.error(f"Error en load_to_parquet: {str(e)}")
        raise


# ============================================================================
# DEFINICIÓN DEL DAG
# ============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bike_sharing_etl',
    default_args=default_args,
    description='ETL pipeline para Capital Bikeshare - análisis de movilidad sostenible',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 21),
    catchup=False,
    tags=['etl', 'bike-sharing', 'sustainability'],
) as dag:

    # Tarea 1: Extract and Validate
    task_extract = PythonOperator(
        task_id='extract_and_validate',
        python_callable=extract_and_validate,
        provide_context=True,
    )

    # Tarea 2: Transform - Clean and Denormalize
    task_clean = PythonOperator(
        task_id='transform_clean_and_denormalize',
        python_callable=transform_clean_and_denormalize,
        provide_context=True,
    )

    # Tarea 3: Transform - Feature Engineering
    task_features = PythonOperator(
        task_id='transform_feature_engineering',
        python_callable=transform_feature_engineering,
        provide_context=True,
    )

    # Tarea 4: Transform - Aggregations
    task_aggregate = PythonOperator(
        task_id='transform_aggregations',
        python_callable=transform_aggregations,
        provide_context=True,
    )

    # Tarea 5: Load to Parquet
    task_load = PythonOperator(
        task_id='load_to_parquet',
        python_callable=load_to_parquet,
        provide_context=True,
    )

    # Dependencias: Flujo lineal
    task_extract >> task_clean >> task_features >> task_aggregate >> task_load
