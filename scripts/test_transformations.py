#!/usr/bin/env python3
"""
Script de prueba para validar transformaciones del pipeline ETL
Útil para testing rápido sin levantar Airflow completo

Uso:
    python scripts/test_transformations.py
"""

import pandas as pd
import sys
from pathlib import Path

# Ajustar paths para ejecución local
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_PATH = BASE_DIR / 'data' / 'raw' / 'hour.csv'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'

print("="*70)
print("TEST: Pipeline ETL - Bike Sharing")
print("="*70)

# ============================================================================
# TEST 1: EXTRACT AND VALIDATE
# ============================================================================

print("\n[1/5] Testing Extract and Validate...")

try:
    df = pd.read_csv(RAW_DATA_PATH)

    expected_columns = [
        'instant', 'dteday', 'season', 'yr', 'mnth', 'hr',
        'holiday', 'weekday', 'workingday', 'weathersit',
        'temp', 'atemp', 'hum', 'windspeed',
        'casual', 'registered', 'cnt'
    ]

    assert list(df.columns) == expected_columns, "Columns mismatch!"

    df['dteday'] = pd.to_datetime(df['dteday'])

    print(f"  ✓ Registros cargados: {len(df):,}")
    print(f"  ✓ Rango de fechas: {df['dteday'].min().date()} a {df['dteday'].max().date()}")
    print(f"  ✓ Total de rentas: {df['cnt'].sum():,}")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    sys.exit(1)

# ============================================================================
# TEST 2: CLEAN AND DENORMALIZE
# ============================================================================

print("\n[2/5] Testing Clean and Denormalize...")

try:
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates(subset=['dteday', 'hr'], keep='first')
    duplicates = initial_count - len(df)

    if duplicates > 0:
        print(f"  ⚠ Duplicados eliminados: {duplicates}")
    else:
        print(f"  ✓ Sin duplicados encontrados")

    # Desnormalización
    df['temp_celsius'] = df['temp'] * 41
    df['atemp_celsius'] = df['atemp'] * 50
    df['humidity_pct'] = df['hum'] * 100
    df['windspeed_kmh'] = df['windspeed'] * 67

    print(f"  ✓ Temperatura: {df['temp_celsius'].min():.1f}°C - {df['temp_celsius'].max():.1f}°C")
    print(f"  ✓ Humedad: {df['humidity_pct'].min():.1f}% - {df['humidity_pct'].max():.1f}%")
    print(f"  ✓ Viento: {df['windspeed_kmh'].min():.1f} - {df['windspeed_kmh'].max():.1f} km/h")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    sys.exit(1)

# ============================================================================
# TEST 3: FEATURE ENGINEERING
# ============================================================================

print("\n[3/5] Testing Feature Engineering...")

try:
    # Crear features
    df['is_peak_hour'] = df['hr'].apply(lambda x: (7 <= x <= 9) or (17 <= x <= 19))
    df['is_weekend'] = df['weekday'].apply(lambda x: x in [0, 6])

    season_map = {1: 'Spring', 2: 'Summer', 3: 'Fall', 4: 'Winter'}
    df['season_name'] = df['season'].map(season_map)

    weather_map = {
        1: 'Clear/Partly Cloudy',
        2: 'Mist/Cloudy',
        3: 'Light Rain/Snow',
        4: 'Heavy Rain/Snow'
    }
    df['weather_desc'] = df['weathersit'].map(weather_map)

    day_map = {0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday',
               4: 'Thursday', 5: 'Friday', 6: 'Saturday'}
    df['day_name'] = df['weekday'].map(day_map)

    month_map = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    df['month_name'] = df['mnth'].map(month_map)

    peak_count = df['is_peak_hour'].sum()
    weekend_count = df['is_weekend'].sum()

    print(f"  ✓ Registros en hora pico: {peak_count:,} ({peak_count/len(df)*100:.1f}%)")
    print(f"  ✓ Registros de fin de semana: {weekend_count:,} ({weekend_count/len(df)*100:.1f}%)")
    print(f"  ✓ Total de columnas: {len(df.columns)}")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    sys.exit(1)

# ============================================================================
# TEST 4: AGGREGATIONS
# ============================================================================

print("\n[4/5] Testing Aggregations...")

try:
    # Agregación diaria
    daily_agg = df.groupby('dteday').agg({
        'cnt': 'sum',
        'casual': 'sum',
        'registered': 'sum',
        'temp_celsius': 'mean',
        'humidity_pct': 'mean',
        'windspeed_kmh': 'mean',
        'weathersit': lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0],
        'is_peak_hour': 'sum',
        'is_weekend': 'max'
    }).reset_index()

    print(f"  ✓ Agregación diaria: {len(daily_agg)} días")
    print(f"    Promedio rentas/día: {daily_agg['cnt'].mean():.0f}")

    # Agregación semanal
    df['year'] = df['dteday'].dt.year
    df['week'] = df['dteday'].dt.isocalendar().week

    weekly_agg = df.groupby(['year', 'week']).agg({
        'cnt': 'sum',
        'casual': 'sum',
        'registered': 'sum',
        'temp_celsius': 'mean',
        'humidity_pct': 'mean',
        'windspeed_kmh': 'mean',
        'dteday': ['min', 'max']
    }).reset_index()

    print(f"  ✓ Agregación semanal: {len(weekly_agg)} semanas")
    print(f"    Promedio rentas/semana: {weekly_agg['cnt'].mean():.0f}")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    sys.exit(1)

# ============================================================================
# TEST 5: SAVE TO PARQUET
# ============================================================================

print("\n[5/5] Testing Save to Parquet...")

try:
    # Crear directorio si no existe
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Guardar dataset principal
    full_path = PROCESSED_DIR / 'bike_sharing_full.parquet'
    df.to_parquet(full_path, engine='pyarrow', compression='snappy', index=False)
    full_size = full_path.stat().st_size / 1024

    print(f"  ✓ Dataset completo guardado: {full_path.name} ({full_size:.2f} KB)")

    # Guardar agregaciones
    daily_path = PROCESSED_DIR / 'daily_summary.parquet'
    daily_agg.to_parquet(daily_path, engine='pyarrow', compression='snappy', index=False)
    daily_size = daily_path.stat().st_size / 1024

    print(f"  ✓ Agregación diaria guardada: {daily_path.name} ({daily_size:.2f} KB)")

    weekly_path = PROCESSED_DIR / 'weekly_summary.parquet'
    weekly_agg.to_parquet(weekly_path, engine='pyarrow', compression='snappy', index=False)
    weekly_size = weekly_path.stat().st_size / 1024

    print(f"  ✓ Agregación semanal guardada: {weekly_path.name} ({weekly_size:.2f} KB)")

    total_size = full_size + daily_size + weekly_size
    print(f"\n  Total size: {total_size:.2f} KB")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    sys.exit(1)

# ============================================================================
# RESUMEN FINAL
# ============================================================================

print("\n" + "="*70)
print("✓ TODAS LAS PRUEBAS PASARON EXITOSAMENTE")
print("="*70)

print(f"""
Resumen:
  - Registros procesados: {len(df):,}
  - Columnas finales: {len(df.columns)}
  - Agregación diaria: {len(daily_agg)} registros
  - Agregación semanal: {len(weekly_agg)} registros
  - Archivos generados: 3
  - Tamaño total: {total_size:.2f} KB

Siguiente paso:
  → Levantar Airflow: docker-compose up -d
  → Acceder a: http://localhost:8080
  → Activar DAG: bike_sharing_etl
""")

print("\nColumnas del dataset transformado:")
print(f"  {', '.join(df.columns[:10])}")
print(f"  {', '.join(df.columns[10:20])}")
print(f"  {', '.join(df.columns[20:])}")

print("\n✓ Script completado exitosamente!")
