#!/usr/bin/env python3
"""
Script para explorar los datos transformados del pipeline ETL
Ejecutar: python explore_data.py
"""

import pandas as pd
from pathlib import Path

# Configurar pandas para mostrar más información
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

print("="*80)
print("EXPLORACIÓN DE DATOS - Pipeline ETL Bike Sharing")
print("="*80)

# ============================================================================
# 1. DATASET COMPLETO TRANSFORMADO
# ============================================================================

print("\n" + "="*80)
print("1. DATASET COMPLETO TRANSFORMADO")
print("="*80)

df_full = pd.read_parquet('data/processed/bike_sharing_full.parquet')

print(f"\nDimensiones: {df_full.shape[0]:,} registros × {df_full.shape[1]} columnas")
print(f"\nRango de fechas: {df_full['dteday'].min().date()} a {df_full['dteday'].max().date()}")
print(f"Total de rentas: {df_full['cnt'].sum():,}")

print("\n📋 Columnas disponibles:")
for i, col in enumerate(df_full.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n📊 Primeras 5 filas:")
print(df_full.head())

print("\n📈 Estadísticas descriptivas (variables climáticas):")
print(df_full[['temp_celsius', 'atemp_celsius', 'humidity_pct', 'windspeed_kmh', 'cnt']].describe())

print("\n🌤️ Distribución por clima:")
print(df_full['weather_desc'].value_counts())

print("\n🗓️ Distribución por día de la semana:")
print(df_full.groupby('day_name')['cnt'].mean().sort_values(ascending=False))

print("\n⏰ Top 5 horas con más rentas:")
print(df_full.groupby('hr')['cnt'].sum().sort_values(ascending=False).head())

# ============================================================================
# 2. AGREGACIÓN DIARIA
# ============================================================================

print("\n" + "="*80)
print("2. AGREGACIÓN DIARIA")
print("="*80)

df_daily = pd.read_parquet('data/processed/daily_summary.parquet')

print(f"\nDimensiones: {df_daily.shape[0]:,} días × {df_daily.shape[1]} columnas")

print("\n📋 Columnas:")
for i, col in enumerate(df_daily.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n📊 Primeras 10 filas:")
print(df_daily.head(10))

print("\n📈 Estadísticas de rentas diarias:")
print(df_daily['total_rentals'].describe())

print(f"\n🏆 Día con más rentas: {df_daily.loc[df_daily['total_rentals'].idxmax(), 'date'].date()}")
print(f"   Total: {df_daily['total_rentals'].max():,} rentas")

print(f"\n📉 Día con menos rentas: {df_daily.loc[df_daily['total_rentals'].idxmin(), 'date'].date()}")
print(f"   Total: {df_daily['total_rentals'].min():,} rentas")

# ============================================================================
# 3. AGREGACIÓN SEMANAL
# ============================================================================

print("\n" + "="*80)
print("3. AGREGACIÓN SEMANAL")
print("="*80)

df_weekly = pd.read_parquet('data/processed/weekly_summary.parquet')

print(f"\nDimensiones: {df_weekly.shape[0]:,} semanas × {df_weekly.shape[1]} columnas")

print("\n📋 Columnas:")
for i, col in enumerate(df_weekly.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n📊 Primeras 10 semanas:")
print(df_weekly.head(10))

print("\n📈 Estadísticas de rentas semanales:")
print(df_weekly['total_rentals'].describe())

print(f"\n🏆 Semana con más rentas:")
best_week = df_weekly.loc[df_weekly['total_rentals'].idxmax()]
print(f"   Año {best_week['year']}, Semana {best_week['week']}")
print(f"   {best_week['week_start'].date()} a {best_week['week_end'].date()}")
print(f"   Total: {best_week['total_rentals']:,} rentas")

# ============================================================================
# 4. INSIGHTS CLAVE
# ============================================================================

print("\n" + "="*80)
print("4. 🔍 INSIGHTS CLAVE DEL ANÁLISIS")
print("="*80)

# Comparación usuarios casuales vs registrados
casual_pct = (df_full['casual'].sum() / df_full['cnt'].sum()) * 100
registered_pct = (df_full['registered'].sum() / df_full['cnt'].sum()) * 100

print(f"\n👥 Distribución de usuarios:")
print(f"   Casuales: {casual_pct:.1f}% ({df_full['casual'].sum():,} rentas)")
print(f"   Registrados: {registered_pct:.1f}% ({df_full['registered'].sum():,} rentas)")

# Rentas en hora pico vs no pico
peak_rentals = df_full[df_full['is_peak_hour'] == True]['cnt'].sum()
non_peak_rentals = df_full[df_full['is_peak_hour'] == False]['cnt'].sum()
peak_pct = (peak_rentals / df_full['cnt'].sum()) * 100

print(f"\n⏰ Horas pico (7-9am, 5-7pm):")
print(f"   {peak_pct:.1f}% de las rentas ({peak_rentals:,})")

# Fin de semana vs días laborales
weekend_avg = df_full[df_full['is_weekend'] == True].groupby('dteday')['cnt'].sum().mean()
weekday_avg = df_full[df_full['is_weekend'] == False].groupby('dteday')['cnt'].sum().mean()

print(f"\n📅 Promedio de rentas diarias:")
print(f"   Fin de semana: {weekend_avg:.0f} rentas/día")
print(f"   Días laborales: {weekday_avg:.0f} rentas/día")
print(f"   Diferencia: {((weekday_avg/weekend_avg - 1) * 100):+.1f}%")

# Por estación
print(f"\n🌸 Rentas por estación del año:")
seasonal = df_full.groupby('season_name')['cnt'].sum().sort_values(ascending=False)
for season, total in seasonal.items():
    print(f"   {season:10s}: {total:>8,} rentas ({(total/df_full['cnt'].sum()*100):>5.1f}%)")

print("\n" + "="*80)
print("✅ EXPLORACIÓN COMPLETADA")
print("="*80)

print("""
💡 Próximos pasos sugeridos:

1. Crear visualizaciones:
   - Gráfico de línea: Rentas por día/semana
   - Heatmap: Rentas por hora del día y día de la semana
   - Box plot: Distribución de rentas por clima

2. Análisis adicionales:
   - Correlación entre variables climáticas y rentas
   - Impacto de festivos en demanda
   - Tendencias de crecimiento año a año

3. Dashboard interactivo:
   - Streamlit, Dash, o Jupyter notebook
   - Filtros por fecha, clima, estación
   - Métricas KPI en tiempo real

Archivos disponibles:
  📁 data/processed/bike_sharing_full.parquet
  📁 data/processed/daily_summary.parquet
  📁 data/processed/weekly_summary.parquet
""")
