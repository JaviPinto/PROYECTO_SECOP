import os
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import seaborn as sns
import warnings
import numpy as np

warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_DIR = "/output" 

try:
    print("--> Conectando al clúster Dask...")
    client = Client(DASK_SCHEDULER)

    # Nombres de columnas según tu diagnóstico previo
    C_ENTIDAD = "entidad"
    C_VALOR = "valor_total_adjudicacion"
    C_TIPO = "tipo_de_contrato"
    C_DEP = "departamento_entidad"
    C_ESTADO = "estado_del_procedimiento"

    print("--> Cargando datos...")
    ddf = dd.read_parquet(PARQUET, columns=[C_ENTIDAD, C_VALOR, C_TIPO, C_DEP, C_ESTADO])
    
    # Limpieza rápida
    ddf[C_TIPO] = ddf[C_TIPO].fillna("OTRO").astype(str)

    # 1. CALCULAR MEDIA Y DESVIACIÓN POR TIPO (USANDO DASK)
    print("--> Calculando estadísticas (Media y Sigma) por tipo de contrato...")
    stats = ddf.groupby(C_TIPO)[C_VALOR].agg(['mean', 'std']).compute()
    
    # 2. DEFINIR FUNCIÓN DE FILTRADO PARA MAP_PARTITIONS
    def filtrar_outliers(df_partition, stats_dict):
        # Función interna para aplicar por fila o vectorizada
        def es_outlier(row):
            tipo = row[C_TIPO]
            if tipo in stats_dict:
                m = stats_dict[tipo]['mean']
                s = stats_dict[tipo]['std']
                limite = m + (3 * s)
                if row[C_VALOR] > limite:
                    # Calculamos cuántas sigmas está por encima
                    sigmas = (row[C_VALOR] - m) / s if s > 0 else 0
                    return sigmas
            return np.nan

        df_partition['sigmas_sobre_media'] = df_partition.apply(es_outlier, axis=1)
        return df_partition[df_partition['sigmas_sobre_media'].notnull()]

    # Convertir stats a diccionario para acceso rápido en los workers
    stats_dict = stats.to_dict('index')

    print("--> Aplicando filtro de outliers con map_partitions...")
    # Aplicamos la función a cada partición
    outliers_ddf = ddf.map_partitions(filtrar_outliers, stats_dict=stats_dict)
    
    # Traemos los outliers a memoria (suelen ser pocos comparado con el total)
    df_outliers = outliers_ddf.compute()

    # 3. EXPORTAR TABLA DE OUTLIERS (Top 20 más extremos para el reporte)
    df_reporte = df_outliers.sort_values(by='sigmas_sobre_media', ascending=False).head(50)
    df_reporte.to_csv(os.path.join(OUTPUT_DIR, "pregunta_9_tabla_outliers.csv"), index=False)

    # 4. GRÁFICO DE BARRAS POR DEPARTAMENTO
    print("--> Generando gráfico de concentración por departamento...")
    plt.figure(figsize=(12, 8))
    counts_dep = df_outliers[C_DEP].value_counts().head(15)
    
    sns.barplot(x=counts_dep.values, y=counts_dep.index, palette="magma")
    plt.title("Departamentos con Mayor Concentración de Outliers Económicos", fontsize=14)
    plt.xlabel("Número de Contratos Atípicos")
    plt.ylabel("Departamento")

    # RESPUESTA INTEGRADA
    respuesta = (
        "ANÁLISIS: Los outliers suelen ser contratos legítimos de infraestructura o defensa,\n"
        "pero se distinguen de errores (ej. digitación) por la coherencia entre el objeto\n"
        "del contrato y el valor. Si el 'sigmas_sobre_media' es > 50, es probable error de datos."
    )
    plt.figtext(0.5, 0.01, respuesta, ha="center", fontsize=10, 
                bbox={"facecolor":"white", "alpha":0.8, "edgecolor":"red", "pad":5})

    plt.savefig(os.path.join(OUTPUT_DIR, "pregunta_9_concentracion.png"), bbox_inches='tight')
    plt.close()

    print(f"--> PROCESO COMPLETADO. Se encontraron {len(df_outliers)} outliers.")
    client.close()

except Exception as e:
    print(f"\n[ERROR]: {str(e)}")