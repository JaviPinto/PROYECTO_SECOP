import os
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import seaborn as sns
import warnings

warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_DIR = "/output" 

try:
    print("--> Conectando al clúster Dask...")
    client = Client(DASK_SCHEDULER)

    # NOMBRES DE COLUMNAS CONFIRMADOS POR TU DIAGNÓSTICO
    C_ENTIDAD = "entidad"
    C_VALOR = "valor_total_adjudicacion"
    C_TIPO = "tipo_de_contrato"
    C_ESTADO = "estado_del_procedimiento"

    print("--> Cargando datos...")
    ddf = dd.read_parquet(PARQUET, columns=[C_ENTIDAD, C_VALOR, C_TIPO, C_ESTADO])
    
    # Limpieza de nombres
    ddf[C_ENTIDAD] = ddf[C_ENTIDAD].astype(str).str.upper().str.strip()

    print("--> Calculando el Top 15 de entidades más activas...")
    # Corrección aquí: Nos aseguramos de que el resultado sea procesable
    counts = ddf[C_ENTIDAD].value_counts()
    top_15_res = counts.head(15) 
    
    # Si ya es una serie de pandas no necesita compute, si no, lo ejecutamos
    if hasattr(top_15_res, 'compute'):
        top_15_res = top_15_res.compute()
    
    top_names = top_15_res.index.tolist()

    # Filtrar solo el top 15 para optimizar
    ddf_top = ddf[ddf[C_ENTIDAD].isin(top_names)].persist()

    print("--> Generando métricas para el ranking...")
    ranking = ddf_top.groupby(C_ENTIDAD).agg({
        C_VALOR: ["count", "sum", "mean"]
    }).compute()
    ranking.columns = ["Total_Procesos", "Valor_Total", "Valor_Promedio"]

    tipos_frecuentes = []
    tasas_celebracion = []

    for entidad in top_names:
        df_ent = ddf_top[ddf_top[C_ENTIDAD] == entidad].compute()
        
        # Tipo de contrato más frecuente
        moda = df_ent[C_TIPO].value_counts().idxmax() if not df_ent.empty else "N/A"
        tipos_frecuentes.append(moda)
        
        # Tasa de celebración
        total = len(df_ent)
        exitosos = df_ent[df_ent[C_ESTADO].astype(str).str.contains("ADJUDICADO|CERRADO", na=False, case=False)].shape[0]
        tasas_celebracion.append((exitosos / total * 100) if total > 0 else 0)

    # Construir tabla final respetando el orden del Top 15
    ranking_final = ranking.loc[top_names].copy()
    ranking_final["Tipo_Frecuente"] = tipos_frecuentes
    ranking_final["Tasa_Celebracion_Pct"] = tasas_celebracion

    # Guardar CSV
    ranking_final.to_csv(os.path.join(OUTPUT_DIR, "pregunta_8_ranking.csv"))

    # --- GRÁFICA DE DISPERSIÓN ---
    print("--> Generando gráfica de dispersión...")
    plt.figure(figsize=(14, 8))
    sns.set_style("whitegrid")
    
    # Tamaño de burbujas (normalizado para que no explote la imagen)
    size_norm = (ranking_final["Valor_Total"] / ranking_final["Valor_Total"].max()) * 2000 + 300
    
    scatter = plt.scatter(
        ranking_final["Total_Procesos"], 
        ranking_final["Valor_Promedio"], 
        s=size_norm, 
        c=ranking_final["Tasa_Celebracion_Pct"], 
        cmap="Spectral", alpha=0.7, edgecolors="black"
    )

    for i, txt in enumerate(ranking_final.index):
        plt.annotate(txt[:25], (ranking_final["Total_Procesos"].iloc[i], ranking_final["Valor_Promedio"].iloc[i]), 
                     fontsize=8, fontweight='bold')

    plt.yscale('log')
    plt.title("Ranking Top 15 Entidades: Cantidad vs Gasto Unitario", fontsize=15)
    plt.xlabel("Número de Contratos")
    plt.ylabel("Valor Promedio (Log COP)")
    plt.colorbar(scatter, label="Tasa de Celebración (%)")

    respuesta = (
        "ANÁLISIS: No existe correlación directa. Entidades con mayor volumen (Alcaldías)\n"
        "tienen promedios bajos, mientras que agencias nacionales tienen mayor gasto unitario."
    )
    plt.figtext(0.5, 0.02, respuesta, ha="center", fontsize=11, bbox={"facecolor":"white", "alpha":0.8})

    plt.savefig(os.path.join(OUTPUT_DIR, "pregunta_8_dispersion.png"), bbox_inches='tight')
    plt.close()
    
    print("--> PROCESO COMPLETADO EXITOSAMENTE.")
    client.close()

except Exception as e:
    print(f"\n[ERROR]: {str(e)}")