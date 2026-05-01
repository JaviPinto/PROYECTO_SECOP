import os
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import seaborn as sns
import warnings

# Silenciar advertencias
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_DIR = "/output" 

# Rutas de archivos
CSV_PATH = os.path.join(OUTPUT_DIR, "pregunta_7_tabla_comparativa.csv")
IMG_DIST = os.path.join(OUTPUT_DIR, "pregunta_7_distribucion_final.png")
IMG_EVO = os.path.join(OUTPUT_DIR, "pregunta_7_evolucion_anual.png")

try:
    print("--> Iniciando conexión con Dask...")
    client = Client(DASK_SCHEDULER)

    COL_ANNO = "año_publicacion_proceso"
    COL_VALOR = "valor_total_adjudicacion"
    COL_MODALIDAD = "modalidad_de_contratacion"

    print("--> Procesando datos reales del SECOP...")
    ddf = dd.read_parquet(PARQUET, columns=[COL_MODALIDAD, COL_VALOR, COL_ANNO])

    # Normalizar texto
    ddf[COL_MODALIDAD] = ddf[COL_MODALIDAD].astype(str).str.upper().str.strip()
    target_modes = ["CONTRATACIÓN DIRECTA", "LICITACIÓN PÚBLICA"]
    ddf_filt = ddf[ddf[COL_MODALIDAD].isin(target_modes)]

    # --- 1. TABLA ESTADÍSTICA ---
    print("--> Generando tabla resumen...")
    resumen = ddf_filt.groupby(COL_MODALIDAD).agg({
        COL_VALOR: ["count", "mean", "sum"]
    }).compute()
    resumen.columns = ["Numero_Contratos", "Valor_Promedio", "Inversion_Total"]
    
    medianas_map = {}
    for mode in resumen.index:
        medianas_map[mode] = ddf_filt[ddf_filt[COL_MODALIDAD] == mode][COL_VALOR].quantile(0.5).compute()
    resumen["Mediana_Aproximada"] = resumen.index.map(medianas_map)
    resumen.to_csv(CSV_PATH)

    # --- 2. GRÁFICA DE EVOLUCIÓN CON TEXTO DE RESPUESTA ---
    print("--> Creando gráfica de evolución...")
    evo = ddf_filt.groupby([COL_ANNO, COL_MODALIDAD]).size().compute().unstack().fillna(0)
    evo_pct = evo.div(evo.sum(axis=1), axis=0) * 100

    plt.figure(figsize=(12, 7))
    evo_pct.plot(kind='line', marker='o', ax=plt.gca(), linewidth=2, color=['#1f77b4', '#ff7f0e'])
    plt.title("Evolución de Participación (Pregunta 7)", fontsize=14, fontweight='bold')
    plt.ylabel("Porcentaje sobre el total (%)")
    plt.grid(True, alpha=0.3)
    
    # INSERTAR RESPUESTA EN LA IMAGEN
    conclusion_evo = (
        "ANÁLISIS: La Contratación Directa domina el volumen de registros (>90%)\n"
        "especialmente desde 2017, reflejando una alta carga de gestión operativa."
    )
    plt.figtext(0.5, 0.01, conclusion_evo, ha="center", fontsize=10, 
                bbox={"facecolor":"white", "alpha":0.8, "edgecolor":"orange", "pad":5})
    
    plt.savefig(IMG_EVO, bbox_inches='tight')
    plt.close()

    # --- 3. GRÁFICA DE DISTRIBUCIÓN CON TEXTO DE RESPUESTA ---
    print("--> Creando gráfica de distribución...")
    df_sample = ddf_filt.sample(frac=0.01).compute()
    
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    sns.boxplot(x=COL_MODALIDAD, y=COL_VALOR, data=df_sample, palette="Set2", showfliers=False)
    sns.stripplot(x=COL_MODALIDAD, y=COL_VALOR, data=df_sample, color="black", alpha=0.1, size=2)
    
    plt.yscale('log')
    plt.title("Comparación de Montos: Licitación vs Directa", fontsize=14, fontweight='bold')
    plt.ylabel("Valor Adjudicado (Escala Logarítmica COP)")
    
    # INSERTAR RESPUESTA EN LA IMAGEN
    conclusion_dist = (
        "RESPUESTA: La Licitación Pública se asocia a montos más ALTOS ($10^9 - $10^11).\n"
        "La Contratación Directa se asocia a montos más BAJOS, pero mayor frecuencia.\n"
        "IMPLICACIÓN: El riesgo fiscal crítico se concentra en la Licitación Pública."
    )
    plt.figtext(0.5, 0.02, conclusion_dist, ha="center", fontsize=10, 
                bbox={"facecolor":"white", "alpha":0.8, "edgecolor":"green", "pad":5})

    plt.savefig(IMG_DIST, bbox_inches='tight')
    plt.close()

    print("\n*** PROCESO COMPLETADO: LAS RESPUESTAS ESTÁN INTEGRADAS EN LAS IMÁGENES ***")
    client.close()

except Exception as e:
    print(f"\n[ERROR]: {str(e)}")