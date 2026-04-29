import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_CSV = "/output/pregunta_6_efectividad_orden.csv"
OUTPUT_NO_FINAL = "/output/pregunta_6_estados_no_finales.csv"
OUTPUT_PNG = "/output/pregunta_6_efectividad_orden.png"

client = Client(DASK_SCHEDULER)
print(client)

cols = [
    "ordenentidad",
    "estado_del_procedimiento",
    "modalidad_de_contratacion",
    "valor_total_adjudicacion",
]

ddf = dd.read_parquet(PARQUET, engine="pyarrow", columns=cols)

ddf = ddf[
    ddf["ordenentidad"].notnull() &
    ddf["estado_del_procedimiento"].notnull()
]

def clasificar_estado_particion(df):
    df["ordenentidad"] = df["ordenentidad"].astype(str).str.strip().str.upper()
    df["estado_del_procedimiento"] = df["estado_del_procedimiento"].astype(str).str.strip().str.upper()
    df["modalidad_de_contratacion"] = df["modalidad_de_contratacion"].astype(str).str.strip().str.upper()

    estado = df["estado_del_procedimiento"]

    df["estado_clasificado"] = "NO_FINAL"

    df.loc[
        estado.str.contains("SELECCIONADO|APROBADO|ADJUDIC|CELEBR|CONTRAT", na=False),
        "estado_clasificado"
    ] = "CELEBRADO"

    df.loc[
        estado.str.contains("DESIER", na=False),
        "estado_clasificado"
    ] = "DESIERTO"

    df.loc[
        estado.str.contains("CANCEL", na=False),
        "estado_clasificado"
    ] = "CANCELADO"

    return df

ddf = ddf.map_partitions(clasificar_estado_particion)

print("\nDistribución de estados clasificados:")
dist_estados = ddf["estado_clasificado"].value_counts().compute()
print(dist_estados.to_string())

no_finales = (
    ddf[ddf["estado_clasificado"] == "NO_FINAL"]
    .groupby(["ordenentidad", "estado_del_procedimiento"])
    .size()
    .compute()
    .reset_index(name="Cantidad")
    .sort_values("Cantidad", ascending=False)
)

no_finales.to_csv(OUTPUT_NO_FINAL, index=False, encoding="utf-8-sig")

ddf_finales = ddf[
    ddf["estado_clasificado"].isin(["CELEBRADO", "DESIERTO", "CANCELADO"])
]

conteo = (
    ddf_finales.groupby(["ordenentidad", "estado_clasificado"])
    .size()
    .compute()
    .reset_index(name="Cantidad")
)

tabla = conteo.pivot_table(
    index="ordenentidad",
    columns="estado_clasificado",
    values="Cantidad",
    aggfunc="sum",
    fill_value=0
)

for col in ["CELEBRADO", "DESIERTO", "CANCELADO"]:
    if col not in tabla.columns:
        tabla[col] = 0

tabla["Total procesos finales"] = tabla[["CELEBRADO", "DESIERTO", "CANCELADO"]].sum(axis=1)

tabla["Tasa éxito %"] = (tabla["CELEBRADO"] / tabla["Total procesos finales"] * 100).round(2)
tabla["Tasa desierto %"] = (tabla["DESIERTO"] / tabla["Total procesos finales"] * 100).round(2)
tabla["Tasa cancelado %"] = (tabla["CANCELADO"] / tabla["Total procesos finales"] * 100).round(2)
tabla["Tasa fallidos %"] = (
    (tabla["DESIERTO"] + tabla["CANCELADO"]) /
    tabla["Total procesos finales"] * 100
).round(2)

valor_orden = (
    ddf_finales.groupby("ordenentidad")["valor_total_adjudicacion"]
    .sum()
    .compute()
    .reset_index()
    .rename(columns={"valor_total_adjudicacion": "Valor total adjudicado"})
)

tabla = tabla.reset_index().rename(columns={"ordenentidad": "Orden Entidad"})

tabla = tabla.merge(
    valor_orden.rename(columns={"ordenentidad": "Orden Entidad"}),
    on="Orden Entidad",
    how="left"
)

tabla = tabla.sort_values("Tasa éxito %", ascending=False).reset_index(drop=True)
tabla.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("\nEFECTIVIDAD POR ORDEN DEL ESTADO")
print(tabla.to_string(index=False))

print("\nÓRDENES CON MAYOR TASA DE PROCESOS FALLIDOS")
fallidos = tabla.sort_values("Tasa fallidos %", ascending=False).head(5)
print(fallidos[[
    "Orden Entidad",
    "Total procesos finales",
    "Tasa éxito %",
    "Tasa desierto %",
    "Tasa cancelado %",
    "Tasa fallidos %",
    "Valor total adjudicado"
]].to_string(index=False))

graf = tabla.set_index("Orden Entidad")[[
    "Tasa éxito %",
    "Tasa desierto %",
    "Tasa cancelado %"
]]

graf = graf.sort_values("Tasa éxito %", ascending=True)

fig, ax = plt.subplots(figsize=(12, 7))

left = None

for col in graf.columns:
    vals = graf[col].values

    if left is None:
        ax.barh(graf.index, vals, label=col)
        left = vals
    else:
        ax.barh(graf.index, vals, left=left, label=col)
        left = left + vals

ax.set_title("Efectividad de procesos por orden de entidad", fontsize=13, fontweight="bold")
ax.set_xlabel("Porcentaje de procesos finales")
ax.set_ylabel("Orden de entidad")
ax.set_xlim(0, 100)
ax.legend(loc="lower right", fontsize=8)
ax.spines[["top", "right", "left"]].set_visible(False)

plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight")

print(f"\nTabla guardada en: {OUTPUT_CSV}")
print(f"Estados no finales guardados en: {OUTPUT_NO_FINAL}")
print(f"Gráfica guardada en: {OUTPUT_PNG}")

print("\nINTERPRETACIÓN:")
print(
    "Para calcular la efectividad se consideraron únicamente procesos con estados finales: "
    "CELEBRADO, DESIERTO y CANCELADO. Los estados NO_FINAL corresponden a procesos publicados, "
    "en evaluación, abiertos, en borrador o suspendidos, por lo que no se incluyen en el denominador "
    "principal de efectividad."
)

client.close()