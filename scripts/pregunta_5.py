import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_CSV = "/output/pregunta_5_duracion_valor.csv"
OUTPUT_PIVOT = "/output/pregunta_5_tabla_pivote.csv"
OUTPUT_PNG = "/output/pregunta_5_duracion_valor_heatmap.png"

client = Client(DASK_SCHEDULER)
print(client)

cols = [
    "departamento_entidad",
    "tipo_de_contrato",
    "valor_total_adjudicacion",
    "duracion",
    "unidad_de_duracion",
]

ddf = dd.read_parquet(PARQUET, engine="pyarrow", columns=cols)

ddf["departamento_entidad"] = ddf["departamento_entidad"].fillna("").str.strip().str.upper()
ddf["tipo_de_contrato"] = ddf["tipo_de_contrato"].fillna("").str.strip().str.upper()
ddf["unidad_de_duracion"] = ddf["unidad_de_duracion"].fillna("").str.strip().str.upper()

ddf["duracion_num"] = dd.to_numeric(ddf["duracion"], errors="coerce")

unidad = ddf["unidad_de_duracion"]

ddf["duracion_dias"] = ddf["duracion_num"]
ddf["duracion_dias"] = ddf["duracion_dias"].where(
    ~unidad.str.contains("MES", na=False),
    ddf["duracion_num"] * 30
)
ddf["duracion_dias"] = ddf["duracion_dias"].where(
    ~unidad.str.contains("AÑO|ANO|YEAR", na=False),
    ddf["duracion_num"] * 365
)

def clasificar_duracion_particion(df):
    df["categoria_duracion"] = "Sin Clasificar"
    df.loc[df["duracion_dias"] < 30, "categoria_duracion"] = "Corto"
    df.loc[df["duracion_dias"].between(30, 365, inclusive="both"), "categoria_duracion"] = "Mediano"
    df.loc[df["duracion_dias"] > 365, "categoria_duracion"] = "Largo"
    return df

ddf = ddf.map_partitions(clasificar_duracion_particion)

ddf = ddf[
    (ddf["departamento_entidad"] != "") &
    (ddf["valor_total_adjudicacion"].notnull()) &
    (ddf["valor_total_adjudicacion"] > 0) &
    (ddf["duracion_dias"].notnull()) &
    (ddf["duracion_dias"] > 0) &
    (ddf["categoria_duracion"] != "Sin Clasificar")
]

top_deptos = (
    ddf.groupby("departamento_entidad")["valor_total_adjudicacion"]
    .sum()
    .compute()
    .sort_values(ascending=False)
    .head(10)
    .index
    .tolist()
)

ddf_top = ddf[ddf["departamento_entidad"].isin(top_deptos)]

resumen = (
    ddf_top.groupby(["categoria_duracion", "departamento_entidad"])
    .agg({
        "valor_total_adjudicacion": ["mean", "count"],
        "duracion_dias": "mean"
    })
    .compute()
)

resumen.columns = ["Valor Promedio", "Cantidad Contratos", "Duración Promedio Días"]

resumen = resumen.reset_index().rename(columns={
    "categoria_duracion": "Categoría duración",
    "departamento_entidad": "Departamento"
})

orden_categorias = ["Corto", "Mediano", "Largo"]
resumen["orden_cat"] = resumen["Categoría duración"].map({
    "Corto": 1,
    "Mediano": 2,
    "Largo": 3
})
resumen = resumen.sort_values(["orden_cat", "Departamento"]).drop(columns="orden_cat")

resumen.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

pivot = resumen.pivot(
    index="Categoría duración",
    columns="Departamento",
    values="Valor Promedio"
).fillna(0)

pivot = pivot.reindex([c for c in orden_categorias if c in pivot.index])
pivot = pivot[[d for d in top_deptos if d in pivot.columns]]

pivot.to_csv(OUTPUT_PIVOT, encoding="utf-8-sig")

print("\nTABLA PIVOTE — VALOR PROMEDIO")
print(pivot.to_string())

prom_categoria = (
    ddf.groupby("categoria_duracion")["valor_total_adjudicacion"]
    .mean()
    .compute()
    .reindex([c for c in orden_categorias if c in ddf["categoria_duracion"].dropna().unique().compute()])
)

print("\nPROMEDIO GENERAL POR DURACIÓN")
print(prom_categoria.to_string())

if all(cat in prom_categoria.index for cat in ["Corto", "Mediano", "Largo"]):
    corto = prom_categoria.loc["Corto"]
    mediano = prom_categoria.loc["Mediano"]
    largo = prom_categoria.loc["Largo"]

    if corto < mediano < largo:
        print("\n→ Sí, el valor promedio crece con la duración.")
    else:
        print("\n→ No necesariamente. El valor promedio no crece de forma ordenada con la duración.")

fig, ax = plt.subplots(figsize=(14, 6))

im = ax.imshow(pivot.values / 1e9, aspect="auto")

ax.set_xticks(range(len(pivot.columns)))
ax.set_xticklabels(pivot.columns, rotation=45, ha="right", fontsize=8)

ax.set_yticks(range(len(pivot.index)))
ax.set_yticklabels(pivot.index, fontsize=9)

for i in range(len(pivot.index)):
    for j in range(len(pivot.columns)):
        valor = pivot.values[i, j] / 1e9
        ax.text(j, i, f"{valor:.1f}", ha="center", va="center", fontsize=7)

ax.set_title("Valor promedio adjudicado por duración y top 10 departamentos", fontsize=13, fontweight="bold")
ax.set_xlabel("Departamento")
ax.set_ylabel("Categoría de duración")

cbar = plt.colorbar(im, ax=ax)
cbar.set_label("Valor promedio adjudicado (miles de millones COP)")

plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight")

print(f"\nTabla resumen guardada en: {OUTPUT_CSV}")
print(f"Tabla pivote guardada en: {OUTPUT_PIVOT}")
print(f"Gráfica guardada en: {OUTPUT_PNG}")

client.close()