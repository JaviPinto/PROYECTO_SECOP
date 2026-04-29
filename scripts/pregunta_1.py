# ============================================================
# Pregunta 1 — Concentración territorial del gasto público
# ============================================================

import pandas as pd
import matplotlib
matplotlib.use("Agg")   # sin pantalla dentro de Docker
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET        = "/datos/secop_limpio.parquet"
OUTPUT_PNG     = "/output/concentracion_territorial.png"

# ── 1. Conectar al cluster ───────────────────────────────────
print("Conectando al cluster Dask...")
client = Client(DASK_SCHEDULER)
print(client)

# ── 2. Cargar solo columnas necesarias (lazy) ────────────────
ddf = dd.read_parquet(
    PARQUET,
    engine="pyarrow",
    columns=["departamento_entidad", "valor_total_adjudicacion"],
)

# ── 3. Filtrar y agrupar (lazy) ──────────────────────────────
ddf_filtrado = ddf[
    ddf["departamento_entidad"].notnull() &
    (ddf["valor_total_adjudicacion"] > 0)
]

print("Ejecutando compute()...")
resumen = (
    ddf_filtrado
    .groupby("departamento_entidad")["valor_total_adjudicacion"]
    .sum()
    .compute()
    .reset_index()
    .rename(columns={
        "departamento_entidad"    : "Departamento",
        "valor_total_adjudicacion": "Valor Total",
    })
    .sort_values("Valor Total", ascending=False)
    .reset_index(drop=True)
)

# ── 4. Calcular % y % acumulado ──────────────────────────────
total_nacional     = resumen["Valor Total"].sum()
resumen["% Total"] = (resumen["Valor Total"] / total_nacional * 100).round(2)
resumen["% Acum"]  = resumen["% Total"].cumsum().round(2)

# ── 5. Tabla top 10 ──────────────────────────────────────────
top10 = resumen.head(10).copy()
top10["Valor Total (COP)"] = top10["Valor Total"].apply(lambda x: f"${x:,.0f}")
top10["% Total fmt"]       = top10["% Total"].apply(lambda x: f"{x:.2f}%")
top10["% Acum fmt"]        = top10["% Acum"].apply(lambda x: f"{x:.2f}%")

print("\n" + "=" * 70)
print("TOP 10 DEPARTAMENTOS — CONCENTRACIÓN DEL GASTO PÚBLICO")
print("=" * 70)
print(top10[["Departamento", "Valor Total (COP)", "% Total fmt", "% Acum fmt"]].to_string(index=False))

# ── 6. Análisis de centralización ────────────────────────────
top5_pct = resumen.head(5)["% Total"].sum()

print("\n" + "=" * 70)
print("ANÁLISIS DE CENTRALIZACIÓN")
print("=" * 70)
print(f"Total nacional contratado : ${total_nacional:,.0f} COP")
print(f"% acumulado top 5 depart. : {top5_pct:.2f}%")

if top5_pct > 60:
    print(f"\n→ SÍ, los 5 primeros concentran el {top5_pct:.1f}% del gasto.")
    print("  Alta centralización: la mayoría del presupuesto se ejecuta")
    print("  en pocos departamentos.")
else:
    print(f"\n→ NO, los 5 primeros concentran solo el {top5_pct:.1f}% del gasto.")
    print("  La distribución territorial es relativamente equilibrada.")

# ── 7. Gráfica ───────────────────────────────────────────────
top10_raw = resumen.head(10).copy()
colores   = ["#1D9E75" if i < 5 else "#9FE1CB" for i in range(len(top10_raw))]

fig, ax = plt.subplots(figsize=(12, 7))

bars = ax.barh(
    top10_raw["Departamento"][::-1].values,
    top10_raw["Valor Total"][::-1].values / 1e12,
    color=colores[::-1],
    edgecolor="none",
    height=0.6,
)

for bar, val, pct in zip(
    bars,
    top10_raw["Valor Total"][::-1].values,
    top10_raw["% Total"][::-1].values,
):
    ax.text(
        val / 1e12 + 0.02,
        bar.get_y() + bar.get_height() / 2,
        f"  ${val/1e12:.1f}T  ({pct:.1f}%)",
        va="center", ha="left", fontsize=9, color="#3d3d3a",
    )

ax.set_xlabel("Valor total contratado (billones COP)", fontsize=10)
ax.set_title(
    "Concentración territorial del gasto público — Top 10 departamentos",
    fontsize=13, fontweight="bold", pad=16,
)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}T"))
ax.spines[["top", "right", "left"]].set_visible(False)
ax.tick_params(axis="y", labelsize=9)
ax.tick_params(axis="x", labelsize=9)
ax.set_xlim(0, top10_raw["Valor Total"].max() / 1e12 * 1.35)

leyenda = [
    mpatches.Patch(color="#1D9E75", label="Top 5 (mayor concentración)"),
    mpatches.Patch(color="#9FE1CB", label="Posiciones 6–10"),
]
ax.legend(handles=leyenda, loc="lower right", fontsize=9, frameon=False)

plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight")
print(f"\nGráfica guardada en: {OUTPUT_PNG}")

client.close()