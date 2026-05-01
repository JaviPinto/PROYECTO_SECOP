import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET        = "/datos/secop_limpio.parquet"
OUTPUT_PNG     = "/output/pregunta_1_concentracion_territorial.png"

print("Conectando al cluster Dask...")
client = Client(DASK_SCHEDULER)
print(client)

ddf = dd.read_parquet(
    PARQUET,
    engine="pyarrow",
    columns=["departamento_entidad", "valor_total_adjudicacion","adjudicado","estado_del_procedimiento", "estado_resumen"],
)

ddf_filtrado = ddf[
    ddf["departamento_entidad"].notnull() &
    ddf["valor_total_adjudicacion"].notnull() &
    (ddf["valor_total_adjudicacion"] > 0) &
    (ddf["adjudicado"] == "Si") &
    (ddf["estado_del_procedimiento"] == "SELECCIONADO") &
    (ddf["estado_resumen"] == "Adjudicado")
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

total_nacional     = resumen["Valor Total"].sum()
resumen["% Total"] = (resumen["Valor Total"] / total_nacional * 100).round(2)
resumen["% Acum"]  = resumen["% Total"].cumsum().round(2)

top10     = resumen.head(10).copy()
top5_pct  = resumen.head(5)["% Total"].sum()
top10_raw = resumen.head(10).copy()
colores   = ["#1D9E75" if i < 5 else "#9FE1CB" for i in range(len(top10_raw))]

if top5_pct > 60:
    conclusion = (
        f"Los 5 primeros concentran el {top5_pct:.1f}% del gasto.\n"
        f"Alta centralización: la mayoría del presupuesto se ejecuta en pocos departamentos."
    )
else:
    conclusion = (
        f"Los 5 primeros concentran solo el {top5_pct:.1f}% del gasto.\n"
        f"La distribución territorial es relativamente equilibrada."
    )

fig = plt.figure(figsize=(14, 20))
fig.patch.set_facecolor("#FAFAFA")
fig.suptitle(
    "Concentración territorial del gasto público — Top 10 departamentos",
    fontsize=15, fontweight="bold", y=0.98, color="#1a1a1a",
)

gs = fig.add_gridspec(3, 1, height_ratios=[5, 3.5, 2], hspace=0.45)

ax = fig.add_subplot(gs[0])

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
ax.set_title("Gráfica de barras — valor contratado por departamento", fontsize=11, pad=10)
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

ax_tab = fig.add_subplot(gs[1])
ax_tab.axis("off")
ax_tab.set_title("TOP 10 DEPARTAMENTOS — CONCENTRACIÓN DEL GASTO PÚBLICO", fontsize=11, fontweight="bold", pad=10, loc="left")

tabla_data = []
for _, row in top10.iterrows():
    tabla_data.append([
        row["Departamento"],
        f"${row['Valor Total']:,.0f}",
        f"{row['% Total']:.2f}%",
        f"{row['% Acum']:.2f}%",
    ])

tabla = ax_tab.table(
    cellText=tabla_data,
    colLabels=["Departamento", "Valor Total (COP)", "% del Total", "% Acumulado"],
    cellLoc="center",
    loc="center",
)
tabla.auto_set_font_size(False)
tabla.set_fontsize(8.5)
tabla.scale(1, 1.6)

for j in range(4):
    tabla[0, j].set_facecolor("#1D9E75")
    tabla[0, j].set_text_props(color="white", fontweight="bold")

for i in range(1, len(tabla_data) + 1):
    color = "#F0FAF6" if i % 2 == 0 else "white"
    for j in range(4):
        tabla[i, j].set_facecolor(color)
        tabla[i, j].set_edgecolor("#e0e0e0")

ax_txt = fig.add_subplot(gs[2])
ax_txt.axis("off")
ax_txt.set_title("ANÁLISIS DE CENTRALIZACIÓN", fontsize=11, fontweight="bold", pad=10, loc="left")

metricas = (
    f"Total nacional contratado : ${total_nacional:,.0f} COP\n"
    f"% acumulado top 5 depart. : {top5_pct:.2f}%"
)

ax_txt.text(
    0.01, 0.80, metricas,
    transform=ax_txt.transAxes,
    fontsize=9.5, color="#2d2d2d",
    verticalalignment="top",
    fontfamily="monospace",
)
ax_txt.axhline(y=0.52, color="#e0e0e0", linewidth=0.8)
ax_txt.text(
    0.01, 0.42, f"→ {conclusion}",
    transform=ax_txt.transAxes,
    fontsize=9.5,
    color="#1D9E75" if top5_pct > 60 else "#C0392B",
    verticalalignment="top",
    fontweight="bold",
)

plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\nGráfica guardada en: {OUTPUT_PNG}")

client.close()