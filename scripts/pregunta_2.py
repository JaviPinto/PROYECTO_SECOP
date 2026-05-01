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
OUTPUT_PNG     = "/output/pregunta_2_variabilidad_tipo_contrato.png"

# ── 1. Conectar al cluster ───────────────────────────────────
print("Conectando al cluster Dask...")
client = Client(DASK_SCHEDULER)
print(client)

# ── 2. Carga lazy — solo columnas necesarias ─────────────────
ddf = dd.read_parquet(
    PARQUET,
    engine="pyarrow",
    columns=["departamento_entidad", "valor_total_adjudicacion","adjudicado","estado_del_procedimiento",
              "estado_resumen","tipo_de_contrato","modalidad_de_contratacion"],
)

# ── 3. Filtrar (lazy) ────────────────────────────────────────
ddf = ddf[
    ddf["departamento_entidad"].notnull() &
    ddf["valor_total_adjudicacion"].notnull() &
    (ddf["valor_total_adjudicacion"] > 0) &
    (ddf["adjudicado"] == "Si") &
    (ddf["estado_del_procedimiento"] == "SELECCIONADO") &
    (ddf["estado_resumen"] == "Adjudicado")
]

# ── 4. Agrupar y compute() ───────────────────────────────────
# Se calculan mean y std en un solo groupby para un solo compute()
print("Ejecutando compute()...")
agg = (
    ddf.groupby("tipo_de_contrato")["valor_total_adjudicacion"]
    .agg(["mean", "std", "count"])
    .compute()
    .reset_index()
)

agg.columns = ["Tipo de Contrato", "Promedio", "Std", "Contratos"]

# ── 5. Calcular CV y limpiar ─────────────────────────────────
agg["CV"] = (agg["Std"] / agg["Promedio"]).round(4)

# Filtrar tipos con al menos 30 contratos para que el CV sea significativo
agg = agg[agg["Contratos"] >= 30].copy()
agg = agg.sort_values("CV", ascending=False).reset_index(drop=True)

# Formateo para tabla
tabla = agg.copy()
tabla["Promedio (COP)"]  = tabla["Promedio"].apply(lambda x: f"${x:,.0f}")
tabla["Std (COP)"]       = tabla["Std"].apply(lambda x: f"${x:,.0f}")
tabla["CV"]              = tabla["CV"].apply(lambda x: f"{x:.4f}")
tabla["Contratos"]       = tabla["Contratos"].apply(lambda x: f"{x:,}")

print("\n" + "=" * 70)
print("VARIABILIDAD POR TIPO DE CONTRATO — CV DESCENDENTE")
print("=" * 70)
print(tabla[["Tipo de Contrato", "Promedio (COP)", "Std (COP)", "CV", "Contratos"]].to_string(index=False))

# ── 6. Análisis ──────────────────────────────────────────────
top1     = agg.iloc[0]
top1_cv  = float(top1["CV"].replace(",", "")) if isinstance(top1["CV"], str) else top1["CV"]
# Recalcular sobre agg original (antes del formateo)
agg_raw  = (
    ddf.groupby("tipo_de_contrato")["valor_total_adjudicacion"]
    .agg(["mean", "std", "count"])
    .compute()
    .reset_index()
)
agg_raw.columns = ["Tipo de Contrato", "Promedio", "Std", "Contratos"]
agg_raw = agg_raw[agg_raw["Contratos"] >= 30].copy()
agg_raw["CV"] = (agg_raw["Std"] / agg_raw["Promedio"]).round(4)
agg_raw = agg_raw.sort_values("CV", ascending=False).reset_index(drop=True)

mayor_cv  = agg_raw.iloc[0]
menor_cv  = agg_raw.iloc[-1]

print("\n" + "=" * 70)
print("ANÁLISIS DE DISPERSIÓN")
print("=" * 70)
print(f"Mayor dispersión : {mayor_cv['Tipo de Contrato']}  (CV = {mayor_cv['CV']:.4f})")
print(f"Menor dispersión : {menor_cv['Tipo de Contrato']}  (CV = {menor_cv['CV']:.4f})")

conclusion_mayor = (
    f"El tipo '{mayor_cv['Tipo de Contrato']}' tiene el CV más alto ({mayor_cv['CV']:.2f}), "
    f"lo que indica que los valores contratados son muy heterogéneos entre sí. "
    f"Esta modalidad agrupa contratos de naturaleza muy distinta "
    f"(desde obras pequeñas hasta megaproyectos), sin un rango de valor uniforme."
)
print(f"\n→ {conclusion_mayor}")

# ── 7. Figura combinada ───────────────────────────────────────
top_n   = min(15, len(agg_raw))   # mostrar hasta 15 tipos
plot_df = agg_raw.head(top_n).copy()

fig = plt.figure(figsize=(14, 20))
fig.patch.set_facecolor("#FAFAFA")
fig.suptitle(
    "Variabilidad por tipo de contrato — Coeficiente de Variación (CV)",
    fontsize=15, fontweight="bold", y=0.98, color="#1a1a1a",
)

gs = fig.add_gridspec(3, 1, height_ratios=[5, 3.5, 2], hspace=0.45)

# — Gráfica de barras CV —
ax_bar = fig.add_subplot(gs[0])
colores = ["#C0392B" if i == 0 else ("#E07B6A" if i < 3 else "#9FD0C8")
           for i in range(len(plot_df))]

bars = ax_bar.barh(
    plot_df["Tipo de Contrato"][::-1].values,
    plot_df["CV"][::-1].values,
    color=colores[::-1],
    edgecolor="none",
    height=0.6,
)

for bar, cv in zip(bars, plot_df["CV"][::-1].values):
    ax_bar.text(
        cv + 0.01,
        bar.get_y() + bar.get_height() / 2,
        f"  {cv:.2f}",
        va="center", ha="left", fontsize=9, color="#3d3d3a",
    )

ax_bar.set_xlabel("Coeficiente de Variación (CV = Std / Media)", fontsize=10)
ax_bar.set_title(f"Top {top_n} tipos de contrato por dispersión de valores", fontsize=11, pad=10)
ax_bar.spines[["top", "right", "left"]].set_visible(False)
ax_bar.tick_params(axis="y", labelsize=8.5)
ax_bar.tick_params(axis="x", labelsize=9)
ax_bar.set_xlim(0, plot_df["CV"].max() * 1.2)

leyenda = [
    mpatches.Patch(color="#C0392B", label="Mayor dispersión"),
    mpatches.Patch(color="#E07B6A", label="Alta dispersión"),
    mpatches.Patch(color="#9FD0C8", label="Dispersión moderada"),
]
ax_bar.legend(handles=leyenda, loc="lower right", fontsize=9, frameon=False)

# — Tabla —
ax_tab = fig.add_subplot(gs[1])
ax_tab.axis("off")
ax_tab.set_title("Tabla resumen — CV por tipo de contrato", fontsize=11, pad=10, loc="left")

tabla_data = []
for _, row in agg_raw.head(top_n).iterrows():
    tabla_data.append([
        row["Tipo de Contrato"],
        f"${row['Promedio']:,.0f}",
        f"${row['Std']:,.0f}",
        f"{row['CV']:.4f}",
        f"{int(row['Contratos']):,}",
    ])

tabla = ax_tab.table(
    cellText=tabla_data,
    colLabels=["Tipo de Contrato", "Promedio (COP)", "Std (COP)", "CV", "# Contratos"],
    cellLoc="center",
    loc="center",
)
tabla.auto_set_font_size(False)
tabla.set_fontsize(8)
tabla.scale(1, 1.5)

for j in range(5):
    tabla[0, j].set_facecolor("#2C3E50")
    tabla[0, j].set_text_props(color="white", fontweight="bold")

for i in range(1, len(tabla_data) + 1):
    color = "#FDF2F2" if i == 1 else ("#F9F9F9" if i % 2 == 0 else "white")
    for j in range(5):
        tabla[i, j].set_facecolor(color)
        tabla[i, j].set_edgecolor("#e0e0e0")

# — Análisis en texto —
ax_txt = fig.add_subplot(gs[2])
ax_txt.axis("off")
ax_txt.set_title("Análisis de dispersión", fontsize=11, pad=10, loc="left")

metricas = (
    f"Mayor CV: {mayor_cv['Tipo de Contrato']}  →  CV = {mayor_cv['CV']:.4f}  |  "
    f"Promedio: ${mayor_cv['Promedio']:,.0f} COP\n"
    f"Menor CV: {menor_cv['Tipo de Contrato']}  →  CV = {menor_cv['CV']:.4f}  |  "
    f"Promedio: ${menor_cv['Promedio']:,.0f} COP"
)

ax_txt.text(
    0.01, 0.80, metricas,
    transform=ax_txt.transAxes,
    fontsize=9, color="#2d2d2d",
    verticalalignment="top",
    fontfamily="monospace",
)
ax_txt.axhline(y=0.55, color="#e0e0e0", linewidth=0.8)
ax_txt.text(
    0.01, 0.45,
    f"→ {conclusion_mayor}",
    transform=ax_txt.transAxes,
    fontsize=9, color="#C0392B",
    verticalalignment="top",
    fontweight="bold",
    wrap=True,
)

plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\nGráfica guardada en: {OUTPUT_PNG}")

client.close()