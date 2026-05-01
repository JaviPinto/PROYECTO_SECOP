import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET        = "/datos/secop_limpio.parquet"
OUTPUT_PNG     = "/output/pregunta_3_evolucion_mensual_contratacion.png"

# ── 1. Conectar ──────────────────────────────────────────────
print("Conectando al cluster Dask...")
client = Client(DASK_SCHEDULER)
print(client)

# ── 2. Carga lazy — columnas necesarias ──────────────────────
ddf = dd.read_parquet(
    PARQUET,
    engine="pyarrow",
    columns=[
        "año_publicacion_proceso",
        "mes_publicacion_proceso",
        "tipo_de_contrato",
        "valor_total_adjudicacion"
    ],
)

# ── 3. Filtrar nulos y valores inválidos (lazy) ───────────────
ddf = ddf[
    ddf["año_publicacion_proceso"].notnull() &
    ddf["mes_publicacion_proceso"].notnull() &
    ddf["tipo_de_contrato"].notnull() &
    ddf["valor_total_adjudicacion"].notnull() ]

# ── 4. Top 3 tipos de contrato — compute() auxiliar ──────────
print("Calculando top 3 tipos de contrato...")
top3_tipos = (
    ddf.groupby("tipo_de_contrato")["valor_total_adjudicacion"]
    .count()
    .compute()
    .sort_values(ascending=False)
    .head(3)
    .index.tolist()
)
print(f"Top 3 tipos: {top3_tipos}")

# ── 5. Filtrar solo top 3 y agrupar por mes/año/tipo ─────────
ddf_top3 = ddf[ddf["tipo_de_contrato"].isin(top3_tipos)]

print("Ejecutando compute() principal...")
serie = (
    ddf_top3
    .groupby(["año_publicacion_proceso", "mes_publicacion_proceso", "tipo_de_contrato"])
    .size()
    .compute()
    .reset_index()
    .rename(columns={0: "cantidad"})
)

# Serie total (todos los tipos) para la línea general
serie_total = (
    ddf
    .groupby(["año_publicacion_proceso", "mes_publicacion_proceso"])
    .size()
    .compute()
    .reset_index()
    .rename(columns={0: "cantidad"})
)

# ── 6. Construir columna fecha para el eje X ─────────────────
def construir_fecha(df):
    df["fecha"] = pd.to_datetime(
        df["año_publicacion_proceso"].astype(int).astype(str) + "-" +
        df["mes_publicacion_proceso"].astype(int).astype(str).str.zfill(2) + "-01"
    )
    return df.sort_values("fecha")

serie       = construir_fecha(serie)
serie_total = construir_fecha(serie_total)

# ── 7. Análisis — pico inusual ────────────────────────────────
pico        = serie_total.loc[serie_total["cantidad"].idxmax()]
pico_fecha  = pico["fecha"].strftime("%B %Y")
pico_cant   = int(pico["cantidad"])
media_cant  = serie_total["cantidad"].mean()
veces_media = pico_cant / media_cant

print("\n" + "=" * 70)
print("ANÁLISIS DE EVOLUCIÓN MENSUAL")
print("=" * 70)
print(f"Período analizado : {serie_total['fecha'].min().strftime('%b %Y')} → {serie_total['fecha'].max().strftime('%b %Y')}")
print(f"Pico máximo       : {pico_fecha}  ({pico_cant:,} contratos)")
print(f"Media mensual     : {media_cant:,.0f} contratos")
print(f"El pico es {veces_media:.1f}x la media mensual")

if veces_media > 2:
    conclusion = (
        f"Sí hay un pico inusual en {pico_fecha} con {pico_cant:,} contratos "
        f"({veces_media:.1f}x la media). Esto suele atribuirse a cierres de "
        f"vigencia presupuestal (diciembre), inicio de gobierno (agosto), o "
        f"respuesta a emergencias nacionales."
    )
else:
    conclusion = (
        f"No hay picos extremos. El mes de mayor actividad fue {pico_fecha} "
        f"con {pico_cant:,} contratos, apenas {veces_media:.1f}x la media mensual."
    )
print(f"\n→ {conclusion}")

# ── 8. Figura combinada ───────────────────────────────────────
COLORES = ["#1D9E75", "#E07B2A", "#2A7AE0"]
tipos_colores = dict(zip(top3_tipos, COLORES))

fig = plt.figure(figsize=(15, 20))
fig.patch.set_facecolor("#FAFAFA")
fig.suptitle(
    "Evolución mensual de la contratación pública — SECOP",
    fontsize=15, fontweight="bold", y=0.98, color="#1a1a1a",
)

gs = fig.add_gridspec(3, 1, height_ratios=[5, 3, 2], hspace=0.45)

# — Serie de tiempo —
ax_line = fig.add_subplot(gs[0])

# Línea total en gris de fondo
ax_line.plot(
    serie_total["fecha"],
    serie_total["cantidad"],
    color="#CCCCCC", linewidth=1.5,
    label="Total general", zorder=1,
)

# Línea por cada top 3 tipo
for tipo in top3_tipos:
    sub = serie[serie["tipo_de_contrato"] == tipo].sort_values("fecha")
    ax_line.plot(
        sub["fecha"],
        sub["cantidad"],
        color=tipos_colores[tipo],
        linewidth=2,
        label=tipo,
        zorder=2,
    )

# Marcar el pico
ax_line.axvline(
    x=pico["fecha"], color="#C0392B",
    linestyle="--", linewidth=1.2, alpha=0.7, zorder=3,
)
ax_line.annotate(
    f"Pico: {pico_fecha}\n{pico_cant:,} contratos",
    xy=(pico["fecha"], pico_cant),
    xytext=(30, -40), textcoords="offset points",
    fontsize=8.5, color="#C0392B",
    arrowprops=dict(arrowstyle="->", color="#C0392B", lw=1.2),
)

ax_line.set_xlabel("Mes - Año", fontsize=10)
ax_line.set_ylabel("Número de contratos publicados", fontsize=10)
ax_line.set_title("Contratos publicados por mes — Total y top 3 tipos", fontsize=11, pad=10)
ax_line.xaxis.set_major_formatter(mdates.DateFormatter("%b-%Y"))
ax_line.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
plt.setp(ax_line.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)
ax_line.spines[["top", "right"]].set_visible(False)
ax_line.legend(fontsize=9, frameon=False, loc="upper left")
ax_line.yaxis.set_major_formatter(
    plt.FuncFormatter(lambda x, _: f"{int(x):,}")
)

# — Tabla: top 10 meses con más contratos —
ax_tab = fig.add_subplot(gs[1])
ax_tab.axis("off")
ax_tab.set_title("Top 10 meses con mayor actividad contractual", fontsize=11, pad=10, loc="left")

top10_meses = (
    serie_total
    .sort_values("cantidad", ascending=False)
    .head(10)
    .copy()
)
top10_meses["Mes-Año"]    = top10_meses["fecha"].dt.strftime("%B %Y")
top10_meses["Contratos"]  = top10_meses["cantidad"].apply(lambda x: f"{x:,}")
top10_meses["vs Media"]   = top10_meses["cantidad"].apply(
    lambda x: f"{x/media_cant:.2f}x"
)

tabla_data = top10_meses[["Mes-Año", "Contratos", "vs Media"]].values.tolist()

tabla = ax_tab.table(
    cellText=tabla_data,
    colLabels=["Mes - Año", "# Contratos", "vs Media mensual"],
    cellLoc="center",
    loc="center",
)
tabla.auto_set_font_size(False)
tabla.set_fontsize(9)
tabla.scale(1, 1.7)

for j in range(3):
    tabla[0, j].set_facecolor("#2C3E50")
    tabla[0, j].set_text_props(color="white", fontweight="bold")

for i in range(1, len(tabla_data) + 1):
    color = "#FDF2F2" if i == 1 else ("#F9F9F9" if i % 2 == 0 else "white")
    for j in range(3):
        tabla[i, j].set_facecolor(color)
        tabla[i, j].set_edgecolor("#e0e0e0")

# — Análisis en texto —
ax_txt = fig.add_subplot(gs[2])
ax_txt.axis("off")
ax_txt.set_title("Análisis de picos", fontsize=11, pad=10, loc="left")

metricas = (
    f"Período analizado : {serie_total['fecha'].min().strftime('%b %Y')} → {serie_total['fecha'].max().strftime('%b %Y')}\n"
    f"Pico máximo       : {pico_fecha}  ({pico_cant:,} contratos)   |   "
    f"Media mensual: {media_cant:,.0f} contratos   |   Factor: {veces_media:.1f}x"
)

ax_txt.text(
    0.01, 0.85, metricas,
    transform=ax_txt.transAxes,
    fontsize=9, color="#2d2d2d",
    verticalalignment="top",
    fontfamily="monospace",
)
ax_txt.axhline(y=0.58, color="#e0e0e0", linewidth=0.8)
ax_txt.text(
    0.01, 0.48, f"→ {conclusion}",
    transform=ax_txt.transAxes,
    fontsize=9,
    color="#C0392B" if veces_media > 2 else "#1D9E75",
    verticalalignment="top",
    fontweight="bold",
)

plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\nGráfica guardada en: {OUTPUT_PNG}")

client.close()