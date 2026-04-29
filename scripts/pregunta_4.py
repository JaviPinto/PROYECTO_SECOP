import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import dask.dataframe as dd
from dask.distributed import Client
import warnings
warnings.filterwarnings("ignore")

DASK_SCHEDULER = "tcp://dask-scheduler:8786"
PARQUET = "/datos/secop_limpio.parquet"
OUTPUT_CSV = "/output/pregunta_4_proveedores_dominantes.csv"
OUTPUT_PNG = "/output/pregunta_4_proveedores_dominantes.png"

client = Client(DASK_SCHEDULER)
print(client)

cols = [
    "nombre_del_proveedor",
    "valor_total_adjudicacion",
    "tipo_de_contrato",
    "departamento_proveedor",
]

ddf = dd.read_parquet(PARQUET, engine="pyarrow", columns=cols)

ddf["nombre_del_proveedor"] = ddf["nombre_del_proveedor"].fillna("").str.strip().str.upper()
ddf["departamento_proveedor"] = ddf["departamento_proveedor"].fillna("").str.strip().str.upper()
ddf["tipo_de_contrato"] = ddf["tipo_de_contrato"].fillna("").str.strip().str.upper()

ddf = ddf[
    (ddf["nombre_del_proveedor"] != "") &
    (ddf["valor_total_adjudicacion"].notnull()) &
    (ddf["valor_total_adjudicacion"] > 0)
]

resumen = (
    ddf.groupby("nombre_del_proveedor")
    .agg({"valor_total_adjudicacion": ["sum", "count"]})
    .compute()
)

resumen.columns = ["Valor Total", "Cantidad Contratos"]
resumen = (
    resumen.reset_index()
    .rename(columns={"nombre_del_proveedor": "Proveedor"})
    .sort_values("Valor Total", ascending=False)
    .reset_index(drop=True)
)

total_nacional = resumen["Valor Total"].sum()
resumen["% Total"] = resumen["Valor Total"] / total_nacional * 100
resumen["% Acumulado"] = resumen["% Total"].cumsum()

n_proveedores_30 = (resumen["% Acumulado"] < 30).sum() + 1

top20 = resumen.head(20).copy()
proveedores_top20 = top20["Proveedor"].tolist()

ddf_top20 = ddf[ddf["nombre_del_proveedor"].isin(proveedores_top20)]

freq_depto = (
    ddf_top20.groupby(["nombre_del_proveedor", "departamento_proveedor"])
    .size()
    .compute()
    .reset_index(name="conteo")
)

idx = freq_depto.groupby("nombre_del_proveedor")["conteo"].idxmax()

depto_principal = (
    freq_depto.loc[idx, ["nombre_del_proveedor", "departamento_proveedor"]]
    .rename(columns={
        "nombre_del_proveedor": "Proveedor",
        "departamento_proveedor": "Departamento más frecuente"
    })
)

top20 = top20.merge(depto_principal, on="Proveedor", how="left")

top20["Valor Total (COP)"] = top20["Valor Total"].apply(lambda x: f"${x:,.0f}")
top20["% Total fmt"] = top20["% Total"].apply(lambda x: f"{x:.2f}%")
top20["% Acumulado fmt"] = top20["% Acumulado"].apply(lambda x: f"{x:.2f}%")

top20.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("\nTOP 20 PROVEEDORES DOMINANTES")
print(top20[[
    "Proveedor",
    "Valor Total (COP)",
    "Cantidad Contratos",
    "% Total fmt",
    "% Acumulado fmt",
    "Departamento más frecuente"
]].to_string(index=False))

print("\nANÁLISIS DE CONCENTRACIÓN")
print(f"Total nacional adjudicado: ${total_nacional:,.0f} COP")
print(f"Participación acumulada del Top 20: {top20['% Total'].sum():.2f}%")
print(f"Proveedores necesarios para acumular el 30% del gasto: {n_proveedores_30}")

top20_plot = top20.sort_values("Valor Total", ascending=True)

fig, ax = plt.subplots(figsize=(13, 9))

bars = ax.barh(
    top20_plot["Proveedor"],
    top20_plot["Valor Total"] / 1e12,
    height=0.6
)

for bar, val, pct in zip(bars, top20_plot["Valor Total"], top20_plot["% Total"]):
    ax.text(
        val / 1e12 + 0.02,
        bar.get_y() + bar.get_height() / 2,
        f"${val/1e12:.1f}T ({pct:.2f}%)",
        va="center",
        fontsize=8
    )

ax.set_title("Top 20 proveedores por valor total adjudicado", fontsize=14, fontweight="bold")
ax.set_xlabel("Valor total adjudicado (billones COP)")
ax.set_ylabel("Proveedor")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}T"))
ax.spines[["top", "right", "left"]].set_visible(False)
ax.tick_params(axis="y", labelsize=8)
ax.set_xlim(0, top20["Valor Total"].max() / 1e12 * 1.35)

plt.tight_layout()
plt.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight")

print(f"\nTabla guardada en: {OUTPUT_CSV}")
print(f"Gráfica guardada en: {OUTPUT_PNG}")

client.close()