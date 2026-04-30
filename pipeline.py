# ============================================================
# SECOP — Lectura y limpieza con Dask distribuido
# Conecta al scheduler Dask que corre en Docker
# ============================================================

import concurrent.futures
import threading
import pathlib
import numpy as np
import requests
import dask.dataframe as dd
import pandas as pd
from io import StringIO #Convierte el texto de la respuesta HTTP en un archivo legible por pandas
from warnings import filterwarnings
from dask.distributed import Client

filterwarnings("ignore")

# ── Configuración ────────────────────────────────────────────
DASK_SCHEDULER  = "tcp://dask-scheduler:8786"   # nombre del servicio en docker-compose
BASE_URL        = "https://www.datos.gov.co/resource/p6dx-8zbt.csv"
CHUNK           = 50_000
TOTAL_FILAS     = 8_600_000
MAX_WORKERS     = 10
DIR_CHUNKS      = pathlib.Path("/datos/secop_chunks")   # volumen compartido Docker
DIR_OUTPUT      = pathlib.Path("/datos/secop_limpio.parquet")

DIR_CHUNKS.mkdir(parents=True, exist_ok=True)

COLS_NUMERICAS = ["valor_total_adjudicacion", "precio_base", "duracion"]

DATE_COLS = [
    "fecha_de_publicacion_del", "fecha_de_ultima_publicaci",
    "fecha_de_publicacion_fase", "fecha_de_publicacion_fase_1",
    "fecha_de_publicacion", "fecha_de_publicacion_fase_2",
    "fecha_de_publicacion_fase_3", "fecha_de_recepcion_de",
    "fecha_de_apertura_de_respuesta", "fecha_de_apertura_efectiva",
    "fecha_adjudicacion",
]

MAPEO_DEPTOS = {
    "BOGOTA": "BOGOTÁ D.C.", "BOGOTÁ": "BOGOTÁ D.C.",
    "BOGOTA D.C.": "BOGOTÁ D.C.",
    "DISTRITO CAPITAL DE BOGOTÁ": "BOGOTÁ D.C.",
    "CUNDINAMRACA": "CUNDINAMARCA",
    "ATLANTICO": "ATLÁNTICO",
    "CARTAGENA": "BOLÍVAR",
    "COTA": "CUNDINAMARCA",
    "NO DEFINIDO": np.nan,
}


# ── 1. Descarga paralela ─────────────────────────────────────
def descargar_chunk(offset):
    hilo = threading.current_thread().name.split("_")[-1]
    ruta = DIR_CHUNKS / f"secop_chunk_{offset:07d}.csv"

    if ruta.exists():
        print(f"  [Hilo-{hilo}] Saltando offset={offset:,} (ya existe)")
        return ruta

    print(f"  [Hilo-{hilo}] Descargando offset={offset:,}…")
    resp = requests.get(f"{BASE_URL}?$limit={CHUNK}&$offset={offset}", timeout=300)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text), low_memory=False)
    df.to_csv(ruta, index=False)

    tam_kb = ruta.stat().st_size / 1024
    print(f"  [Hilo-{hilo}] OK offset={offset:,}: {len(df):,} filas | {tam_kb:.0f} KB → {ruta.name}")
    return ruta


def descargar_todos():
    offsets = list(range(0, TOTAL_FILAS, CHUNK))
    for i in range(0, len(offsets), MAX_WORKERS):
        lote = offsets[i : i + MAX_WORKERS]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            list(ex.map(descargar_chunk, lote))
        print(f"Descargadas {min((i + MAX_WORKERS) * CHUNK, TOTAL_FILAS):,} / {TOTAL_FILAS:,} filas\n")


# ── 2. Carga lazy ─────────────────────────────────────────────
def cargar_dask():
    archivos = sorted(DIR_CHUNKS.glob("*.csv"))
    if not archivos:
        raise FileNotFoundError(f"No hay CSVs en {DIR_CHUNKS}")

    columnas = pd.read_csv(archivos[0], nrows=0).columns.tolist()
    dtypes_forzados = {col: "object" for col in columnas}

    ddf = dd.read_csv(
        str(DIR_CHUNKS / "*.csv"),
        dtype=dtypes_forzados,
        assume_missing=True,
    )
    print(f"Particiones Dask: {ddf.npartitions}")
    return ddf


# ── 3. Limpieza ───────────────────────────────────────────────
def convertir_a_dias_vectorizado(duracion, unidad):
    u    = unidad.astype(str).str.upper().str.strip()
    dias = duracion.copy()
    dias = dias.where(~u.str.contains("MES",  na=False), duracion * 30)
    dias = dias.where(~u.str.contains("AÑO",  na=False), duracion * 365)
    dias = dias.where(~u.str.contains("YEAR", na=False), duracion * 365)
    return dias


def limpiar_particion(df):
    for col in ["entidad", "nombre_del_proveedor",
                "estado_del_procedimiento", "modalidad_de_contratacion"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    for col in COLS_NUMERICAS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("float64")

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "fecha_de_publicacion_del" in df.columns:
        df["año_publicacion_proceso"] = df["fecha_de_publicacion_del"].dt.year.astype("Int64")
        df["mes_publicacion_proceso"] = df["fecha_de_publicacion_del"].dt.month.astype("Int64")

    if "fecha_adjudicacion" in df.columns:
        df["año_adjudicacion"] = df["fecha_adjudicacion"].dt.year.astype("Int64")
        df["mes_adjudicacion"]  = df["fecha_adjudicacion"].dt.month.astype("Int64")

    if "duracion" in df.columns and "unidad_de_duracion" in df.columns:
        df["duracion_dias"] = convertir_a_dias_vectorizado(
            df["duracion"], df["unidad_de_duracion"]
        ).astype("float64")

    if "departamento_proveedor" in df.columns:
        df["departamento_proveedor"] = (
            df["departamento_proveedor"]
            .astype(str).str.strip().str.upper()
            .replace(MAPEO_DEPTOS)
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )

    if "precio_base" in df.columns:
        df["correcion_precio_base"] = df["precio_base"] < 0
        df["precio_base"] = df["precio_base"].clip(lower=0).fillna(0).astype("float64")

    if "duracion_dias" in df.columns:
        conditions = [
            df["duracion_dias"] < 30,
            df["duracion_dias"].between(30, 365),
            df["duracion_dias"] > 365,
        ]
        df["clasificacion_duracion"] = np.select(
            conditions, ["Corto", "Mediano", "Largo"], default="Sin Clasificar"
        )

    return df


def limpiar_dask(ddf):
    return ddf.map_partitions(limpiar_particion)


# ── 4. Pipeline principal ─────────────────────────────────────
def main():
    # Conectar al cluster Dask en Docker
    print("Conectando al cluster Dask…")
    client = Client(DASK_SCHEDULER)
    print(client)

    print("\n" + "=" * 50)
    print("PASO 1 — Descarga de chunks")
    print("=" * 50)
    descargar_todos()

    print("\n" + "=" * 50)
    print("PASO 2 — Carga lazy con Dask")
    print("=" * 50)
    ddf_raw = cargar_dask()

    print("\n" + "=" * 50)
    print("PASO 3 — Grafo de transformaciones (lazy)")
    print("=" * 50)
    ddf_limpio = limpiar_dask(ddf_raw)
    print("Grafo listo. Ningún dato materializado aún.")

    print("\n" + "=" * 50)
    print("PASO 4 — compute() y guardado en Parquet")
    print("=" * 50)
    ddf_limpio.to_parquet(
        str(DIR_OUTPUT),
        write_index=False,
        overwrite=True,
        engine="pyarrow",
        compression="snappy",
    )
    print(f"Dataset limpio guardado en: {DIR_OUTPUT}/")

    ddf_final = dd.read_parquet(str(DIR_OUTPUT), engine="pyarrow")
    print(f"\nDimensiones finales: {len(ddf_final):,} filas × {len(ddf_final.columns)} columnas")

    client.close()


if __name__ == "__main__":
    main()
