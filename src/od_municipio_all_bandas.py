#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import time
import datetime as dt
import duckdb
import pandas as pd  # solo para compatibilidad; no es imprescindible

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(asctime)s:%(message)s")
logger = logging.getLogger(__name__)

# Franjas horarias (inicio/fin incluidos) — formato HH:MM:SS
TIME_BANDS = [
    ("Madrugada",  "00:00:01", "06:00:00"),
    ("Mañana",     "06:00:01", "10:00:00"),
    ("MedioDia",   "10:00:01", "14:00:00"),
    ("Tarde",      "14:00:01", "17:00:00"),
    ("Noche",      "17:00:01", "21:00:00"),
    ("MediaNoche", "21:00:01", "23:59:59"),
]

def to_epoch(date_str: str, time_str: str) -> int:
    t = dt.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(t.timetuple()))

def daterange(d0: dt.date, d1: dt.date):
    cur = d0
    one = dt.timedelta(days=1)
    while cur <= d1:
        yield cur
        cur += one

class Main:
    def __init__(self):
        p = argparse.ArgumentParser(
            description="OD por municipio (todos los municipios) para múltiples franjas por día (DuckDB)."
        )
        p.add_argument("-d", "--date", required=True,
                       help="Fecha inicial (yyyy-mm-dd).")
        p.add_argument("-D", "--date_end", default=None,
                       help="Fecha final (yyyy-mm-dd). Si no se da, procesa solo la fecha -d.")
        p.add_argument("-w", "--dwell", required=True, type=int,
                       help="Umbral dwell time (segundos) para contar una arista.")
        p.add_argument("-db", "--dataset", required=True,
                       help="Directorio raíz del dataset parquet (particionado en year=/month=/day=).")
        p.add_argument("-sf", "--suffix", default="",
                       help="Sufijo para los archivos de salida (opcional).")
        p.add_argument("-o", "--output", required=True,
                       help="Directorio de salida.")
        self.args = p.parse_args()

    def run(self):
        t0 = time.perf_counter()

        # --- Rango de fechas ---
        d_start = dt.datetime.strptime(self.args.date, "%Y-%m-%d").date()
        d_end = dt.datetime.strptime(self.args.date_end, "%Y-%m-%d").date() if self.args.date_end else d_start

        dataset_root = self.args.dataset.rstrip("/")
        dwell_time = int(self.args.dwell)
        out_dir = self.args.output
        os.makedirs(out_dir, exist_ok=True)

        suffix = (self.args.suffix or "").strip()
        base_name = (suffix + "_" if suffix else "") + "od_municipio_all"

        con = duckdb.connect()

        for cur_date in daterange(d_start, d_end):
            date_str = cur_date.strftime("%Y-%m-%d")
            year, month, day = cur_date.strftime("%Y %m %d").split()
            parquet_glob = f"{dataset_root}/year={year}/month={month}/day={day}/*"

            tbl_enr = f"movement_{year}{month}{day}_enr"
            tbl_tmp1 = f"_tmp1_{year}{month}{day}"

            logging.info(f"==> Procesando {date_str}")

            # 1) Carga del día + construcción de cve_mun_full (todos los registros)
            #    Asegura que cve_ent/cve_mun existan en el dataset.
            con.execute(f"""
                CREATE OR REPLACE TABLE {tbl_enr} AS
                SELECT
                    *,
                    lpad(CAST(cve_ent AS VARCHAR), 2, '0') || lpad(CAST(cve_mun AS VARCHAR), 3, '0') AS cve_mun_full
                FROM read_parquet('{parquet_glob}');
            """)

            # 2) Conteo de dispositivos del día (denominador para normalización)
            dfc = con.execute(f"SELECT COUNT(DISTINCT caid) AS total_caid FROM {tbl_enr}").df()
            if dfc.empty or dfc['total_caid'][0] is None:
                logging.warning(f"   Sin datos en {date_str}; se omite el día.")
                continue
            total_caid = int(dfc["total_caid"][0])
            if total_caid == 0:
                logging.warning(f"   Dispositivos únicos = 0 en {date_str}; se omite el día.")
                continue
            logging.info(f"   Dispositivos únicos: {total_caid}")

            # 3) Precálculo de transiciones municipio→municipio con dwell_time
            con.execute(f"""
                CREATE OR REPLACE TABLE {tbl_tmp1} AS
                WITH base AS (
                    SELECT
                        caid,
                        cve_mun_full AS source_mun,
                        utc_timestamp,
                        LAG(cve_mun_full, -1) OVER (PARTITION BY caid ORDER BY utc_timestamp) AS target_mun,
                        LAG(utc_timestamp, -1) OVER (PARTITION BY caid ORDER BY utc_timestamp) AS target_time
                    FROM {tbl_enr}
                )
                SELECT
                    caid,
                    source_mun,
                    target_mun,
                    utc_timestamp,
                    target_time,
                    (target_time - utc_timestamp) AS dwell_time
                FROM base;
            """)

            # 4) Para cada franja: filtra por ventana, aplica dwell_time y agrega OD
            for band_name, hhmmss_start, hhmmss_end in TIME_BANDS:
                ts1 = to_epoch(date_str, hhmmss_start)
                ts2 = to_epoch(date_str, hhmmss_end)
                if ts1 > ts2:
                    logging.warning(f"   Franja {band_name} inválida (inicio > fin), se omite.")
                    continue

                logging.info(f"   -> {band_name}: {hhmmss_start}-{hhmmss_end}, dwell>{dwell_time}s")

                con.execute(f"""
                    CREATE OR REPLACE TABLE _tmp_band AS
                    SELECT source_mun, target_mun
                    FROM {tbl_tmp1}
                    WHERE target_mun IS NOT NULL
                      AND source_mun <> target_mun
                      AND dwell_time > {dwell_time}
                      AND utc_timestamp >= {ts1}
                      AND utc_timestamp <= {ts2};
                """)

                df_od = con.execute(f"""
                    SELECT
                        source_mun AS source,
                        target_mun AS target,
                        (COUNT(*) * 1000000.0) / {total_caid} AS w
                    FROM _tmp_band
                    GROUP BY source_mun, target_mun;
                """).df()

                # 5) Exporta CSV por franja
                fname = "_".join([
                    base_name,
                    date_str.replace("-", "_"),
                    band_name
                ]) + ".csv"
                fpath = os.path.join(out_dir, fname)
                df_od.to_csv(fpath, index=False)
                logging.info(f"      CSV: {fpath} (aristas: {len(df_od)})")

        elapsed = time.perf_counter() - t0
        logger.info("FINISHED in %.2f seconds." % elapsed)

def exit_program():
    print("Exiting the program...")
    sys.exit(0)

if __name__ == "__main__":
    Main().run()

