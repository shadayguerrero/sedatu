#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import time
import datetime
import pandas as pd
import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(asctime)s:%(message)s")
logger = logging.getLogger(__name__)


class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date",
            help="Start date of 24 hour period of observational unit. Format: yyyy-mm-dd",
            required=True,
        )
        parser.add_argument(
            "-t", "--time",
            help="Timelapse to consider for network. Format: hh:mm-hh:mm (24h)",
            required=True,
        )
        parser.add_argument(
            "-l", "--location",
            help="CSV con los AGEBs a considerar. Debe tener la columna CVEGEO",
            required=True,
        )
        parser.add_argument(
            "-w", "--dwell",
            help="Umbral de dwell time (segundos) para considerar una arista",
            required=True,
            type=int,
        )
        parser.add_argument(
            "-db", "--dataset",
            help="Directorio donde están los parquet del dataset",
            required=True,
        )
        parser.add_argument(
            "-sf", "--suffix",
            help="Sufijo para usar en los archivos de salida",
            default="",
        )
        parser.add_argument(
            "-o", "--output",
            help="Directorio de salida para las redes",
            required=True,
        )
        self.args = parser.parse_args()

    def run(self):
        time_start = time.perf_counter()
        logging.info("Executing main")

        # ------- Parámetros y validaciones -------
        date = self.args.date  # yyyy-mm-dd
        year, month, day = date.split("-")

        timew = self.args.time  # hh:mm-hh:mm
        time1_s, time2_s = timew.split("-")

        # Validación de ventana de tiempo
        t1 = datetime.datetime.strptime(f"{date} {time1_s}", "%Y-%m-%d %H:%M")
        t2 = datetime.datetime.strptime(f"{date} {time2_s}", "%Y-%m-%d %H:%M")
        time1 = time.mktime(t1.timetuple())
        time2 = time.mktime(t2.timetuple())
        if time1 > time2:
            print("Timelapse no válido: la hora final es anterior a la inicial.")
            exit_program()

        # Entradas
        dataset_path = self.args.dataset
        dwell_time = self.args.dwell
        output_dir = self.args.output
        suffix = (self.args.suffix or "").strip()
        output_name_cvegeo = (suffix + "_" if suffix else "") + "od_cvegeo"

        # Nombre de archivo (solo CSV)
        filename_cvegeo = "_".join([output_name_cvegeo, date, timew]).replace("-", "_").replace(":", "_")

        logging.info(f"OD CVEGEO | fecha: {date} | ventana: {timew} | dwell>{dwell_time}s")

        # ------- Carga de AGEBs de interés -------
        loc_df = pd.read_csv(self.args.location)
        if "CVEGEO" not in loc_df.columns:
            print("El CSV de 'location' debe contener la columna 'CVEGEO'.")
            exit_program()
        cvegeo_list = list(loc_df["CVEGEO"].astype(str).unique())
        if len(cvegeo_list) == 0:
            print("La lista de CVEGEO está vacía.")
            exit_program()

        # Formato para IN (...) de DuckDB
        cvegeo_str = "'" + "','".join(cvegeo_list) + "'"

        # ------- Lectura/filtrado parquet por fecha y ventana -------
        dataset_by_date = f"{dataset_path}/year={year}/month={month}/day={day}/*"
        table_name = "movement_geoenriched"

        con = duckdb.connect()
        con.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT *
            FROM read_parquet('{dataset_by_date}')
            WHERE (utc_timestamp >= {time1} AND utc_timestamp <= {time2})
              AND cvegeo IN ({cvegeo_str});
            """
        )
        logging.info("Tabla base creada")

        # ------- Conteo de dispositivos (denominador de la ponderación) -------
        dfc = con.execute(
            f"SELECT count(DISTINCT caid) AS total_caid FROM {table_name}"
        ).df()
        total_caid = int(dfc["total_caid"][0])
        if total_caid == 0:
            print("El número total de dispositivos es cero tras el filtrado.")
            exit_program()
        logging.info(f"Total de dispositivos: {total_caid}")

        # ------- Construcción de transiciones por CVEGEO -------
        # Usamos una CTE para claridad y evitar referenciar alias en la misma SELECT.
        con.execute(
            f"""
            CREATE OR REPLACE TABLE _tmp1 AS
            WITH base AS (
                SELECT
                    caid,
                    cvegeo AS source_cvegeo,
                    utc_timestamp,
                    LAG(cvegeo, -1) OVER (PARTITION BY caid ORDER BY utc_timestamp) AS target_cvegeo,
                    LAG(utc_timestamp, -1) OVER (PARTITION BY caid ORDER BY utc_timestamp) AS target_time
                FROM {table_name}
            )
            SELECT
                caid,
                source_cvegeo,
                target_cvegeo,
                utc_timestamp,
                target_time,
                (target_time - utc_timestamp) AS dwell_time
            FROM base;
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE _tmp3_cvegeo AS
            SELECT
                source_cvegeo,
                target_cvegeo,
                caid,
                utc_timestamp,
                target_time,
                dwell_time
            FROM _tmp1
            WHERE target_cvegeo IS NOT NULL
              AND source_cvegeo <> target_cvegeo
              AND dwell_time > {dwell_time};
            """
        )
        logging.info("Transiciones CVEGEO construidas")

        # ------- Agregación: matriz OD por CVEGEO (peso normalizado por millón) -------
        df_cvegeo = con.execute(
            f"""
            SELECT
                source_cvegeo AS source,
                target_cvegeo AS target,
                (COUNT(*) * 1000000.0) / {total_caid} AS w
            FROM _tmp3_cvegeo
            GROUP BY source_cvegeo, target_cvegeo;
            """
        ).df()
        logging.info("DataFrame OD por CVEGEO listo")

        # ------- Exportar solo CSV -------
        os.makedirs(output_dir, exist_ok=True)
        output_csv = os.path.join(output_dir, filename_cvegeo + ".csv")
        df_cvegeo.to_csv(output_csv, index=False)
        logging.info(f"CSV escrito: {output_csv}")

        # ------- Fin -------
        elapsed = time.perf_counter() - time_start
        logger.info("FINISHED in %.2f seconds." % elapsed)


def exit_program():
    print("Exiting the program...")
    sys.exit(0)


if __name__ == "__main__":
    m = Main()
    m.run()

