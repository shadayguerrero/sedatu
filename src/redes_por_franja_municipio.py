import os
import duckdb
import argparse

FRANJAS = {
    "madrugada": ("00:00:01", "06:00:00"),
    "mañana":    ("06:00:01", "10:00:00"),
    "mediodia":  ("10:00:01", "14:00:00"),
    "tarde":     ("14:00:01", "17:00:00"),
    "noche":     ("17:00:01", "21:00:00"),
    "medianoche":("21:00:01", "23:59:59"),
}

def generar_red_por_franja(date, dataset, output_dir):
    year, month, day = date.split("-")
    parquet_path = f"{dataset}/year={year}/month={month}/day={day}/*"
    os.makedirs(output_dir, exist_ok=True)

    con = duckdb.connect()

    # Leer datos y construir clave de municipio
    con.execute(f"""
        CREATE TABLE dia AS
        SELECT 
            caid,
            to_timestamp(utc_timestamp) AS ts,
            CAST(cve_ent AS VARCHAR) || LPAD(CAST(cve_mun AS VARCHAR), 3, '0') AS cve_mun_full
        FROM read_parquet('{parquet_path}')
        WHERE cve_ent IS NOT NULL AND cve_mun IS NOT NULL
    """)

    for franja, (inicio, fin) in FRANJAS.items():
        print(f"⏱ Procesando franja: {franja}")

        # Filtrar por franja horaria
        con.execute(f"""
            CREATE OR REPLACE TABLE franja AS
            SELECT *
            FROM dia
            WHERE ts BETWEEN TIMESTAMP '{date} {inicio}' AND TIMESTAMP '{date} {fin}'
        """)

        # Obtener origen (primer ping) y destino (último ping)
        con.execute("""
            CREATE OR REPLACE TABLE od AS (
                WITH ordenado AS (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY caid ORDER BY ts ASC) AS rn_asc,
                           ROW_NUMBER() OVER (PARTITION BY caid ORDER BY ts DESC) AS rn_desc
                    FROM franja
                )
                SELECT 
                    origen.caid,
                    origen.cve_mun_full AS source,
                    destino.cve_mun_full AS target
                FROM 
                    (SELECT * FROM ordenado WHERE rn_asc = 1) AS origen
                    JOIN
                    (SELECT * FROM ordenado WHERE rn_desc = 1) AS destino
                    ON origen.caid = destino.caid
            )
        """)

        # Obtener municipios únicos observados
        con.execute("""
            CREATE OR REPLACE TABLE municipios AS (
                SELECT DISTINCT cve_mun_full FROM franja
            )
        """)

        # Producto cartesiano de municipios (source, target)
        con.execute("""
            CREATE OR REPLACE TABLE todas_las_combinaciones AS (
                SELECT a.cve_mun_full AS source, b.cve_mun_full AS target
                FROM municipios a CROSS JOIN municipios b
            )
        """)

        # Conteo de movimientos observados
        con.execute("""
            CREATE OR REPLACE TABLE movimientos AS (
                SELECT source, target, COUNT(*) AS peso
                FROM od
                GROUP BY source, target
            )
        """)

        # Red completa con todos los pares posibles
        con.execute("""
            CREATE OR REPLACE TABLE red AS (
                SELECT
                    todas.source,
                    todas.target,
                    COALESCE(movimientos.peso, 0) AS peso
                FROM todas_las_combinaciones todas
                LEFT JOIN movimientos ON
                    todas.source = movimientos.source AND
                    todas.target = movimientos.target
            )
        """)

        # Exportar CSV
        output_file = os.path.join(output_dir, f"red_municipal_{franja}_{date.replace('-', '_')}.csv")
        con.execute(f"COPY red TO '{output_file}' (FORMAT CSV, HEADER)")
        print(f"✅ Guardado: {output_file}")

    con.close()

# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date", required=True, help="Fecha (YYYY-MM-DD)")
    parser.add_argument("-db", "--dataset", required=True, help="Ruta base a los parquet")
    parser.add_argument("-o", "--output", required=True, help="Directorio de salida")
    args = parser.parse_args()

    generar_red_por_franja(args.date, args.dataset, args.output)

