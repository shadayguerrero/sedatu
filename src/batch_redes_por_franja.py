import os
import subprocess
from datetime import datetime, timedelta
import argparse

def ejecutar_redes_por_dia(start_date, end_date, dataset, output_dir, script_path="redes_por_franja_municipio.py"):
    current = start_date
    while current <= end_date:
        fecha_str = current.strftime("%Y-%m-%d")
        print(f"🚀 Ejecutando para: {fecha_str}")
        try:
            subprocess.run([
                "python", script_path,
                "-d", fecha_str,
                "-db", dataset,
                "-o", output_dir
            ], check=True)
        except subprocess.CalledProcessError:
            print(f"❌ Error al procesar {fecha_str}")
        current += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", required=True, help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("-e", "--end", required=True, help="Fecha fin (YYYY-MM-DD)")
    parser.add_argument("-db", "--dataset", required=True, help="Ruta a los datos parquet")
    parser.add_argument("-o", "--output", required=True, help="Ruta para guardar las salidas")
    parser.add_argument("--script", default="redes_por_franja_municipio.py", help="Ruta al script de redes por franja")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    ejecutar_redes_por_dia(start_date, end_date, args.dataset, args.output, args.script)

