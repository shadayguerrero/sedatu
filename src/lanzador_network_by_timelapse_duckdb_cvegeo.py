python network_by_timelapse_duckdb_cvegeo.py \
  -d 2021-01-15 \
  -t 06:00-10:00 \
  -l /ruta/agebs_interes.csv \
  -w 300 \
  -db /data/covid/aws/movement_parquet \
  -sf cdmx_edomex \
  -o /data/salidas/od
