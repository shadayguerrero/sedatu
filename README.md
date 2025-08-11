# sedatu

# Generación de redes de movilidad por franja horaria (municipio)

## Script utilizado
- **Archivo:** `redes_por_franja_municipio.py`
- **Propósito:** Construir, para un día dado, redes de movilidad dirigidas a nivel municipio desglosadas por franjas horarias. Cada CSV contiene **todas** las combinaciones `source → target` observadas en la franja, con su **peso** (número de dispositivos). Las combinaciones sin movimientos reales se incluyen con **peso = 0**.

---

## Franjas horarias consideradas
| Franja       | Inicio    | Fin       |
|--------------|-----------|-----------|
| Madrugada    | 00:00:01  | 06:00:00  |
| Mañana       | 06:00:01  | 10:00:00  |
| Medio Día    | 10:00:01  | 14:00:00  |
| Tarde        | 14:00:01  | 17:00:00  |
| Noche        | 17:00:01  | 21:00:00  |
| Media Noche  | 21:00:01  | 23:59:59  |

> Identificador municipal: `cve_mun_full = cve_ent (2 dígitos) + cve_mun (3 dígitos)` con ceros a la izquierda cuando aplique (ej. `02` + `004` → `02004`).

---

## Cómo se ejecutó

### Un solo día
```bash
python redes_por_franja_municipio.py   -d 2020-03-15   -db /data/covid/aws/movement_parquet   -o /data/shaday_data/GIT/movilidad/redes_por_franja
```

### Rango de días (batch)
```bash
python batch_redes_por_franja.py   -s 2020-03-15   -e 2020-03-18   -db /data/covid/aws/movement_parquet   -o /data/shaday_data/GIT/movilidad/redes_por_franja
```

---

## Ubicación de resultados
```
/data/shaday_data/GIT/movilidad/redes_por_franja
```

**Patrón de nombre de archivos:**
```
red_municipal_<franja>_<YYYY_MM_DD>.csv
```

**Ejemplos:**
```
red_municipal_madrugada_2020_03_15.csv
red_municipal_mañana_2020_03_15.csv
red_municipal_mediodia_2020_03_15.csv
red_municipal_tarde_2020_03_15.csv
red_municipal_noche_2020_03_15.csv
red_municipal_medianoche_2020_03_15.csv
```

---

## ¿Qué contiene cada CSV?

**Columnas:**
- `source`: municipio de origen (`cve_mun_full`).
- `target`: municipio de destino (`cve_mun_full`).
- `peso`: número de dispositivos (`caid`) que realizaron el desplazamiento `source → target` en esa franja.
  - Si no hubo desplazamientos para el par, se incluye igualmente el registro con `peso = 0` (matriz completa de pares).

**Ejemplo (formato de tabla):**

| source | target | peso |
|--------|--------|------|
| 02004  | 02007  | 15   |
| 02004  | 02004  | 0    |
| 02007  | 02004  | 3    |

---

## Resumen del procedimiento del script

1. **Lectura de pings** del día desde Parquet.
2. **Cálculo de `cve_mun_full`** como `cve_ent` (2 dígitos) + `cve_mun` (3 dígitos) con ceros a la izquierda.
3. **Filtrado por franja horaria** (una a la vez).
4. **Origen–Destino por dispositivo**:
   - Origen = **primer ping** del dispositivo en la franja.
   - Destino = **último ping** del dispositivo en la franja.
5. **Conteo de movimientos** por par `source, target`.
6. **Completar matriz de pares** (`source × target`) con `peso = 0` donde no hubo movilidad.
7. **Exportación** de un CSV por franja.

---

## Notas
- Las redes son **dirigidas**.
- El identificador municipal preserva ceros a la izquierda (formato fijo de 5 caracteres).
- Incluir pares con `peso = 0` facilita análisis matriciales, descomposiciones y visualizaciones densas.
