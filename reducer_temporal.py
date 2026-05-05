#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

# provincia -> año -> total acumulado
datos = defaultdict(lambda: defaultdict(int))

for line in sys.stdin:
    line = line.strip()
    if not line or "COD_PROVINCIA" in line:
        continue
    fields = line.split('|')
    if len(fields) < 5:
        continue
    provincia = fields[0].strip()
    anio_raw = fields[3].strip()
    if anio_raw == "DESC_ANTIG_PERMISO":
        continue
    # Anterior_2014 lo ponemos como 2013
    anio = 2013 if anio_raw == "Anterior_2014" else int(anio_raw)
    try:
        cantidad = int(fields[4])
        datos[provincia][anio] += cantidad
    except ValueError:
        continue

# Convertir a acumulativo por año
years = list(range(2013, 2027))
output = []
for provincia, anios in datos.items():
    acumulado = 0
    for year in years:
        acumulado += anios.get(year, 0)
        output.append({
            "provincia": provincia,
            "year": year,
            "conductores": acumulado
        })

print(json.dumps(output, ensure_ascii=False))
