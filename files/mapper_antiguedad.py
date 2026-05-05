#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    fields = line.split('|')
    
    if len(fields) >= 5:
        antiguedad = fields[3]
        cantidad = fields[4]
        # Salida: Clave (Año/Antigüedad) <tab> Valor
        print(f"{antiguedad}\t{cantidad}")
