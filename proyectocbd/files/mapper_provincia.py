#!/usr/bin/python3
import sys

# Formato entrada: COD_PROVINCIA|IND_SEXO|CLASE_PERMISO|DESC_ANTIG_PERMISO|NUM_CONDUCTORES
for line in sys.stdin:
    line = line.strip()
    
    # Saltar líneas vacías o la cabecera
    if not line or "COD_PROVINCIA" in line:
        continue
    
    fields = line.split('|')
    
    if len(fields) >= 5:
        provincia = fields[0]
        try:
            # Aseguramos que la cantidad sea un número
            cantidad = int(fields[4])
            # Formato compatible con Python 3.0+ (evita f-strings)
            print("{0}\t{1}".format(provincia, cantidad))
        except ValueError:
            # En caso de que la cantidad no sea numérica, saltar
            continue