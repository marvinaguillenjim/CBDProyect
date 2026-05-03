#!/usr/bin/python3
import sys

# Formato entrada: COD_PROVINCIA|IND_SEXO|CLASE_PERMISO|DESC_ANTIG_PERMISO|NUM_CONDUCTORES
for line in sys.stdin:
    line = line.strip()
    if not line or "COD_PROVINCIA" in line:
        continue
    
    fields = line.split('|')
    
    if len(fields) >= 5:
        provincia = fields[0]
        sexo = fields[1]
        antiguedad = fields[3]
        try:
            cantidad = int(fields[4])
            # Emitimos 3 campos para que el Reducer sea feliz
            print("{0}\t{1}\t{2}".format("provincia", provincia, cantidad))
            print("{0}\t{1}\t{2}".format("sexo", sexo, cantidad))
            print("{0}\t{1}\t{2}".format("antiguedad", antiguedad, cantidad))
        except ValueError:
            continue