#!/usr/bin/python3
import sys

# Formato entrada: COD_PROVINCIA|IND_SEXO|CLASE_PERMISO|DESC_ANTIG_PERMISO|NUM_CONDUCTORES
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    fields = line.split('|')
    
    if len(fields) >= 5:
        provincia = fields[0]
        cantidad = fields[4]
        # Salida: Clave (Provincia) <tab> Valor (Cantidad)
        print(f"{provincia}\t{cantidad}")