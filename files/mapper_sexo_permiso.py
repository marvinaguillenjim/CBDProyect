#!/usr/bin/python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    fields = line.split('|')
    
    if len(fields) >= 5:
        sexo = fields[1]
        permiso = fields[2]
        cantidad = fields[4]
        # Salida: Clave compuesta (Sexo_Permiso) <tab> Valor
        print(f"{sexo}_{permiso}\t{cantidad}")