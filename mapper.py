#!/usr/bin/env python3
"""
mapper.py - Mapper de Python Streaming para Hadoop MapReduce
Censo de Conductores de España

Entrada (stdin): líneas del CSV con formato:
  COD_PROVINCIA|IND_SEXO|CLASE_PERMISO|DESC_ANTIG_PERMISO|NUM_CONDUCTORES

Salida (stdout): pares clave-valor separados por tabulador
  Emite múltiples claves para distintos análisis:
    - provincia\tconductores         → total por provincia
    - sexo\tconductores              → total por sexo
    - permiso\tconductores           → total por clase de permiso
    - provincia_sexo\tconductores    → combinación provincia+sexo
    - anio\tconductores              → tendencia temporal por año
"""

import sys

def map_line(line):
    """Procesa una línea y emite pares clave-valor."""
    line = line.strip()
    # Saltamos cabecera y líneas vacías
    if not line or line.startswith("COD_PROVINCIA"):
        return

    campos = line.split("|")
    if len(campos) != 5:
        return

    provincia    = campos[0].strip()
    sexo         = campos[1].strip()
    permiso      = campos[2].strip()
    antiguedad   = campos[3].strip()
    try:
        num_conductores = int(campos[4].strip())
    except ValueError:
        return

    # ─── Clave 1: total por provincia ───────────────────────────
    print(f"provincia\t{provincia}\t{num_conductores}")

    # ─── Clave 2: total por sexo ────────────────────────────────
    print(f"sexo\t{sexo}\t{num_conductores}")

    # ─── Clave 3: total por clase de permiso ────────────────────
    print(f"permiso\t{permiso}\t{num_conductores}")

    # ─── Clave 4: provincia + sexo (distribución geográfica) ───
    print(f"provincia_sexo\t{provincia}_{sexo}\t{num_conductores}")

    # ─── Clave 5: evolución temporal por año ────────────────────
    print(f"anio\t{antiguedad}\t{num_conductores}")

    # ─── Clave 6: permiso + sexo ─────────────────────────────────
    print(f"permiso_sexo\t{permiso}_{sexo}\t{num_conductores}")

def main():
    for line in sys.stdin:
        map_line(line)

if __name__ == "__main__":
    main()
