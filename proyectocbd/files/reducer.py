#!/usr/bin/env python3
"""
reducer.py - Reducer de Python Streaming para Hadoop MapReduce
Censo de Conductores de España

Entrada (stdin): salida ordenada del mapper con formato:
  tipo_clave\tclave\tvalor

Salida (stdout): JSON con los resultados agregados por tipo de análisis
  { "provincia": {"01": 12345, ...},
    "sexo":      {"M": 99999, "V": 88888},
    ...
  }

En un clúster Hadoop real se ejecutaría:
  hadoop jar hadoop-streaming.jar \
    -input  s3://bucket/censo.txt \
    -output s3://bucket/output/ \
    -mapper mapper.py \
    -reducer reducer.py

Localmente (para pruebas):
  cat datos.txt | python3 mapper.py | sort | python3 reducer.py
"""

import sys
import json
from collections import defaultdict

def main():
    resultados = defaultdict(lambda: defaultdict(int))

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        partes = line.split("\t")
        if len(partes) != 3:
            continue

        tipo_clave, clave, valor_str = partes
        try:
            valor = int(valor_str)
        except ValueError:
            continue

        resultados[tipo_clave][clave] += valor

    # Convertir defaultdict a dict normal para serializar
    output = {k: dict(v) for k, v in resultados.items()}

    print(json.dumps(output, ensure_ascii=False))

if __name__ == "__main__":
    main()
