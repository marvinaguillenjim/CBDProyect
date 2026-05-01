#!/usr/bin/env python3
"""
run_mapreduce.py - Simulación local del pipeline MapReduce
Equivale a ejecutar en Hadoop:
  cat datos.txt | python3 mapper.py | sort | python3 reducer.py > resultados.json

En AWS con Hadoop Streaming sobre EMR sería:
  aws emr add-steps --cluster-id j-XXXXX \
    --steps Type=STREAMING, \
      Args=[-input,s3://bucket/censo.txt, \
            -output,s3://bucket/output/, \
            -mapper,s3://bucket/mapper.py, \
            -reducer,s3://bucket/reducer.py]

Este script genera 'resultados_mapreduce.json' que consume el dashboard Streamlit.
"""

import subprocess
import json
import os
import sys

DATA_FILE   = "2026_1_Nconductores_censo_conductores_clases_antiguedad202601.txt"
OUTPUT_FILE = "resultados_mapreduce.json"

def run_pipeline(data_path: str) -> dict:
    """Ejecuta mapper | sort | reducer como subprocesos, igual que Hadoop Streaming."""
    print("▶  Fase MAP  ...")
    mapper_proc = subprocess.Popen(
        [sys.executable, "mapper.py"],
        stdin=open(data_path, encoding="utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    print("▶  Fase SHUFFLE/SORT  ...")
    sort_proc = subprocess.Popen(
        ["sort"],
        stdin=mapper_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    mapper_proc.stdout.close()

    print("▶  Fase REDUCE  ...")
    reducer_proc = subprocess.Popen(
        [sys.executable, "reducer.py"],
        stdin=sort_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    sort_proc.stdout.close()

    stdout, stderr = reducer_proc.communicate()

    if reducer_proc.returncode != 0:
        print(f"❌ Error en reducer: {stderr.decode()}")
        sys.exit(1)

    return json.loads(stdout.decode("utf-8"))

def main():
    # Buscar el fichero de datos en el directorio actual o en uploads
    data_path = DATA_FILE
    if not os.path.exists(data_path):
        data_path = os.path.join(
            "/mnt/user-data/uploads", DATA_FILE
        )
    if not os.path.exists(data_path):
        print(f"❌ No se encontró el fichero de datos: {DATA_FILE}")
        sys.exit(1)

    print(f"📂 Procesando: {data_path}")
    resultados = run_pipeline(data_path)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Resultados guardados en '{OUTPUT_FILE}'")

    # Resumen rápido
    for clave, datos in resultados.items():
        total = sum(datos.values())
        print(f"   {clave:20s} → {len(datos):4d} claves únicas  |  {total:,} conductores totales")

if __name__ == "__main__":
    main()
