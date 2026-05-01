"""
lambda_function.py  –  AWS Lambda Handler
Triggered por un evento S3 cuando se sube el fichero de censo.

Flujo completo:
  1. Fichero CSV sube a  s3://BUCKET/input/censo.txt
  2. S3 Event Notification dispara esta Lambda
  3. Lambda descarga el CSV a /tmp
  4. Ejecuta mapper.py | sort | reducer.py en subprocesos
  5. Sube el JSON resultado a  s3://BUCKET/output/resultados_mapreduce.json
  6. El dashboard Streamlit lee desde S3

Despliegue:
  aws lambda create-function \
    --function-name censo-conductores-mapreduce \
    --runtime python3.12 \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::ACCOUNT:role/lambda-s3-role \
    --zip-file fileb://function.zip \
    --timeout 300 \
    --memory-size 1024
"""

import os
import json
import subprocess
import sys
import boto3

S3_OUTPUT_KEY = "output/resultados_mapreduce.json"
TMP_INPUT  = "/tmp/censo.txt"
TMP_MAPPER  = "/tmp/mapper.py"
TMP_REDUCER = "/tmp/reducer.py"

s3 = boto3.client("s3")


def download_scripts(bucket: str):
    """Descarga mapper.py y reducer.py desde S3 a /tmp."""
    for script in ["mapper.py", "reducer.py"]:
        s3.download_file(bucket, f"scripts/{script}", f"/tmp/{script}")
    os.chmod(TMP_MAPPER,  0o755)
    os.chmod(TMP_REDUCER, 0o755)


def run_mapreduce(input_path: str) -> dict:
    """Ejecuta el pipeline mapper | sort | reducer y devuelve el JSON."""
    mapper_proc = subprocess.Popen(
        [sys.executable, TMP_MAPPER],
        stdin=open(input_path, encoding="utf-8"),
        stdout=subprocess.PIPE,
    )
    sort_proc = subprocess.Popen(
        ["sort"],
        stdin=mapper_proc.stdout,
        stdout=subprocess.PIPE,
    )
    mapper_proc.stdout.close()

    reducer_proc = subprocess.Popen(
        [sys.executable, TMP_REDUCER],
        stdin=sort_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    sort_proc.stdout.close()

    stdout, stderr = reducer_proc.communicate()
    if reducer_proc.returncode != 0:
        raise RuntimeError(f"Reducer falló: {stderr.decode()}")

    return json.loads(stdout.decode("utf-8"))


def lambda_handler(event, context):
    """
    Entry point de AWS Lambda.
    El evento S3 tiene la forma:
      { "Records": [{ "s3": { "bucket": {"name": "..."}, "object": {"key": "..."} } }] }
    """
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key    = record["object"]["key"]

    print(f"📂 Procesando s3://{bucket}/{key}")

    # 1. Descargar datos
    s3.download_file(bucket, key, TMP_INPUT)

    # 2. Descargar scripts MapReduce desde S3
    download_scripts(bucket)

    # 3. Ejecutar pipeline
    resultados = run_mapreduce(TMP_INPUT)

    # 4. Subir resultado a S3
    s3.put_object(
        Bucket=bucket,
        Key=S3_OUTPUT_KEY,
        Body=json.dumps(resultados, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    total = sum(resultados.get("provincia", {}).values())
    print(f"✅ Resultado subido a s3://{bucket}/{S3_OUTPUT_KEY}")
    print(f"   Total conductores procesados: {total:,}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensaje": "MapReduce completado",
            "total_conductores": total,
            "output_s3": f"s3://{bucket}/{S3_OUTPUT_KEY}",
        })
    }
