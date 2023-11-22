#!/bin/bash

# Archivo JSON con la configuración de los trabajos
JOBS_FILE="glue-jobs/raw/bulk/jobs-config/jobs.json"

# Leer cada trabajo del archivo JSON y crearlo
cat "$JOBS_FILE" | python -c '
import json
import sys
import subprocess

# Leer el JSON desde la entrada estándar
jobs = json.load(sys.stdin)

# Iterar sobre cada trabajo y ejecutar el comando aws glue create-job
for job in jobs:

    json_str = json.dumps(job)
    subprocess.run(["aws", "glue", "create-job", "--cli-input-json", json_str])

    # Iniciar el trabajo
    start_job_cmd = ["aws", "glue", "start-job-run", "--job-name", job["Name"]]
    subprocess.run(start_job_cmd)
'
