import time

import boto3
import pandas as pd

DATABASE = "foodlens"
OUTPUT = "s3://foodlens-ni-2026/athena-results/"
REGION = "us-east-1"

athena = boto3.client("athena", region_name=REGION)

def run_query(sql: str) -> pd.DataFrame:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
        WorkGroup="primary",
    )

    query_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(status)

        time.sleep(1)

    paginator = athena.get_paginator("get_query_results")

    rows = []

    for page in paginator.paginate(QueryExecutionId=query_id):
        rows.extend(page["ResultSet"]["Rows"])

    headers = [c["VarCharValue"] for c in rows[0]["Data"]]

    data = []

    for row in rows[1:]:
        data.append(
            [
                c.get("VarCharValue", "")
                for c in row["Data"]
            ]
        )

    return pd.DataFrame(data, columns=headers)