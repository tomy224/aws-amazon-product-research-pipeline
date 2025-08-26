import boto3
import csv
import io
import os
from datetime import datetime

s3 = boto3.client("s3")

# 環境変数
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
INPUT_KEY = "input/keepa_seller_asin.csv"
CHUNK_SIZE = 1000  # 1チャンクあたりのASIN件数

def lambda_handler(event, context):
    # 実行日で出力ディレクトリ名を作成
    today = datetime.utcnow().strftime("%Y-%m-%d")
    input_prefix = f"input/{today}/"

    # 元CSVを読み込み
    response = s3.get_object(Bucket=BUCKET_NAME, Key=INPUT_KEY)
    content = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.reader(content)
    rows = list(reader)

    if not rows or len(rows) <= 1:
        return {"message": "データがありません"}

    header = rows[0]
    data_rows = rows[1:]

    chunk_keys = []
    for i in range(0, len(data_rows), CHUNK_SIZE):
        chunk_rows = data_rows[i:i+CHUNK_SIZE]
        if not chunk_rows:
            continue
        chunk_csv = io.StringIO()
        writer = csv.writer(chunk_csv)
        writer.writerow(header)
        writer.writerows(chunk_rows)
        chunk_content = chunk_csv.getvalue()

        chunk_id = f"{i // CHUNK_SIZE + 1:03}"
        chunk_key = f"{input_prefix}chunk_{chunk_id}/asin_list.csv"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=chunk_key,
            Body=chunk_content.encode("utf-8"),
            ContentType="text/csv"
        )
        chunk_keys.append(chunk_key)

    return {
        "message": f"{len(chunk_keys)} チャンクを作成しました",
        "chunks": chunk_keys,
        "input_prefix": input_prefix
    }
