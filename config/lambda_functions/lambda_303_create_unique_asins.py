import boto3
import csv
import os
import json

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
MAX_ASINS_PER_BATCH = 20  # Keepa APIで一度に処理するASIN数

def lambda_handler(event, context):
    """
    event = {
        "input_key": "input/xxx/batch_001_yahoraku.csv"
    }
    """
    input_key = event.get("input_key")
    if not input_key:
        return {"statusCode": 400, "message": "input_key が指定されていません"}

    # 出力ファイルパス
    output_key = input_key.replace(".csv", "_unique_asins.csv")
    
    # 一時ファイル
    tmp_input = "/tmp/input.csv"
    tmp_output = "/tmp/output.csv"
    
    # 入力CSVを読み込む
    s3.download_file(BUCKET_NAME, input_key, tmp_input)
    
    # ASINの一意リストを作成
    unique_asins = set()
    with open(tmp_input, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'ASIN' in row and row['ASIN']:
                unique_asins.add(row['ASIN'])
    
    asin_list = list(filter(None, unique_asins))  # 空文字を除外
    
    # ASINリストを出力
    with open(tmp_output, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['ASIN'])
        for asin in asin_list:
            writer.writerow([asin])
    
    # S3にアップロード
    s3.upload_file(tmp_output, BUCKET_NAME, output_key)
    
    # Keepa API呼び出し用にASINをバッチに分割
    asin_batches = []
    for i in range(0, len(asin_list), MAX_ASINS_PER_BATCH):
        batch = asin_list[i:i + MAX_ASINS_PER_BATCH]
        asin_batches.append(batch)
    
    return {
        "statusCode": 200,
        "message": f"{len(asin_list)}件の一意ASINを抽出しました",
        "input_key": input_key,
        "output_key": output_key,
        "asin_count": len(asin_list),
        "asin_batch": asin_batches  # Step Functionsで各バッチを処理するために返す
    }