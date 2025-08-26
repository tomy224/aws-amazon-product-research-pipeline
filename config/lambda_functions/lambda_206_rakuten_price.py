import boto3
import csv
import os
from modules.apis.rakuten_api import RakutenAPI

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "3"))

def lambda_handler(event, context):
    """
    event = {
        "input_key": "input/xxx/batch_001_result_pricing_filtered.csv",
        "jan_code": "4901234567890"
    }
    """
    input_key = event.get("input_key")
    jan_code = event.get("jan_code")
    if not input_key or not jan_code:
        return {"statusCode": 400, "message": "input_key または jan_code が指定されていません"}

    output_key = input_key.replace("input/", "output/").replace("_pricing_filtered.csv", "_yahoraku.csv") 

    # 一時ファイル
    tmp_output = "/tmp/rakuten_result.csv"
    tmp_input = "/tmp/input.csv"
    
    # 入力CSVから対応するASINを取得
    try:
        s3.download_file(BUCKET_NAME, input_key, tmp_input)
        
        # JAN→ASINのマッピング作成
        asin = ""
        with open(tmp_input, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'JAN' in row and 'ASIN' in row and row['JAN'] == jan_code:
                    asin = row['ASIN']
                    break
    except Exception as e:
        print(f"入力ファイル読み込みエラー: {str(e)}")
        asin = ""  # エラー時は空文字に設定

    # Rakuten API 呼び出し
    app_id = os.environ.get("RAKUTEN_APPLICATION_ID")
    rakuten = RakutenAPI(app_id)
    results = rakuten.search_by_jan(jan_code, max_items=MAX_ITEMS)

    if not results:
        return {
            "statusCode": 200,
            "message": f"JAN {jan_code} の商品は見つかりませんでした",
            "jan_code": jan_code
        }

    # CSVヘッダー - ASINを追加
    headers = ['JAN', 'ASIN', 'API', '価格', '送料条件', '商品URL']

    with open(tmp_output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for item in results:
            # ASINを追加
            item['ASIN'] = asin
            writer.writerow(item)

    # 既存S3に追記 or 新規
    try:
        existing = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
        old_data = existing['Body'].read().decode('utf-8-sig')
        new_data = open(tmp_output, encoding='utf-8-sig').read()
        merged_data = old_data.strip() + "\n" + "\n".join(new_data.strip().split("\n")[1:])  # skip header
    except s3.exceptions.NoSuchKey:
        merged_data = open(tmp_output, encoding='utf-8-sig').read()

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=output_key,
        Body=merged_data.encode("utf-8-sig"),
        ContentType="text/csv"
    )

    return {
        "statusCode": 200,
        "jan_code": jan_code,
        "asin": asin,  # ASINも返すように追加
        "output_key": output_key,
        "item_count": len(results)
    }