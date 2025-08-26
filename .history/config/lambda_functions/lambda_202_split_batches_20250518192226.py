import boto3
import csv
import io
import os

s3 = boto3.client("s3")

# 設定
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
DEFAULT_BATCH_SIZE = 20  # デフォルトのバッチサイズ

def lambda_handler(event, context):
    input_key = event.get("input_key")
    if not input_key:
        raise ValueError("event に 'input_key' が含まれていません")
    
    # バッチサイズをイベントから取得（パラメータがなければデフォルト値を使用）
    batch_size = int(event.get("batch_size", DEFAULT_BATCH_SIZE))

    # chunkのパスを抽出（例: input/2025-05-12/chunk_001/）
    chunk_prefix = "/".join(input_key.split("/")[:-1]) + "/"

    # S3からCSVを読み込み
    response = s3.get_object(Bucket=BUCKET_NAME, Key=input_key)
    content = response["Body"].read().decode("utf-8").splitlines()
    reader = csv.reader(content)
    rows = list(reader)

    if not rows or len(rows) <= 1:
        return {"message": "データがありません", "batches": [], "batch_data": []}

    header = rows[0]
    data_rows = rows[1:]
    
    # ASIN列のインデックスを特定（通常は0列目か「ASIN」という名前の列）
    asin_index = 0  # デフォルトは最初の列
    if "ASIN" in header:
        asin_index = header.index("ASIN")
    
    batches = []
    batch_data = []  # ここが新しい：ファイルパスとASINリストのペアを保持
    
    for i in range(0, len(data_rows), batch_size):
        batch_rows = data_rows[i:i+batch_size]
        if not batch_rows:
            continue
            
        # バッチ内のASINリストを抽出
        batch_asins = [row[asin_index] for row in batch_rows if len(row) > asin_index]
        
        batch_csv = io.StringIO()
        writer = csv.writer(batch_csv)
        writer.writerow(header)
        writer.writerows(batch_rows)
        batch_content = batch_csv.getvalue()

        batch_id = f"{i // batch_size + 1:03}"
        batch_key = f"{chunk_prefix}batch_{batch_id}.csv"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=batch_key,
            Body=batch_content.encode("utf-8"),
            ContentType="text/csv"
        )
        
        batches.append(batch_key)
        
        # ファイルパスとASINリストのペアを保存
        batch_data.append({
            "file_path": batch_key,
            "asins": batch_asins
        })

    return {
        "message": f"{len(batches)} 個のバッチファイルを作成しました",
        "batches": batches,  # 後方互換性のため
        "batch_data": batch_data,  # 新しい：ファイルパスとASINリストのペア
        "chunk_prefix": chunk_prefix,
        "input_key": input_key,
        "batch_size": batch_size
    }



# import boto3
# import csv
# import io
# import os

# s3 = boto3.client("s3")

# # 設定
# BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
# DEFAULT_BATCH_SIZE = 20  # デフォルトのバッチサイズ

# def lambda_handler(event, context):
#     input_key = event.get("input_key")
#     if not input_key:
#         raise ValueError("event に 'input_key' が含まれていません")
    
#     # バッチサイズをイベントから取得（パラメータがなければデフォルト値を使用）
#     batch_size = int(event.get("batch_size", DEFAULT_BATCH_SIZE))

#     # chunkのパスを抽出（例: input/2025-05-12/chunk_001/）
#     chunk_prefix = "/".join(input_key.split("/")[:-1]) + "/"

#     # S3からCSVを読み込み
#     response = s3.get_object(Bucket=BUCKET_NAME, Key=input_key)
#     content = response["Body"].read().decode("utf-8").splitlines()
#     reader = csv.reader(content)
#     rows = list(reader)

#     if not rows or len(rows) <= 1:
#         return {"message": "データがありません", "batches": []}

#     header = rows[0]
#     data_rows = rows[1:]

#     batches = []
#     for i in range(0, len(data_rows), batch_size):  # ここでbatch_sizeを使用
#         batch_rows = data_rows[i:i+batch_size]
#         if not batch_rows:
#             continue
#         batch_csv = io.StringIO()
#         writer = csv.writer(batch_csv)
#         writer.writerow(header)
#         writer.writerows(batch_rows)
#         batch_content = batch_csv.getvalue()

#         batch_id = f"{i // batch_size + 1:03}"
#         batch_key = f"{chunk_prefix}batch_{batch_id}.csv"

#         s3.put_object(
#             Bucket=BUCKET_NAME,
#             Key=batch_key,
#             Body=batch_content.encode("utf-8"),
#             ContentType="text/csv"
#         )
#         batches.append(batch_key)

#     return {
#         "message": f"{len(batches)} 個のバッチファイルを作成しました",
#         "batches": batches,
#         "chunk_prefix": chunk_prefix,
#         "input_key": input_key,  # 入力ファイル情報も返しておくと便利
#         "batch_size": batch_size  # 使用したバッチサイズも返す
#     }





