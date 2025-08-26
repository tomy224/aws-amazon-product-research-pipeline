# lambda_207_merge_filtered_files.py
import boto3
import os
import pandas as pd
import io

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")

def lambda_handler(event, context):
    """
    event = {
        "chunk_path": "input/2025-05-15/chunk_001",  # チャンクの親パス
        "filtered_files": [
            "output/2025-05-15/chunk_001/batch_1_result_pricing_filtered.csv",
            "output/2025-05-15/chunk_001/batch_2_result_pricing_filtered.csv"
        ]
    }
    """
    # イベントからデータを取得
    chunk_path = event.get("chunk_path", "")  # デフォルト値を空文字列に設定
    filtered_files = event.get("filtered_files", [])
    
    if not chunk_path or not filtered_files:
        return {
            "statusCode": 400,
            "message": "chunk_path または filtered_files が指定されていません"
        }
    
    # この時点で chunk_path は None ではないことを保証済み
    # 末尾のスラッシュを削除
    chunk_path = chunk_path.rstrip('/')
    
    # 出力ファイル名の設定
    merged_file = f"{chunk_path.replace('input/', 'output/')}/merged_pricing_filtered.csv"
    input_merged_file = merged_file.replace("output/", "input/")
    
    # 各ファイルの読み込みと統合
    all_data = []
    
    for file_path in filtered_files:
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=file_path)
            content = response["Body"].read()
            df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig")
            all_data.append(df)
        except Exception as e:
            print(f"ファイル読み込みエラー {file_path}: {str(e)}")
    
    if not all_data:
        return {
            "statusCode": 400,
            "message": "読み込み可能なファイルがありませんでした"
        }
    
    # データフレームの結合
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # 重複削除（必要に応じて）
    if "JAN" in merged_df.columns:
        merged_df = merged_df.drop_duplicates(subset=["JAN"])
    
    # CSVとして保存
    csv_buffer = io.StringIO()
    merged_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    
    # S3に保存
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=merged_file,
        Body=csv_buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv"
    )
    
    # 入力フォルダにもコピー
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=input_merged_file,
        Body=csv_buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv"
    )
    
    # JAN一覧を抽出
    jan_codes = merged_df["JAN"].dropna().unique().tolist() if "JAN" in merged_df.columns else []
    
    # JAN→ASINマッピングを作成
    jan_asin_map = {}
    for _, row in merged_df.iterrows():
        if 'JAN' in row and 'ASIN' in row and pd.notna(row['JAN']) and pd.notna(row['ASIN']):
            jan_asin_map[str(row['JAN'])] = row['ASIN']

    return {
        "statusCode": 200,
        "message": f"{len(filtered_files)}個のファイルを統合しました",
        "merged_file": merged_file,
        "input_merged_file": input_merged_file,
        "total_records": len(merged_df),
        "jan_codes": merged_df["JAN"].dropna().unique().tolist() if "JAN" in merged_df.columns else [],
        "jan_asin_map": jan_asin_map  # JAN→ASINマッピングを追加
    }