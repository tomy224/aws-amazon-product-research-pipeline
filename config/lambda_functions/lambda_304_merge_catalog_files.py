import boto3
import csv
import io
import os
import logging
from datetime import datetime

# S3クライアント
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    カタログバッチファイルを統合するLambda関数
    
    event = {
        "chunk_path": "input/2025-05-18/chunk_001",
        "batch_files": [
            "input/2025-05-18/chunk_001/batch_001_result.csv",
            "input/2025-05-18/chunk_001/batch_002_result.csv",
            ...
        ]
    }
    """
    
    try:
        # イベントからパラメータを取得
        chunk_path = event.get("chunk_path", "")
        batch_files = event.get("batch_files", [])
        
        if not chunk_path or not batch_files:
            logger.error("chunk_path または batch_files が指定されていません")
            return {
                "statusCode": 400,
                "message": "入力パラメータが不足しています"
            }
        
        # chunk_pathの末尾のスラッシュを削除
        chunk_path = chunk_path.rstrip('/')
        
        logger.info(f"カタログファイル統合開始: {len(batch_files)}件のファイルを処理")
        
        # 統合データを格納するリスト
        all_data = []
        header = None
        total_records = 0
        
        # 各バッチファイルを読み込んで統合
        for i, batch_file in enumerate(batch_files):
            try:
                logger.info(f"ファイル {i+1}/{len(batch_files)} を処理中: {batch_file}")
                
                # S3からCSVファイルを読み込み
                response = s3.get_object(Bucket=BUCKET_NAME, Key=batch_file)
                content = response["Body"].read().decode("utf-8")
                
                # CSVデータを解析
                csv_reader = csv.reader(io.StringIO(content))
                rows = list(csv_reader)
                
                if not rows:
                    logger.warning(f"空のファイル: {batch_file}")
                    continue
                
                # 最初のファイルからヘッダーを取得
                if header is None:
                    header = rows[0]
                    logger.info(f"ヘッダーを設定: {len(header)}列")
                
                # データ行を追加（ヘッダー行を除く）
                data_rows = rows[1:] if len(rows) > 1 else []
                all_data.extend(data_rows)
                total_records += len(data_rows)
                
                logger.info(f"ファイル {batch_file}: {len(data_rows)}件のレコードを追加")
                
            except Exception as e:
                logger.error(f"ファイル {batch_file} の処理でエラー: {str(e)}")
                # エラーが発生したファイルはスキップして続行
                continue
        
        if not all_data or not header:
            logger.error("統合できるデータが見つかりませんでした")
            return {
                "statusCode": 400,
                "message": "統合できるデータが見つかりませんでした"
            }
        
        # 統合CSVファイルを作成
        output_csv = io.StringIO()
        csv_writer = csv.writer(output_csv)
        
        # ヘッダーを書き込み
        csv_writer.writerow(header)
        
        # 全データを書き込み
        csv_writer.writerows(all_data)
        
        # 出力ファイルのパスを設定
        merged_file = f"{chunk_path.replace('input/', 'output/')}/merged_catalog.csv"
        input_merged_file = merged_file.replace("output/", "input/")
        
        # S3に統合ファイルを保存（output/に保存）
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=merged_file,
            Body=output_csv.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
        
        # input/にもコピー（後続処理用）
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': merged_file},
            Key=input_merged_file
        )
        
        logger.info(f"カタログファイル統合完了: {total_records}件のレコードを統合")
        logger.info(f"出力ファイル: {merged_file}")
        logger.info(f"入力用ファイル: {input_merged_file}")
        
        return {
            "statusCode": 200,
            "message": f"{len(batch_files)}個のファイルを統合しました",
            "merged_file": merged_file,
            "input_merged_file": input_merged_file,
            "total_records": total_records,
            "processed_files": len([f for f in batch_files if f]),  # 実際に処理されたファイル数
            "copy_task": {
                "src_key": merged_file,
                "dst_key": input_merged_file
            }
        }
        
    except Exception as e:
        logger.error(f"カタログファイル統合処理エラー: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "message": f"処理中にエラーが発生しました: {str(e)}",
            "success": False,
            "error": str(e)
        }