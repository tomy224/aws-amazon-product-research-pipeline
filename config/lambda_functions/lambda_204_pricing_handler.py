import boto3
import os
import logging
import time
from datetime import datetime

from modules.apis.sp_api import AmazonProductAPI

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    プライシングデータを処理するLambda関数
    
    🔧 修正点：
    - フィルタリング後のデータが0件の場合の対応
    - 空ファイルでもS3に保存（copy_taskが正常に動作するため）
    - copy_taskの条件分岐を追加
    """
    input_key = event.get("input_key")
    batch_index = event.get("batch_index", 0)
    total_batches = event.get("total_batches", 1)
    
    is_last_batch = (batch_index >= total_batches - 1)
    
    if not input_key:
        return {"statusCode": 400, "message": "input_key が指定されていません。"}

    bucket = os.environ.get("BUCKET_NAME", "your-bucket-name")
    
    # ファイル名生成
    path_parts = input_key.split('/')
    filename = path_parts[-1]
    directory = '/'.join(path_parts[:-1])
    output_directory = directory.replace("input", "output")
    basename = filename.rsplit('.', 1)[0]
    
    output_key = f"{output_directory}/{basename}_pricing.csv"
    filtered_key = f"{output_directory}/{basename}_pricing_filtered.csv"
    
    logger.info(f"📁 入力ファイル: {input_key}")
    logger.info(f"📁 出力ファイル: {output_key}")
    logger.info(f"📁 フィルタ済みファイル: {filtered_key}")

    tmp_input = "/tmp/input.csv"
    tmp_output = "/tmp/output.csv"
    tmp_filtered_output = "/tmp/filtered_output.csv"

    try:
        # S3からダウンロード
        s3.download_file(bucket, input_key, tmp_input)
        logger.info(f"✅ ダウンロード成功: {input_key}")

        # API初期化
        analyzer = AmazonProductAPI()

        # プライシングデータ処理
        logger.info("🔄 プライシングデータ処理開始...")
        all_data, filtered_data = analyzer.process_pricing_data(
            input_file=tmp_input,
            output_file=tmp_output,
            batch_size=20
        )

        logger.info(f"📊 処理結果: 全体 {len(all_data)}件, フィルタ後 {len(filtered_data)}件")

        # 全データをS3にアップロード
        s3.upload_file(tmp_output, bucket, output_key)
        logger.info(f"✅ 価格データをアップロード: {output_key}")

        # 🔧 重要な修正：フィルタ済みデータの処理
        copy_task_src_key = ""
        copy_task_dst_key = ""
        
        if filtered_data and len(filtered_data) > 0:
            # フィルタ済みデータがある場合：通常の処理
            logger.info(f"📊 フィルタ済みデータあり: {len(filtered_data)}件")
            
            analyzer.save_results(filtered_data, tmp_filtered_output)
            s3.upload_file(tmp_filtered_output, bucket, filtered_key)
            logger.info(f"✅ フィルタ済みデータをアップロード: {filtered_key}")
            
            # copy_taskを設定
            copy_task_src_key = filtered_key
            copy_task_dst_key = filtered_key.replace("output/", "input/")
            
        else:
            # 🔧 フィルタ済みデータが0件の場合：空ファイルを作成
            logger.warning(f"⚠️ フィルタ済みデータが0件です")
            
            # 空のCSVファイルを作成（ヘッダーのみ）
            import pandas as pd
            empty_df = pd.DataFrame()
            
            # 元データがある場合はヘッダーを保持
            if all_data and len(all_data) > 0:
                # 元データの列構造を使用して空のDataFrameを作成
                sample_data = all_data[0]
                empty_df = pd.DataFrame(columns=sample_data.keys())
            
            # 空ファイルを保存
            empty_df.to_csv(tmp_filtered_output, index=False, encoding='utf-8-sig')
            s3.upload_file(tmp_filtered_output, bucket, filtered_key)
            logger.info(f"✅ 空のフィルタ済みファイルを作成: {filtered_key}")
            
            # 🔧 空ファイルの場合はcopy_taskを空にする（コピーをスキップ）
            copy_task_src_key = ""
            copy_task_dst_key = ""
            logger.info("📋 フィルタ済みデータが0件のため、copy_taskを空に設定")

        # JANコード一覧を抽出
        jan_codes = []
        if filtered_data and len(filtered_data) > 0:
            jan_codes = sorted(set(row["JAN"] for row in filtered_data if "JAN" in row and row["JAN"]))

        # 🔧 戻り値の修正：copy_taskの条件分岐
        result = {
            "statusCode": 200,
            "input": input_key,
            "output": output_key,
            "filtered_output": filtered_key,
            "total_items": len(all_data),
            "filtered_items": len(filtered_data),
            "is_last_batch": is_last_batch,
            "jan_codes": jan_codes,
            "copy_task": {
                "src_key": copy_task_src_key,
                "dst_key": copy_task_dst_key
            },
            "has_filtered_data": len(filtered_data) > 0  # 🔧 追加：フィルタデータの有無を明示
        }
        
        logger.info(f"🎯 copy_task設定:")
        logger.info(f"  src_key: '{copy_task_src_key}'")
        logger.info(f"  dst_key: '{copy_task_dst_key}'")
        logger.info(f"  has_filtered_data: {len(filtered_data) > 0}")
        
        return result

    except Exception as e:
        logger.error(f"❌ エラー発生: {str(e)}")
        import traceback
        logger.error(f"📊 エラー詳細: {traceback.format_exc()}")
        
        return {
            "statusCode": 500,
            "message": "プライシング処理中にエラーが発生しました。",
            "error": str(e),
            "input": input_key,
            "copy_task": {
                "src_key": "",
                "dst_key": ""
            },
            "has_filtered_data": False
        }