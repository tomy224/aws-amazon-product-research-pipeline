import boto3
import os
import time
import logging
from datetime import datetime

from modules.apis.sp_api import AmazonProductAPI

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    input_key = event.get("input_key")
    if not input_key:
        return {"statusCode": 400, "message": "input_key が指定されていません。"}

    bucket_name = os.environ.get("BUCKET_NAME", "your-bucket-name")

    # 一時ファイルパス（Lambdaで使用可能な /tmp 以下）
    tmp_input_file = "/tmp/input.csv"
    tmp_output_file = "/tmp/output.csv"

    # 出力先キー
    output_key = input_key.replace("input/", "output/").replace(".csv", "_result.csv")

    try:
        # S3から入力ファイルをダウンロード
        s3.download_file(bucket_name, input_key, tmp_input_file)
        logger.info(f"S3からダウンロード完了: {input_key}")

        # AmazonProductAPIの初期化（内部で環境変数を取得）
        analyzer = AmazonProductAPI()

        # カタログAPI処理の実行（バッチ処理）
        logger.info("カタログAPIのバッチ処理を開始")
        catalog_data = analyzer.process_catalog_data(
            input_file=tmp_input_file,
            output_file=tmp_output_file,
            batch_size=20
        )

        # 結果をS3にアップロード
        s3.upload_file(tmp_output_file, bucket_name, output_key)
        logger.info(f"出力ファイルをS3にアップロード完了: {output_key}")

        return {
            "statusCode": 200,
            "message": f"{len(catalog_data)} 件のカタログ情報を処理しました。",
            "input": input_key,
            "output": output_key,
            "copy_task": {
                "src_key": output_key,
                "dst_key": output_key.replace("output/", "input/")
            }
        }


    except Exception as e:
        logger.error(f"処理中にエラーが発生: {str(e)}")
        return {
            "statusCode": 500,
            "message": "処理中にエラーが発生しました",
            "error": str(e)
        }
