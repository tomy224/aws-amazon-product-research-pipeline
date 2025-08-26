import boto3
import csv
import os
import json
import logging
import time
from datetime import datetime

from modules.apis.keepa_api import ProductAnalyzer

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# S3クライアント
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
BATCH_SIZE = 10  # 一度に処理する最大ASIN数

def lambda_handler(event, context):
    """
    複数ASINを一括処理するハンドラー
    
    event = {
        "input_key": "input/xxx/merged_yahoraku_unique_asins.csv",
        "asins": ["B01ABCDEF", "B02GHIJKL", ...] // 処理対象のASINリスト
    }
    または
    event = {
        "input_key": "input/xxx/merged_yahoraku_unique_asins.csv",
        "asin": "B01ABCDEF" // 単一ASIN（後方互換性のため）
    }
    """
    # イベントからパラメータを取得
    input_key = event.get("input_key")
    
    # 複数ASINか単一ASINかを判定
    asins = event.get("asins", [])
    if not asins and event.get("asin"):
        # 単一ASINの場合はリストに変換（後方互換性維持）
        asins = [event.get("asin")]
        logger.info(f"単一ASINモード: {asins[0]}")
    
    # バッチサイズを制限
    if len(asins) > BATCH_SIZE:
        original_count = len(asins)
        asins = asins[:BATCH_SIZE]
        logger.info(f"ASINをバッチサイズに制限しました: {original_count}件 → {len(asins)}件")
    
    if not input_key or not asins:
        logger.error("input_keyまたはasinsが指定されていません")
        return {
            "statusCode": 400,
            "message": "入力パラメータが不足しています"
        }
    
    # 処理ASINのログ出力
    logger.info(f"処理対象ASIN: {asins}")
    logger.info(f"ASIN数: {len(asins)}件")
    
    # 出力ファイル名の構築
    # merged_yahoraku_unique_asins.csv → merged_keepa_results.csv
    output_dir = os.path.dirname(input_key.replace("input/", "output/"))
    output_key = f"{output_dir}/merged_keepa_results.csv"
    
    # 一時ファイル
    tmp_output = "/tmp/keepa_output.csv"
    
    try:
        # 開始ログ
        logger.info(f"Keepa商品分析を開始: ASINs={asins}, 入力={input_key}")
        start_time = time.time()
        
        # アナライザーのインスタンス作成
        analyzer = ProductAnalyzer()
        
        # Keepa APIで商品情報を取得
        logger.info(f"ASINからKeepa情報を取得します: {len(asins)}件")
        df = analyzer.get_product_data(asins)
        
        if df.empty:
            logger.warning(f"ASINリスト {asins} の商品情報が取得できませんでした")
            return {
                "statusCode": 200,
                "message": f"{len(asins)}件のASINの商品情報が取得できませんでした",
                "asins": asins,
                "success": False
            }
        
        # 成功したASINリストを作成
        successful_asins = df['ASIN'].tolist() if 'ASIN' in df.columns else []
        failed_asins = [asin for asin in asins if asin not in successful_asins]
        
        logger.info(f"取得成功: {len(successful_asins)}/{len(asins)}件")
        
        # フィルタリング用の条件
        # 在庫切れ率50%以上のデータのみ
        filtered_df = None
        if 'amazon_90日間在庫切れ率' in df.columns:
            pre_filter_count = len(df)
            filtered_df = df[df['amazon_90日間在庫切れ率'] >= 0.50].copy()
            
            # フィルタリング後のファイル名を生成
            filtered_output_key = output_key.replace('.csv', '_filtered.csv')
            
            logger.info(f"在庫切れ率50%以上のフィルタリング: {len(filtered_df)}/{pre_filter_count}件")
        
        # 結果をS3に追記（存在していればマージ、なければ新規）
        try:
            # 既存ファイルをチェック
            try:
                existing = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
                # ローカルにダウンロード
                s3.download_file(BUCKET_NAME, output_key, tmp_output)
                file_exists = True
                logger.info(f"既存ファイルを読み込みました: {output_key}")
            except s3.exceptions.NoSuchKey:
                file_exists = False
                logger.info(f"新規ファイルを作成します: {output_key}")
            
            # 結果の保存
            if file_exists:
                # 既存ファイルにヘッダーを確認
                with open(tmp_output, 'r', encoding='utf-8-sig') as f:
                    reader = csv.reader(f)
                    headers = next(reader, None)
                
                # DataFrameをCSVとして追記（ヘッダーなし）
                with open(tmp_output, 'a', encoding='utf-8-sig') as f:
                    df.to_csv(f, index=False, header=False)
                
                # S3にアップロード
                s3.upload_file(tmp_output, BUCKET_NAME, output_key)
                logger.info(f"既存ファイルに追記しました: {output_key}")
            else:
                # 新規ファイルとして保存
                df.to_csv(tmp_output, index=False, encoding='utf-8-sig')
                s3.upload_file(tmp_output, BUCKET_NAME, output_key)
                logger.info(f"新規ファイルを作成しました: {output_key}")
            
            # フィルタリング済みファイルの処理（在庫切れ率50%以上）
            filtered_file_exists = False
            if filtered_df is not None and not filtered_df.empty:
                try:
                    # 既存のフィルタリング済みファイルをチェック
                    s3.head_object(Bucket=BUCKET_NAME, Key=filtered_output_key)
                    filtered_file_exists = True
                    # ローカルにダウンロード
                    s3.download_file(BUCKET_NAME, filtered_output_key, "/tmp/filtered_output.csv")
                except s3.exceptions.ClientError:
                    filtered_file_exists = False
                
                # フィルタリング済みデータの保存
                if filtered_file_exists:
                    # 既存ファイルに追記
                    with open("/tmp/filtered_output.csv", 'a', encoding='utf-8-sig') as f:
                        filtered_df.to_csv(f, index=False, header=False)
                    s3.upload_file("/tmp/filtered_output.csv", BUCKET_NAME, filtered_output_key)
                    logger.info(f"フィルタリング済みファイルに追記しました: {filtered_output_key}")
                else:
                    # 新規ファイルとして保存
                    filtered_df.to_csv("/tmp/filtered_output.csv", index=False, encoding='utf-8-sig')
                    s3.upload_file("/tmp/filtered_output.csv", BUCKET_NAME, filtered_output_key)
                    logger.info(f"フィルタリング済みファイルを作成しました: {filtered_output_key}")
        
        except Exception as e:
            logger.error(f"ファイル保存エラー: {str(e)}")
            return {
                "statusCode": 500,
                "message": f"ファイル保存中にエラーが発生しました: {str(e)}",
                "asins": asins,
                "successful_asins": successful_asins,
                "failed_asins": failed_asins,
                "success": False,
                "error": str(e)
            }
        
        # 処理時間の計算
        end_time = time.time()
        processing_time = end_time - start_time
        
        return {
            "statusCode": 200,
            "message": f"{len(successful_asins)}/{len(asins)}件のASINの処理が完了しました",
            "asins": asins,
            "successful_asins": successful_asins,
            "failed_asins": failed_asins,
            "success": len(successful_asins) > 0,
            "processing_time": processing_time,
            "output_key": output_key,
            "filtered_output_key": filtered_output_key if 'filtered_output_key' in locals() else None,
            "copy_task": {
                "src_key": output_key,
                "dst_key": output_key.replace("output/", "input/")
            }
        }
        
    except Exception as e:
        logger.error(f"Keepa API処理エラー: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "message": f"処理中にエラーが発生しました: {str(e)}",
            "asins": asins,
            "success": False,
            "error": str(e)
        }