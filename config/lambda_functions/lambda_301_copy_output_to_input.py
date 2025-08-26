import boto3
import os
import logging
import json

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")

def lambda_handler(event, context):
    """
    S3ファイルをoutputからinputにコピーするLambda関数
    
    🔍 詳細デバッグ版：
    - 入力データの詳細ログ
    - S3バケット名の確認
    - ファイルの詳細存在チェック
    - エラーの詳細分析
    """
    
    # 🔍 デバッグ：入力データを詳細にログ出力
    logger.info("=" * 60)
    logger.info("🚀 Copy Lambda開始")
    logger.info(f"🗂️ バケット名: {BUCKET_NAME}")
    logger.info(f"📥 受信イベント: {json.dumps(event, indent=2, ensure_ascii=False)}")
    logger.info("=" * 60)
    
    files = event.get("files", [])
    if not files:
        logger.error("❌ コピー対象ファイルがありません")
        raise ValueError("コピー対象ファイルがありません")

    results = []
    errors = []

    logger.info(f"📊 処理開始: {len(files)}件のファイルを処理")

    for i, f in enumerate(files):
        src_key = f.get("src_key")
        dst_key = f.get("dst_key")

        logger.info(f"\n📁 ファイル {i+1}/{len(files)}:")
        logger.info(f"  📤 コピー元: '{src_key}'")
        logger.info(f"  📥 コピー先: '{dst_key}'")
        
        # 🔍 基本的な入力チェック
        if not src_key or not dst_key:
            error_msg = f"❌ src_keyまたはdst_keyが空です"
            logger.error(f"  {error_msg}")
            errors.append({
                "file_index": i+1, 
                "error": error_msg, 
                "src_key": src_key, 
                "dst_key": dst_key
            })
            continue

        # 🔍 空文字列チェック
        if src_key.strip() == "" or dst_key.strip() == "":
            error_msg = f"❌ src_keyまたはdst_keyが空文字列です"
            logger.error(f"  {error_msg}")
            errors.append({
                "file_index": i+1, 
                "error": error_msg, 
                "src_key": src_key, 
                "dst_key": dst_key
            })
            continue

        try:
            # 🔍 ソースファイルの詳細存在確認
            logger.info(f"  🔍 ソースファイル存在確認中...")
            logger.info(f"      バケット: '{BUCKET_NAME}'")
            logger.info(f"      キー: '{src_key}'")
            
            try:
                # ファイルの存在とメタデータを確認
                response = s3.head_object(Bucket=BUCKET_NAME, Key=src_key)
                file_size = response.get('ContentLength', 0)
                last_modified = response.get('LastModified', 'Unknown')
                content_type = response.get('ContentType', 'Unknown')
                
                logger.info(f"  ✅ ソースファイル確認OK:")
                logger.info(f"      サイズ: {file_size:,} bytes")
                logger.info(f"      更新日時: {last_modified}")
                logger.info(f"      Content-Type: {content_type}")
                
                # 🔍 空ファイルのチェック
                if file_size == 0:
                    logger.warning(f"  ⚠️ ファイルは空です (0 bytes)")
                    errors.append({
                        "file_index": i+1, 
                        "error": "空ファイル", 
                        "src_key": src_key, 
                        "dst_key": dst_key,
                        "file_size": file_size
                    })
                    continue
                    
            except s3.exceptions.NoSuchKey as e:
                error_msg = f"❌ ソースファイルが見つかりません"
                logger.error(f"  {error_msg}")
                logger.error(f"  📋 NoSuchKey詳細: {str(e)}")
                
                # 🔍 バケット内の類似ファイルを探してみる
                try:
                    # フォルダ内のファイル一覧を取得
                    folder_path = '/'.join(src_key.split('/')[:-1]) + '/'
                    logger.info(f"  🗂️ フォルダ '{folder_path}' の内容確認中...")
                    
                    paginator = s3.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=folder_path)
                    
                    found_files = []
                    for page in pages:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                found_files.append(obj['Key'])
                    
                    logger.info(f"  📋 フォルダ内のファイル ({len(found_files)}件):")
                    for file_key in found_files[:10]:  # 最大10件表示
                        logger.info(f"      - {file_key}")
                    
                    if len(found_files) > 10:
                        logger.info(f"      ... 他 {len(found_files) - 10} 件")
                        
                except Exception as list_error:
                    logger.error(f"  ❌ フォルダ内容確認エラー: {str(list_error)}")
                
                errors.append({
                    "file_index": i+1, 
                    "error": error_msg, 
                    "src_key": src_key, 
                    "dst_key": dst_key,
                    "details": str(e)
                })
                continue
                
            except Exception as e:
                error_msg = f"❌ ソースファイル確認エラー: {str(e)}"
                logger.error(f"  {error_msg}")
                errors.append({
                    "file_index": i+1, 
                    "error": error_msg, 
                    "src_key": src_key, 
                    "dst_key": dst_key,
                    "details": str(e)
                })
                continue

            # 🔍 コピー処理の詳細実行
            logger.info(f"  🔄 コピー実行中...")
            
            copy_source = {"Bucket": BUCKET_NAME, "Key": src_key}
            logger.info(f"      CopySource: {copy_source}")
            logger.info(f"      Destination Bucket: '{BUCKET_NAME}'")
            logger.info(f"      Destination Key: '{dst_key}'")
            
            s3.copy_object(
                Bucket=BUCKET_NAME,
                CopySource=copy_source,
                Key=dst_key
            )

            # 🔍 コピー後の確認
            try:
                copy_response = s3.head_object(Bucket=BUCKET_NAME, Key=dst_key)
                copy_size = copy_response.get('ContentLength', 0)
                logger.info(f"  ✅ コピー成功:")
                logger.info(f"      コピー先サイズ: {copy_size:,} bytes")
                
                results.append({
                    "from": src_key, 
                    "to": dst_key, 
                    "size": copy_size,
                    "status": "success"
                })
                
            except Exception as e:
                error_msg = f"❌ コピー後確認エラー: {str(e)}"
                logger.error(f"  {error_msg}")
                errors.append({
                    "file_index": i+1, 
                    "error": error_msg, 
                    "src_key": src_key, 
                    "dst_key": dst_key
                })

        except Exception as e:
            error_msg = f"❌ コピー処理エラー: {str(e)}"
            logger.error(f"  {error_msg}")
            logger.error(f"  📋 エラー詳細: {type(e).__name__}")
            
            # 🔍 特別なエラー処理
            if "NoSuchKey" in str(e):
                logger.error(f"  🎯 NoSuchKeyエラーの詳細分析:")
                logger.error(f"      - バケット名: '{BUCKET_NAME}'")
                logger.error(f"      - ファイルキー: '{src_key}'")
                logger.error(f"      - キーの長さ: {len(src_key)} 文字")
                logger.error(f"      - 制御文字チェック: {repr(src_key)}")
            
            errors.append({
                "file_index": i+1, 
                "error": error_msg, 
                "src_key": src_key, 
                "dst_key": dst_key,
                "error_type": type(e).__name__
            })

    # 🔍 最終結果サマリー
    success_count = len(results)
    error_count = len(errors)
    total_count = len(files)
    
    logger.info("=" * 60)
    logger.info(f"🏁 処理完了サマリー:")
    logger.info(f"  📊 合計: {total_count}件")
    logger.info(f"  ✅ 成功: {success_count}件")
    logger.info(f"  ❌ エラー: {error_count}件")
    
    # エラーがある場合は詳細を出力
    if errors:
        logger.error("❌ エラー詳細:")
        for error in errors:
            logger.error(f"  📁 ファイル{error['file_index']}: {error['error']}")
            logger.error(f"      src: '{error['src_key']}'")
            logger.error(f"      dst: '{error['dst_key']}'")
            if 'details' in error:
                logger.error(f"      詳細: {error['details']}")
    
    logger.info("=" * 60)

    return {
        "message": f"{success_count}/{total_count} 件コピーしました (エラー: {error_count}件)",
        "copied": results,
        "errors": errors,
        "summary": {
            "total": total_count,
            "success": success_count,
            "error": error_count
        },
        "debug_info": {
            "bucket_name": BUCKET_NAME,
            "execution_time": context.get_remaining_time_in_millis() if context else "N/A"
        }
    }