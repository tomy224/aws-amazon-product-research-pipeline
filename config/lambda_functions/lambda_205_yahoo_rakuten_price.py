import boto3
import csv
import os
import json
import logging
from modules.apis.yahoo_api import YahooShoppingAPI
from modules.apis.rakuten_api import RakutenAPI

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "3"))

def lambda_handler(event, context):
    """
    event = {
        "input_key": "input/xxx/merged_pricing_filtered.csv",
        "jan_code": 4901234567890,  # 注意: 数値型かもしれない
        "jan_asin_map": {"4901234567890": "B00XXXX"}  # 注意: キーは文字列型
    }
    """
    # 詳細なイベントログ
    logger.info(f"受信イベント: {json.dumps(event, default=str)}")
    
    input_key = event.get("input_key")
    jan_code = event.get("jan_code")
    jan_asin_map = event.get("jan_asin_map", {})
    
    # JANコードの型変換（数値型→文字列型）
    jan_code_str = str(jan_code) if jan_code is not None else ""
    
    # JAN→ASINマッピングの詳細ログ
    logger.info(f"JAN→ASINマッピング数: {len(jan_asin_map)}")
    logger.info(f"マッピング例（最初の3つ）: {dict(list(jan_asin_map.items())[:3])}")
    logger.info(f"検索対象JAN: {jan_code} (型: {type(jan_code).__name__})")
    logger.info(f"検索対象JAN(文字列変換後): {jan_code_str}")
    
    if not input_key or not jan_code:
        logger.error("input_key または jan_code が指定されていません")
        return {"statusCode": 400, "message": "input_key または jan_code が指定されていません"}

    # 出力ファイル名の構築（より柔軟な方法）
    if "merged_pricing_filtered" in input_key:
        output_key = input_key.replace("input/", "output/").replace("merged_pricing_filtered.csv", "merged_yahoraku.csv")
    else:
        # 従来のパターン
        output_key = input_key.replace("input/", "output/").replace("_pricing_filtered.csv", "_yahoraku.csv")
        
    logger.info(f"入力ファイル: {input_key}")
    logger.info(f"出力ファイル: {output_key}")

    # 一時ファイル - 変数定義を外側に移動
    tmp_output = "/tmp/yahoraku_result.csv"
    tmp_input = "/tmp/input.csv"

    # マッピングからASINを取得（存在すれば）
    asin = ""
    # 文字列キーでの検索
    if jan_code_str in jan_asin_map:
        asin = jan_asin_map[jan_code_str]
        logger.info(f"マッピングから ASIN {asin} を取得しました（文字列キー: {jan_code_str}）")
    else:
        # マッピングキーの型をログ出力
        key_types = {k: type(k).__name__ for k in list(jan_asin_map.keys())[:5]}
        logger.info(f"マッピングキーの型サンプル: {key_types}")
        logger.info(f"JAN {jan_code_str} に対応するマッピングが見つかりませんでした")
        
        # 代替検索：すべてのキーを文字列に変換して比較
        for k, v in jan_asin_map.items():
            if str(k) == jan_code_str:
                asin = v
                logger.info(f"代替検索で ASIN {asin} を取得しました（キー変換: {k} → {jan_code_str}）")
                break
    
    # マッピングがない場合のみ、ファイルから取得を試みる
    if not asin:
        logger.info("マッピングからASINを取得できなかったため、ファイルからの読み込みを試みます")
        
        # 入力CSVから対応するASINを取得するコード（既存のもの）
        try:
            # S3からファイルをダウンロード
            s3.download_file(BUCKET_NAME, input_key, tmp_input)
            logger.info(f"ファイルダウンロード成功: {input_key}")
            
            # JAN→ASINのマッピング作成
            with open(tmp_input, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                # 列名を確認（大文字小文字の違いに対応）
                headers = [h.upper() for h in reader.fieldnames]
                jan_col = None
                asin_col = None
                
                for i, header in enumerate(reader.fieldnames):
                    if header.upper() == 'JAN':
                        jan_col = header
                    if header.upper() == 'ASIN':
                        asin_col = header
                
                if not jan_col or not asin_col:
                    logger.warning(f"必要な列が見つかりません。JAN列: {jan_col}, ASIN列: {asin_col}")
                else:
                    # ファイルを再度開いて検索
                    f.seek(0)
                    next(reader)  # ヘッダーをスキップ
                    
                    for row in reader:
                        if jan_col in row and asin_col in row:
                            # 文字列変換して比較
                            if str(row[jan_col]) == jan_code_str:
                                asin = row[asin_col]
                                logger.info(f"ファイルから ASIN {asin} を取得しました")
                                break
        except Exception as e:
            logger.error(f"入力ファイル読み込みエラー: {str(e)}")
    
    # Yahoo API呼び出し
    client_id = os.environ.get("YAHOO_CLIENT_ID")
    yahoo = YahooShoppingAPI(client_id)
    yahoo_results = yahoo.search_by_jan(jan_code_str, max_items=MAX_ITEMS)
    logger.info(f"Yahoo APIから {len(yahoo_results)} 件の結果を取得")
    
    # Rakuten API 呼び出し
    app_id = os.environ.get("RAKUTEN_APPLICATION_ID")
    rakuten = RakutenAPI(app_id)
    rakuten_results = rakuten.search_by_jan(jan_code_str, max_items=MAX_ITEMS)
    logger.info(f"楽天 APIから {len(rakuten_results)} 件の結果を取得")

    # 結果を統合
    combined_results = []
    
    # Yahoo結果を追加
    for item in yahoo_results:
        item['ASIN'] = asin  # ASINを追加
        combined_results.append(item)
    
    # 楽天結果を追加
    for item in rakuten_results:
        item['ASIN'] = asin  # ASINを追加
        combined_results.append(item)
    
    # 結果の先頭項目をログ出力
    if combined_results:
        logger.info(f"JAN {jan_code_str} の検索結果 {len(combined_results)}件、ASIN: {asin}")
        logger.info(f"結果サンプル: {combined_results[0]}")
    
    if not combined_results:
        logger.warning(f"JAN {jan_code_str} の商品は見つかりませんでした")
        # 結果がなくても copy_task を含める
        return {
            "statusCode": 200,
            "message": f"JAN {jan_code_str} の商品は見つかりませんでした",
            "jan_code": jan_code_str,
            "asin": asin,
            "copy_task": {
                "src_key": output_key,
                "dst_key": output_key.replace("output/", "input/")
            }
        }

    # CSVヘッダー
    headers = ['JAN', 'ASIN', 'API', '価格', '送料条件', '商品URL']

    # ローカルに保存
    with open(tmp_output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for item in combined_results:
            writer.writerow(item)

    # S3に追記（存在していればマージ、なければ新規）
    try:
        try:
            existing = s3.get_object(Bucket=BUCKET_NAME, Key=output_key)
            old_data = existing['Body'].read().decode('utf-8-sig')
            new_data = open(tmp_output, encoding='utf-8-sig').read()
            merged_data = old_data.strip() + "\n" + "\n".join(new_data.strip().split("\n")[1:])  # skip header
            logger.info(f"既存ファイルに追記します: {output_key}")
        except s3.exceptions.NoSuchKey:
            merged_data = open(tmp_output, encoding='utf-8-sig').read()
            logger.info(f"新規ファイルを作成します: {output_key}")

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=merged_data.encode("utf-8-sig"),
            ContentType="text/csv"
        )
        logger.info(f"S3に保存しました: {output_key}")
    except Exception as e:
        logger.error(f"S3保存エラー: {str(e)}")

    return {
        "statusCode": 200,
        "jan_code": jan_code_str,
        "asin": asin,
        "output_key": output_key,
        "item_count": len(combined_results),
        "yahoo_count": len(yahoo_results),
        "rakuten_count": len(rakuten_results),
        "copy_task": {
            "src_key": output_key,
            "dst_key": output_key.replace("output/", "input/")
        }
    }