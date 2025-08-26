import boto3
import pandas as pd
import io
import os
import logging
import re
from datetime import datetime

# S3クライアント
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    4つのCSVファイルを統合する最終処理（output出力版）
    
    🔧 修正点：
    - output フォルダに保存
    - copy_task を返すように変更
    - input フォルダには保存しない
    
    event = {
        "keepa_file": "output/2025-05-26/chunk_001/merged_keepa_results_filtered.csv",
        "pricing_file": "output/2025-05-26/chunk_001/merged_pricing_filtered.csv", 
        "yahoraku_file": "output/2025-05-26/chunk_001/merged_yahoraku.csv",
        "chunk_path": "output/2025-05-26/chunk_001"
    }
    """
    
    try:
        # イベントからファイルパスを取得
        keepa_file = event.get("keepa_file")
        pricing_file = event.get("pricing_file")
        yahoraku_file = event.get("yahoraku_file")
        chunk_path = event.get("chunk_path", "").rstrip('/')
        
        # パラメータ検証
        if not all([keepa_file, pricing_file, yahoraku_file, chunk_path]):
            logger.error("必要なファイルパスが不足しています")
            return {
                "statusCode": 400,
                "message": "必要なパラメータが不足しています"
            }
        
        logger.info(f"🚀 Final Integration 開始:")
        logger.info(f"  - Keepa: {keepa_file}")
        logger.info(f"  - Pricing: {pricing_file}")
        logger.info(f"  - YahoRaku: {yahoraku_file}")
        logger.info(f"  - Chunk Path: {chunk_path}")
        
        # 各ファイルを読み込む
        # 1. Keepaデータ（基準となるデータ）
        keepa_df = read_csv_from_s3(keepa_file)
        logger.info(f"📊 Keepaデータ: {len(keepa_df)}行, {len(keepa_df.columns)}列")
        
        # 2. Pricingデータ
        pricing_df = read_csv_from_s3(pricing_file)
        logger.info(f"📊 Pricingデータ: {len(pricing_df)}行, {len(pricing_df.columns)}列")
        
        # 3. YahoRakuデータ
        yahoraku_df = read_csv_from_s3(yahoraku_file)
        logger.info(f"📊 YahoRakuデータ: {len(yahoraku_df)}行, {len(yahoraku_df.columns)}列")
        
        # data_integrator.pyと同じ順序で結合
        # ステップ1: KeepaとPricingを結合（ASINキー）
        merged_df = merge_keepa_pricing(keepa_df, pricing_df)
        logger.info(f"✅ Keepa + Pricing: {len(merged_df)}行, {len(merged_df.columns)}列")
        
        # ステップ2: YahoRakuデータをASINで横展開結合
        merged_df = merge_yahoraku_data_by_asin(merged_df, yahoraku_df)
        logger.info(f"✅ + YahoRaku: {len(merged_df)}行, {len(merged_df.columns)}列")
        
        # ステップ3: data_integrator.pyと同じ列順序に並び替え
        merged_df = rearrange_columns_like_integrator(merged_df)
        
        # ステップ4: JANコードの形式を統一
        merged_df = fix_jan_format(merged_df)
        
        # 🔧 修正：出力ファイルパスをoutputフォルダに設定
        output_key = f"{chunk_path}/final_integrated_data.csv"
        
        logger.info(f"📁 出力先: {output_key}")
        
        # CSVとして保存
        csv_buffer = io.StringIO()
        merged_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        # 🔧 修正：outputフォルダに保存
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=csv_buffer.getvalue().encode('utf-8-sig'),
            ContentType='text/csv'
        )
        
        # 統計情報の計算
        jan_count = merged_df['JAN'].notna().sum() if 'JAN' in merged_df.columns else 0
        
        logger.info(f"✅ 統合完了: {output_key}")
        logger.info(f"📊 結果: {len(merged_df)}行, {len(merged_df.columns)}列")
        
        # 🔧 修正：copy_taskを返すように変更
        return {
            "statusCode": 200,
            "message": "データ統合が完了しました",
            "output_file": output_key,
            "total_records": int(len(merged_df)),
            "total_columns": len(merged_df.columns),
            "jan_records": int(jan_count),
            "files_integrated": {
                "keepa": keepa_file,
                "pricing": pricing_file,
                "yahoraku": yahoraku_file
            },
            "copy_task": {
                "src_key": output_key,
                "dst_key": output_key.replace("output/", "input/")
            }
        }
        
    except Exception as e:
        logger.error(f"❌ 統合処理エラー: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "message": f"処理中にエラーが発生しました: {str(e)}",
            "error": str(e),
            "copy_task": {
                "src_key": "",
                "dst_key": ""
            }
        }

def read_csv_from_s3(file_key):
    """
    S3からCSVファイルを読み込む（JANを文字列として処理）
    """
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read()
        # JANコードを文字列として読み込む（重要！）
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig", dtype={'JAN': str})
        
        # JANコードの形式を修正（浮動小数点表記を修正）
        if 'JAN' in df.columns:
            df['JAN'] = df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            
        return df
    except Exception as e:
        logger.error(f"❌ ファイル読み込みエラー {file_key}: {str(e)}")
        raise

def merge_keepa_pricing(keepa_df, pricing_df):
    """
    KeepaとPricingデータを結合（data_integrator.pyと同じロジック）
    """
    # Pricingデータの型修正（data_integrator.pyと同じ処理）
    if '自己発送最安値のポイント' in pricing_df.columns:
        # 文字列の'False'と'True'を数値に変換
        pricing_df['自己発送最安値のポイント'] = pricing_df['自己発送最安値のポイント'].replace(['False', 'True'], [0, 1])
        # NaN値を一時的な数値に置き換え
        pricing_df['自己発送最安値のポイント'] = pricing_df['自己発送最安値のポイント'].fillna(-999)
        # 数値型に変換
        pricing_df['自己発送最安値のポイント'] = pd.to_numeric(pricing_df['自己発送最安値のポイント'], errors='coerce')
    
    # Keepaをベースにして左結合（data_integrator.pyと同じ）
    merged_df = pd.merge(
        keepa_df,     # Keepaデータを基準
        pricing_df,
        on='ASIN',
        how='left',   # Keepaデータを基準に左結合
        suffixes=('', '_sp')  # Keepaの列名を優先
    )
    
    # JAN列の統合（Keepa優先）
    if 'JAN_sp' in merged_df.columns:
        merged_df['JAN'] = merged_df['JAN'].combine_first(merged_df['JAN_sp'])
        merged_df = merged_df.drop(columns=['JAN_sp'])
        logger.info("✅ JAN列を統合しました (Keepa優先)")
    
    # 自己発送最安値のポイント列の後処理
    if '自己発送最安値のポイント' in merged_df.columns:
        # 文字列やブール値を数値に変換
        merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace(['False', 'True'], [0, 1])
        merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace([False, True], [0, 1])
        # -999を再びNoneに戻す
        merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace(-999, None)
        # 数値型に変換
        merged_df['自己発送最安値のポイント'] = pd.to_numeric(merged_df['自己発送最安値のポイント'], errors='coerce')
    
    return merged_df

def merge_yahoraku_data_by_asin(base_df, yahoraku_df):
    """
    YahoRakuデータをASINベースで横展開して結合（data_integrator.pyと同じ処理）
    """
    logger.info(f"🔄 Yahoo/Rakuten結合開始")
    
    # ASINが存在しない場合の処理
    if 'ASIN' not in base_df.columns:
        logger.warning("⚠️ ベースデータにASIN列がありません")
        return base_df
    
    if 'ASIN' not in yahoraku_df.columns:
        logger.warning("⚠️ YahoRakuデータにASIN列がありません")
        return base_df
    
    # ASIN列を文字列に変換
    base_df['ASIN'] = base_df['ASIN'].astype(str)
    yahoraku_df['ASIN'] = yahoraku_df['ASIN'].astype(str)
    
    # API列の存在確認
    if 'API' not in yahoraku_df.columns:
        logger.warning("⚠️ YahoRakuデータにAPI列がありません")
        return base_df
    
    # API別に分割
    yahoo_df = yahoraku_df[yahoraku_df['API'] == 'Yahoo'].copy()
    rakuten_df = yahoraku_df[yahoraku_df['API'] == 'Rakuten'].copy()
    
    logger.info(f"📊 Yahoo: {len(yahoo_df)}件, Rakuten: {len(rakuten_df)}件")
    
    # 各ASINごとに処理
    base_asins = base_df['ASIN'].dropna().unique()
    added_yahoo_data = 0
    added_rakuten_data = 0
    
    for asin in base_asins:
        # Yahoo情報（上位3件）
        yahoo_rows = yahoo_df[yahoo_df['ASIN'] == asin].head(3)
        for i, (_, row) in enumerate(yahoo_rows.iterrows(), 1):
            for col in ['価格', '送料条件', '商品URL']:
                if col in row and pd.notna(row[col]):
                    col_name = f'ヤフー_{col}_{i}'
                    base_df.loc[base_df['ASIN'] == asin, col_name] = row[col]
                    added_yahoo_data += 1
        
        # Rakuten情報（上位3件）
        rakuten_rows = rakuten_df[rakuten_df['ASIN'] == asin].head(3)
        for i, (_, row) in enumerate(rakuten_rows.iterrows(), 1):
            for col in ['価格', '送料条件', '商品URL']:
                if col in row and pd.notna(row[col]):
                    col_name = f'楽天_{col}_{i}'
                    base_df.loc[base_df['ASIN'] == asin, col_name] = row[col]
                    added_rakuten_data += 1
    
    logger.info(f"✅ Yahoo/Rakuten結合完了 - Yahoo: {added_yahoo_data}個, Rakuten: {added_rakuten_data}個のデータを追加")
    
    return base_df

def rearrange_columns_like_integrator(df):
    """
    data_integrator.pyと同じ列順序に並び替え
    """
    # data_integrator.pyと同じ列順序定義
    column_order = [
        # 基本情報1
        'ASIN', 'JAN', '商品名', 'カテゴリー', 'メーカー型番', 'レビュー有無', 
        'メーカー名', 'ブランド名', '総出品者数', 'セット数', '商品追跡日', 
        '商品発売日', '追跡開始からの経過日数', 'アダルト商品対象',

        # 基本情報2
        '参考価格', 'パッケージ最長辺', 'パッケージ中辺', 'パッケージ最短辺', 
        'パッケージ重量', '現在ランキング', '30日間平均ランキング', 
        '90日間平均ランキング', '180日間平均ランキング', 'amazonURL', 
        'KeepaURL', 'バリエーションASIN',

        # 価格情報
        'Amazon価格', 'カート価格', 'カート価格送料', 'カート価格のポイント', 
        'リードタイム（時間）', 'FBA最安値', 'FBA最安値のポイント', 
        '自己発送最安値', '自己発送最安値の送料', '自己発送最安値のポイント', 
        'FBA_販売手数料', 'FBA_配送代行手数料',

        # 出品者情報
        'amazon本体有無1', 'amazon本体有無2', 'FBA数', '自己発送数', 
        'FBA最安値出品者数', '自己発送最安値出品者数', 
        'amazon_30日間在庫切れ率', 'amazon_90日間在庫切れ率',

        # 販売数情報
        '30日間_総販売数', '30日間_新品販売数', '30日間_中古販売数', 
        '30日間_コレクター販売数', 'Keepa30日間販売数', 
        '90日間_総販売数', '90日間_新品販売数', '90日間_中古販売数', 
        '90日間_コレクター販売数', 'Keepa90日間販売数',
        '180日間_総販売数', '180日間_新品販売数', '180日間_中古販売数', 
        '180日間_コレクター販売数', 'Keepa180日間販売数',

        # 価格履歴
        'amazon価格_現在価格', 'amazon価格_最高価格', 'amazon価格_最低価格',
        'amazon価格_30日平均価格', 'amazon価格_90日平均価格', 
        'amazon価格_180日平均価格', '新品価格_現在価格', '新品価格_最高価格',
        '新品価格_最低価格', '新品価格_30日平均価格', '新品価格_90日平均価格',
        '新品価格_180日平均価格',

        # その他
        '画像URL', '元コード', 'コードタイプ'
    ]
    
    # 存在する列のみを抽出
    specified_columns = [col for col in column_order if col in df.columns]
    
    # 指定されていない残りの列（ヤフー・楽天列など）を取得
    remaining_columns = [col for col in df.columns if col not in column_order]
    
    # 指定列 + 残りの列の順で新しい列順を作成
    new_column_order = specified_columns + remaining_columns
    
    # 並び替えを実行
    df = df[new_column_order]
    
    logger.info(f"✅ カラム並び替え完了: 指定列 {len(specified_columns)}列 + 追加列 {len(remaining_columns)}列")
    
    return df

def fix_jan_format(df):
    """
    JANコードの形式を統一（data_integrator.pyと同じ処理）
    """
    if 'JAN' in df.columns:
        # 文字列として処理
        df['JAN'] = df['JAN'].astype(str)
        # 浮動小数点形式を修正
        df['JAN'] = df['JAN'].replace(r'\.0$', '', regex=True)
        # 指数表記を修正
        df['JAN'] = df['JAN'].apply(
            lambda x: f"{float(x):.0f}" if re.match(r'\d+\.\d+e\+\d+', str(x).lower()) else x
        )
        logger.info("✅ JANコード形式を統一しました")
    
    return df