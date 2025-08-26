import boto3
import pandas as pd
import io
import os
import json
import logging
import traceback
from datetime import datetime

# S3クライアント
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-bucket-name")

# ロガーの設定
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    データフィルタリングを行うLambda関数
    data_filtered.pyと同じフィルタリングロジックを実装
    
    event = {
        "input_file": "input/2025-06-03/chunk_001/calculated_data.csv",
        "chunk_path": "output/2025-06-03/chunk_001"
    }
    """
    
    try:
        # イベントからパラメータを取得
        input_file = event.get("input_file")
        chunk_path = event.get("chunk_path", "").rstrip('/')
        
        if not input_file or not chunk_path:
            logger.error("必要なパラメータが不足しています")
            return {
                "statusCode": 400,
                "message": "input_file または chunk_path が指定されていません"
            }
        
        logger.info(f"🚀 データフィルタリング開始:")
        logger.info(f"  - 入力ファイル: {input_file}")
        logger.info(f"  - 出力パス: {chunk_path}")
        
        # 入力ファイルを読み込み
        df = read_csv_from_s3(input_file)
        logger.info(f"📊 入力データ: {len(df)}行, {len(df.columns)}列")
        
        # フィルタリング設定を読み込み
        filter_config = load_filter_config()
        
        # フィルタリングとソート処理を実行
        filtered_df = filter_and_sort_data(df, filter_config)
        
        if filtered_df is None or len(filtered_df) == 0:
            logger.warning("⚠️ フィルタリング後のデータが0件です")
            # 空ファイルを作成
            filtered_df = pd.DataFrame()
        
        # 出力ファイルパスの設定
        output_file = f"{chunk_path}/filtered_data.csv"
        
        # 結果をS3に保存
        save_dataframe_to_s3(filtered_df, output_file)
        
        # 統計情報の計算
        original_count = len(df)
        filtered_count = len(filtered_df)
        excluded_count = original_count - filtered_count
        
        logger.info(f"✅ フィルタリング完了: {output_file}")
        
        return {
            "statusCode": 200,
            "message": "データフィルタリングが完了しました",
            "input_file": input_file,
            "output_file": output_file,
            "original_records": int(original_count),
            "filtered_records": int(filtered_count),
            "excluded_records": int(excluded_count),
            "filter_rate": round((filtered_count / original_count * 100), 2) if original_count > 0 else 0,
            "copy_task": {
                "src_key": output_file,
                "dst_key": output_file.replace("output/", "input/")
            }
        }
        
    except Exception as e:
        logger.error(f"❌ フィルタリング処理エラー: {str(e)}", exc_info=True)
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
    """S3からCSVファイルを読み込む"""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read()
        # JANコードを文字列として読み込む
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig", dtype={'JAN': str})
        
        # JANコードの形式修正
        if 'JAN' in df.columns:
            df['JAN'] = df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            
        return df
    except Exception as e:
        logger.error(f"❌ ファイル読み込みエラー {file_key}: {str(e)}")
        raise

def save_dataframe_to_s3(df, file_key):
    """DataFrameをS3にCSVとして保存"""
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=csv_buffer.getvalue().encode('utf-8-sig'),
            ContentType='text/csv'
        )
        logger.info(f"✅ ファイル保存完了: {file_key}")
    except Exception as e:
        logger.error(f"❌ ファイル保存エラー {file_key}: {str(e)}")
        raise

def load_filter_config():
    """
    S3からフィルタリング設定を読み込む
    設定ファイルがない場合はデフォルト値を使用
    """
    try:
        # S3から設定ファイルを読み込み
        response = s3.get_object(Bucket=BUCKET_NAME, Key='config/settings.yaml')
        content = response["Body"].read().decode('utf-8')
        
        import yaml
        config = yaml.safe_load(content)
        
        # フィルタリング条件を取得
        filter_conditions = config.get('filter_conditions', {})
        
        logger.info(f"📋 フィルタリング設定を読み込みました")
        return filter_conditions
        
    except Exception as e:
        # 設定ファイルが読み込めない場合はデフォルト値を使用
        logger.warning(f"⚠️ 設定ファイル読み込み失敗。デフォルト値を使用: {str(e)}")
        return {
            'profit_min': 100,
            'profit_rate_min': 0,
            'profit_rate_max': 40
        }

def filter_and_sort_data(df, filter_config):
    """
    データのフィルタリングと並び替えを行う（data_filtered.pyと同じロジック）
    
    Args:
        df (pandas.DataFrame): 入力データフレーム
        filter_config (dict): フィルタリング設定
    
    Returns:
        pandas.DataFrame: 処理後のデータフレーム
    """
    try:
        logger.info(f"📈 元のデータサイズ: {len(df)}件")
        
        # フィルタリング条件を取得
        profit_min = filter_config.get('profit_min', 100)
        profit_rate_min = filter_config.get('profit_rate_min', 0)
        profit_rate_max = filter_config.get('profit_rate_max', 40)
        
        logger.info(f"🔍 フィルタリング条件:")
        logger.info(f"  - 利益額 {profit_min}円以上")
        logger.info(f"  - 利益率 {profit_rate_min}%以上{profit_rate_max}%以下")
        
        # 利益額でフィルタリング（NULL値は条件を満たさないとみなす）
        if '手数料・利益_利益額' in df.columns:
            profit_mask = (~df['手数料・利益_利益額'].isna()) & (df['手数料・利益_利益額'] >= profit_min)
            logger.info(f"📊 利益額条件適用: {profit_mask.sum()}/{len(df)}件が条件を満たす")
        else:
            logger.warning("⚠️ '手数料・利益_利益額'列が見つかりません")
            profit_mask = pd.Series(True, index=df.index)
        
        # 利益率でフィルタリング（NULL値は条件を満たさないとみなす）
        if '手数料・利益_利益率' in df.columns:
            # %記号があれば削除して数値に変換
            rate_column = df['手数料・利益_利益率'].copy()
            if rate_column.dtype == 'object':  # 文字列型の場合
                # 非NaN値のみ処理
                mask = ~rate_column.isna()
                rate_column.loc[mask] = rate_column.loc[mask].astype(str).str.replace('%', '').astype(float)
                
            # フィルタリング条件を構築（NULL値は条件を満たさないとみなす）
            rate_mask = (~rate_column.isna()) & ((rate_column >= profit_rate_min) & (rate_column <= profit_rate_max))
            logger.info(f"📊 利益率条件適用: {rate_mask.sum()}/{len(df)}件が条件を満たす")
        else:
            logger.warning("⚠️ '手数料・利益_利益率'列が見つかりません")
            rate_mask = pd.Series(True, index=df.index)
        
        # 両方の条件を組み合わせてフィルタリング
        combined_mask = profit_mask & rate_mask
        filtered_df = df[combined_mask].copy()
        
        # フィルタリング後のサイズ
        filtered_size = len(filtered_df)
        excluded_size = len(df) - filtered_size
        logger.info(f"📊 フィルタリング後のデータサイズ: {filtered_size}件 (除外: {excluded_size}件)")
        
        # ランキングで並び替え
        logger.info("🔢 「現在ランキング」で昇順に並び替え中...")
        if '現在ランキング' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('現在ランキング', 
                                               ascending=True, 
                                               na_position='last').reset_index(drop=True)
            logger.info("✅ ソート完了")
        else:
            logger.warning("⚠️ '現在ランキング'列が見つかりません。ソートをスキップします。")
            
        # 🔧 data_filtered.pyと同じExcel対応フォーマット処理
        filtered_df = apply_excel_formatting(filtered_df)
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"❌ フィルタリング処理エラー: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def apply_excel_formatting(df):
    """
    Excel互換の数値フォーマット処理を適用
    data_filtered.pyと同じロジック
    """
    try:
        # JAN列を ="" 形式に変換し、小数点.0を除く
        if 'JAN' in df.columns:
            df['JAN'] = df['JAN'].apply(
                lambda x: f'="{str(x).split(".")[0]}"' if pd.notna(x) else ''
            )
            logger.info("✅ JAN列のExcel形式変換完了")

        # カテゴリーID列を ="" 形式に変換し、小数点.0を除く
        if 'カテゴリーID' in df.columns:
            df['カテゴリーID'] = df['カテゴリーID'].apply(
                lambda x: f'="{str(x).split(".")[0]}"' if pd.notna(x) else ''
            )
            logger.info("✅ カテゴリーID列のExcel形式変換完了")
        elif 'カテゴリー' in df.columns:
            # フォールバック：'カテゴリー'列がある場合
            df['カテゴリー'] = df['カテゴリー'].apply(
                lambda x: f'="{str(x).split(".")[0]}"' if pd.notna(x) else ''
            )
            logger.info("✅ カテゴリー列のExcel形式変換完了")

        # メーカー型番列を ="" 形式に変換（NaN対策つき）
        if 'メーカー型番' in df.columns:
            df['メーカー型番'] = df['メーカー型番'].apply(
                lambda x: f'="{str(x).split(".")[0]}"' if pd.notna(x) else ''
            )
            logger.info("✅ メーカー型番列のExcel形式変換完了")
        
        logger.info("✅ Excel互換フォーマット処理完了")
        return df
        
    except Exception as e:
        logger.error(f"❌ Excel形式変換エラー: {str(e)}")
        return df