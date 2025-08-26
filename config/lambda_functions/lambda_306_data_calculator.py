import boto3
import pandas as pd
import numpy as np
import io
import os
import json
import logging
import re
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
    統合データに計算処理を行うLambda関数
    data_calculator.pyと同じ計算ロジックを実装
    
    event = {
        "input_file": "output/2025-05-26/chunk_001/final_integrated_data.csv",
        "chunk_path": "output/2025-05-26/chunk_001"
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
        
        logger.info(f"計算処理開始: {input_file}")
        
        # 入力ファイルを読み込み
        df = read_csv_from_s3(input_file)
        logger.info(f"入力データ: {len(df)}行, {len(df.columns)}列")
        
        # data_calculatorと同じ計算処理を実行
        calculator = ProductCalculatorLambda()
        
        # 工程1: 基本的な計算処理
        logger.info("工程1: 基本的な計算処理を実行中")
        result_df = calculator.add_calculation_columns(df)
        
        # 工程2-1: サイズ計算処理
        logger.info("工程2-1: サイズ計算処理を実行中")
        result_df = calculator.add_size_calculations(result_df)
        
        # 工程2-2: カテゴリ計算処理
        logger.info("工程2-2: カテゴリ計算処理を実行中")
        result_df = calculator.add_category_calculations(result_df)
        
        # 工程2-3-1: 仕入れ価格計算処理
        logger.info("工程2-3-1: 仕入れ価格計算処理を実行中")
        result_df = calculator.add_sourcing_price_calculations(result_df)
        
        # 工程2-3-2: ヤフー・楽天情報処理
        logger.info("工程2-3-2: ヤフー・楽天情報処理を実行中")
        result_df = calculator.add_yahoo_rakuten_calculations(result_df)
        
        # 工程3-1: 手数料合計・利益計算処理
        logger.info("工程3-1: 手数料合計・利益計算処理を実行中")
        result_df = calculator.add_profit_calculations(result_df)
        
        # 工程3-2: 期待販売数・期待利益計算処理
        logger.info("工程3-2: 期待販売数・期待利益計算処理を実行中")
        result_df = calculator.add_expected_sales_calculations(result_df)
        
        # 出力ファイルパスの設定
        output_file = f"{chunk_path}/calculated_data.csv"
        
        # 結果をS3に保存
        save_dataframe_to_s3(result_df, output_file)
        
        # 統計情報の計算（JSON serializable）
        total_records = int(len(result_df))
        total_columns = int(len(result_df.columns))
        added_columns = int(total_columns - len(df.columns))
        
        logger.info(f"計算処理完了: {output_file}")
        
        return {
            "statusCode": 200,
            "message": "データ計算処理が完了しました",
            "input_file": input_file,
            "output_file": output_file,
            "total_records": total_records,
            "total_columns": total_columns,
            "added_columns": added_columns,
            "copy_task": {
                "src_key": output_file,
                "dst_key": output_file.replace("output/", "input/")
            }
        }
        
    except Exception as e:
        logger.error(f"計算処理エラー: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "message": f"処理中にエラーが発生しました: {str(e)}",
            "error": str(e)
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
        logger.error(f"ファイル読み込みエラー {file_key}: {str(e)}")
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
        logger.info(f"ファイル保存完了: {file_key}")
    except Exception as e:
        logger.error(f"ファイル保存エラー {file_key}: {str(e)}")
        raise

def load_json_from_s3(file_key):
    """S3からJSONファイルを読み込む"""
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read().decode('utf-8')
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        logger.warning(f"JSONファイルが見つかりません: {file_key}")
        return {}
    except Exception as e:
        logger.error(f"JSON読み込みエラー {file_key}: {str(e)}")
        return {}

class ProductCalculatorLambda:
    """
    Lambda環境用のProductCalculator
    data_calculator.pyと同じ計算ロジックを実装
    """
    
    def __init__(self):
        """初期化"""
        # 設定のデフォルト値
        self.config = {
            'calculator': {
                'point_rate': {
                    'yahoo': 0.05,
                    'rakuten': 0.02
                }
            }
        }
    
    def add_calculation_columns(self, df):
        """基本的な計算列を追加（data_calculator.pyと同じ）"""
        try:
            result_df = df.copy()

            # セット数の計算処理
            if 'セット数(Q)' in result_df.columns and 'セット数(N)' in result_df.columns:
                result_df['セット数_不明'] = ''
                
                def calculate_set_count(row):
                    q_value = row['セット数(Q)'] if pd.notna(row['セット数(Q)']) and 1 <= row['セット数(Q)'] < 10 else None
                    n_value = row['セット数(N)'] if pd.notna(row['セット数(N)']) and 1 <= row['セット数(N)'] < 10 else None
                    
                    if q_value is None and n_value is None:
                        return 1
                    
                    if q_value is not None and n_value is not None:
                        if q_value == n_value:
                            return int(q_value)
                        elif q_value < n_value:
                            return int(n_value)
                        else:
                            return int(q_value)
                    elif q_value is None and n_value is not None:
                        return int(n_value)
                    elif q_value is not None and n_value is None:
                        return int(q_value)
                
                result_df['セット数_セット数'] = result_df.apply(calculate_set_count, axis=1)
                
                # 両方の値がない行に「x」を設定
                result_df.loc[(pd.isna(result_df['セット数(Q)']) | (result_df['セット数(Q)'] < 1) | (result_df['セット数(Q)'] >= 10)) & 
                            (pd.isna(result_df['セット数(N)']) | (result_df['セット数(N)'] < 1) | (result_df['セット数(N)'] >= 10)), 
                            'セット数_不明'] = 'x'

            # セット商品判定
            if '商品名' in result_df.columns:  # 'sp'サフィックスがない場合に対応
                result_df['商品情報_セット商品?'] = ''
                result_df.loc[result_df['商品名'].str.contains('セット', na=False), '商品情報_セット商品?'] = 'x'
                
            # 販売価格の合計計算
            if 'カート価格' in result_df.columns:
                result_df['販売価格_カート合計'] = result_df['カート価格'] + result_df['カート価格送料'].fillna(0) + result_df['カート価格のポイント'].fillna(0)
            
            if 'FBA最安値' in result_df.columns:
                result_df['販売価格_FBA合計'] = result_df['FBA最安値'] + result_df['FBA最安値のポイント'].fillna(0)
            
            if '自己発送最安値' in result_df.columns:
                result_df['販売価格_自己発合計'] = result_df['自己発送最安値'] + result_df['自己発送最安値の送料'].fillna(0) + result_df['自己発送最安値のポイント'].fillna(0)

            # 販売価格_設定販売額の計算
            if all(col in result_df.columns for col in ['販売価格_カート合計', '販売価格_FBA合計', '販売価格_自己発合計']):
                has_cart = result_df['販売価格_カート合計'].notna()
                has_fba = result_df['販売価格_FBA合計'].notna()
                has_self = result_df['販売価格_自己発合計'].notna()
                
                self_price_plus5 = result_df['販売価格_自己発合計'] * 1.05
                result_df['販売価格_設定販売額'] = pd.NA
                
                # カート価格がある場合の処理
                cart_condition = has_cart
                result_df.loc[cart_condition & has_fba & (~has_self), '販売価格_設定販売額'] = result_df.loc[cart_condition & has_fba & (~has_self), '販売価格_カート合計']
                result_df.loc[cart_condition & has_fba & has_self, '販売価格_設定販売額'] = result_df.loc[cart_condition & has_fba & has_self, '販売価格_カート合計']
                
                cart_cond = cart_condition & (~has_fba) & has_self
                for idx in result_df[cart_cond].index:
                    cart_price = result_df.loc[idx, '販売価格_カート合計']
                    self_price_105 = self_price_plus5.loc[idx]
                    if pd.notna(cart_price) and pd.notna(self_price_105):
                        self_price_105_rounded = round(self_price_105)
                        if self_price_105_rounded < cart_price:
                            result_df.loc[idx, '販売価格_設定販売額'] = self_price_105_rounded
                        else:
                            result_df.loc[idx, '販売価格_設定販売額'] = cart_price
                
                # カート価格がない場合の処理
                no_cart_condition = ~has_cart
                result_df.loc[no_cart_condition & has_fba & (~has_self), '販売価格_設定販売額'] = result_df.loc[no_cart_condition & has_fba & (~has_self), '販売価格_FBA合計']
                result_df.loc[no_cart_condition & has_fba & has_self, '販売価格_設定販売額'] = result_df.loc[no_cart_condition & has_fba & has_self, ['販売価格_FBA合計', '販売価格_自己発合計']].min(axis=1)
                result_df.loc[no_cart_condition & (~has_fba) & has_self, '販売価格_設定販売額'] = result_df.loc[no_cart_condition & (~has_fba) & has_self, '販売価格_自己発合計']
            
            # サイズ計算
            size_columns = ['パッケージ最長辺', 'パッケージ中辺', 'パッケージ最短辺', 'パッケージ重量']
            
            # サイズ不明の判定
            result_df['サイズ_サイズ不明'] = np.where(
                result_df[size_columns].isna().any(axis=1),
                '不明',
                ''
            )
            
            # サイズの合計計算
            if 'パッケージ最長辺' in result_df.columns:
                size_unknown = result_df['サイズ_サイズ不明'] == '不明'
                
                result_df.loc[~size_unknown, 'サイズ_合計cm'] = (
                    result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) + 
                    result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) + 
                    result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0)
                )
                result_df.loc[size_unknown, 'サイズ_合計cm'] = pd.NA
                
                # 体積計算
                result_df.loc[~size_unknown, 'サイズ_合計cm3'] = (
                    result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) * 
                    result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) * 
                    result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0)
                )
                result_df.loc[size_unknown, 'サイズ_合計cm3'] = pd.NA

            # 小型標準判定
            if 'パッケージ最長辺' in result_df.columns and 'パッケージ重量' in result_df.columns:
                size_unknown = result_df['サイズ_サイズ不明'] == '不明'
                result_df.loc[~size_unknown, 'サイズ_小型標準判定'] = np.where(
                    (result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) <= 25) & 
                    (result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) <= 18) & 
                    (result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0) <= 2) & 
                    (result_df.loc[~size_unknown, 'パッケージ重量'].fillna(0) <= 250), 
                    '対象', '対象外'
                )
                result_df.loc[size_unknown, 'サイズ_小型標準判定'] = ''

            # Amazon出品判定
            if 'Amazon価格' in result_df.columns:
                result_df['出品者_amazon'] = np.where(result_df['Amazon価格'].fillna(0) >= 1, '有', '無')

            # Amazon在庫切れ率判定
            if 'amazon_90日間在庫切れ率' in result_df.columns:
                result_df['出品者_90日amazonなし率_50%未満'] = ''
                result_df.loc[result_df['amazon_90日間在庫切れ率'] < 0.5, '出品者_90日amazonなし率_50%未満'] = 'x'
                        
            logger.info(f"基本計算処理完了: {len(result_df.columns) - len(df.columns)}列追加")
            return result_df
            
        except Exception as e:
            logger.error(f"基本計算処理エラー: {str(e)}")
            return df

    def add_size_calculations(self, df):
        """サイズ計算処理"""
        try:
            result_df = df.copy()
            
            # S3からサイズデータを読み込み
            size_data = load_json_from_s3('config/shipping_size_data.json')
            
            if not size_data:
                logger.warning("サイズデータが見つかりません")
                return result_df
            
            size_categories = size_data.get('サイズ区分', {})
            storage_fees = size_data.get('在庫保管手数料', {})
            
            def determine_size_category(row):
                if row['サイズ_サイズ不明'] == '不明':
                    return "標準-2"
                    
                sum_of_edges = row['サイズ_合計cm'] if pd.notna(row['サイズ_合計cm']) else 0
                longest_edge = row['パッケージ最長辺'] if pd.notna(row['パッケージ最長辺']) else 0
                middle_edge = row['パッケージ中辺'] if pd.notna(row['パッケージ中辺']) else 0
                shortest_edge = row['パッケージ最短辺'] if pd.notna(row['パッケージ最短辺']) else 0
                weight = row['パッケージ重量'] if pd.notna(row['パッケージ重量']) else 0
                
                size_limits = size_data.get('サイズ区分上限', {})
                
                # サイズカテゴリ判定ロジック（data_calculator.pyと同じ）
                if (weight <= size_limits['小型']['最大重量'] and 
                    sum_of_edges <= size_limits['小型']['最大寸法']['三辺合計'] and
                    longest_edge <= size_limits['小型']['最大寸法']['最長辺'] and
                    middle_edge <= size_limits['小型']['最大寸法']['中辺'] and
                    shortest_edge <= size_limits['小型']['最大寸法']['最短辺']):
                    main_category = "小型"
                elif (weight <= size_limits['標準']['最大重量'] and 
                    sum_of_edges <= size_limits['標準']['最大寸法']['三辺合計'] and
                    longest_edge <= size_limits['標準']['最大寸法']['最長辺'] and
                    middle_edge <= size_limits['標準']['最大寸法']['中辺'] and
                    shortest_edge <= size_limits['標準']['最大寸法']['最短辺']):
                    main_category = "標準"
                elif (weight <= size_limits['大型']['最大重量'] and 
                    sum_of_edges <= size_limits['大型']['最大寸法']['三辺合計']):
                    main_category = "大型"
                elif (weight <= size_limits['特大型']['最大重量'] and 
                    sum_of_edges <= size_limits['特大型']['最大寸法']['三辺合計']):
                    main_category = "特大型"
                else:
                    return "対象外"
                
                # 詳細カテゴリ判定
                matching_categories = [name for name, data in size_categories.items() 
                                    if name.startswith(main_category) and
                                    weight <= data.get('重量', float('inf'))]
                
                return matching_categories[0] if matching_categories else main_category
            
            def calculate_storage_fee(row):
                if row['サイズ_サイズ不明'] == '不明':
                    return 10
                
                volume_cm3 = row['サイズ_合計cm3'] if pd.notna(row['サイズ_合計cm3']) else 0
                size_category = row['サイズ_大きさ'] if pd.notna(row['サイズ_大きさ']) else "対象外"
                
                if size_category == "対象外":
                    return None
                
                main_category = size_category.split('-')[0] if '-' in size_category else size_category
                
                if main_category in storage_fees:
                    fee_rate = storage_fees[main_category].get('単価', 0)
                    storage_fee = fee_rate * (volume_cm3 / 1000)
                    return round(storage_fee)
                
                return None
            
            # サイズ判定と手数料計算
            result_df['サイズ_大きさ'] = result_df.apply(determine_size_category, axis=1)
            result_df['手数料・利益_月額保管料'] = result_df.apply(calculate_storage_fee, axis=1).apply(
                lambda x: -x if pd.notna(x) else None
            )

            # 配送代行手数料計算
            if 'サイズ_大きさ' in result_df.columns and '販売価格_設定販売額' in result_df.columns:
                def calculate_shipping_fee(row):
                    size_category = row['サイズ_大きさ']
                    price = row['販売価格_設定販売額'] if pd.notna(row['販売価格_設定販売額']) else 0
                    
                    if size_category == "対象外" or size_category not in size_categories:
                        return None
                    
                    fee_data = size_categories[size_category].get('配送代行手数料', {})
                    if price <= 1000:
                        return fee_data.get('1000円以下', None)
                    else:
                        return fee_data.get('1000円超', None)
                
                result_df['手数料・利益_発送代行手数料'] = result_df.apply(calculate_shipping_fee, axis=1).apply(
                    lambda x: -x if pd.notna(x) else None
                )
            
            logger.info("サイズ計算処理完了")
            return result_df
            
        except Exception as e:
            logger.error(f"サイズ計算処理エラー: {str(e)}")
            return df

    def add_category_calculations(self, df):
        """カテゴリ計算処理"""
        try:
            result_df = df.copy()
            
            # S3からカテゴリデータを読み込み
            category_data = load_json_from_s3('config/category_data.json')
            
            if not category_data:
                logger.warning("カテゴリデータが見つかりません")
                return result_df
            
            category_mapping = category_data.get('カテゴリマッピング', {})
            
            # カテゴリIDマッピング作成
            category_id_to_name = {}
            for category_name, info in category_mapping.items():
                category_id = info.get('keepaカテゴリID')
                if category_id:
                    category_id_to_name[str(category_id)] = category_name
            
            def get_category_info_and_fee_rate(row):
                # カテゴリーIDの取得（'カテゴリー'列を使用）
                if pd.notna(row.get('カテゴリーID')):
                    try:
                        category_id = str(int(row['カテゴリーID']))
                    except (ValueError, TypeError):
                        category_id = str(row['カテゴリーID'])
                elif pd.notna(row.get('カテゴリー')):
                    try:
                        category_id = str(int(row['カテゴリー']))  # ← フォールバック用（互換性維持）
                    except (ValueError, TypeError):
                        category_id = str(row['カテゴリー'])
                else:
                    category_id = ''
                
                category_name = category_id_to_name.get(category_id, '不明')
                price = row['販売価格_設定販売額'] if pd.notna(row['販売価格_設定販売額']) else 0
                
                fee_rate = None
                fee_category = "不明"
                media_fee = None
                
                if category_name in category_mapping:
                    category_info = category_mapping[category_name]
                    fee_category = category_info.get('販売手数料カテゴリ', "不明")
                    fee_rates = category_info.get('販売手数料率', [])
                    
                    base_media_fee = category_info.get('メディア手数料')
                    if base_media_fee is not None:
                        media_fee = -(base_media_fee * 1.1)
                        media_fee = round(media_fee)
                    
                    # 手数料率計算
                    if isinstance(fee_rates, list):
                        for rate_info in fee_rates:
                            upper_limit = rate_info.get('上限金額')
                            if upper_limit is None or price <= upper_limit:
                                fee_rate = rate_info.get('料率')
                                break
                    elif isinstance(fee_rates, dict):
                        if price <= 750 and '750円以下' in fee_rates:
                            fee_rate = fee_rates['750円以下']
                        elif 750 < price <= 1500 and '750円超 1500円以下' in fee_rates:
                            fee_rate = fee_rates['750円超 1500円以下']
                        elif price > 1500 and '1500円超' in fee_rates:
                            fee_rate = fee_rates['1500円超']
                        elif '750円超' in fee_rates and price > 750:
                            fee_rate = fee_rates['750円超']
                        elif 'default' in fee_rates:
                            fee_rate = fee_rates['default']
                    else:
                        fee_rate = fee_rates
                
                return pd.Series([category_name, fee_category, fee_rate, media_fee])
            
            # カテゴリ処理
            if 'カテゴリー' in result_df.columns and '販売価格_設定販売額' in result_df.columns:
                result_df[['商品情報_カテゴリ', '販売手数料カテゴリ', '手数料・利益_販売手数料率', '手数料・利益_メディア手数料']] = (
                    result_df.apply(get_category_info_and_fee_rate, axis=1)
                )
                
                # 手数料率表示用
                result_df['手数料・利益_販売手数料率_表示用'] = result_df['手数料・利益_販売手数料率'].apply(
                    lambda x: f"{x*100:.1f}%" if pd.notna(x) else "対象外"
                )
                
                # 販売手数料計算
                def calculate_fee(row):
                    if pd.isna(row['手数料・利益_販売手数料率']) or pd.isna(row['販売価格_設定販売額']):
                        return None
                    
                    category_name = row['商品情報_カテゴリ']
                    min_fee = 0
                    if category_name in category_mapping:
                        min_fee = category_mapping[category_name].get('最低販売手数料', 0)
                    
                    calculated_fee = row['販売価格_設定販売額'] * row['手数料・利益_販売手数料率']
                    
                    if min_fee is None:
                        return calculated_fee
                    
                    return max(calculated_fee, min_fee)
                
                result_df['手数料・利益_販売手数料'] = result_df.apply(calculate_fee, axis=1).apply(
                    lambda x: -round(x) if pd.notna(x) else None
                )
                
                # 販売手数料（税込）
                result_df['手数料・利益_販売手数料(税込)'] = result_df['手数料・利益_販売手数料'].apply(
                    lambda x: round(x * 1.1) if pd.notna(x) else None
                )
                
                logger.info("カテゴリ計算処理完了")
            else:
                logger.warning("カテゴリ計算に必要な列がありません")
            
            return result_df
            
        except Exception as e:
            logger.error(f"カテゴリ計算処理エラー: {str(e)}")
            return df

    def add_sourcing_price_calculations(self, df):
        """仕入れ価格計算処理"""
        try:
            result_df = df.copy()
            
            # 仕入れサイト情報
            sourcing_sites = [
                {
                    'name': 'ネッシー',
                    'price_column': 'ネッシー_価格',
                    'is_tax_included': False,
                    'url_prefix': 'https://www.netsea.jp/search/?keyword=',
                    'url_column': None
                },
                {
                    'name': 'スーデリ',
                    'price_column': 'スーデリ_価格',
                    'is_tax_included': False,
                    'url_prefix': 'https://www.superdelivery.com/p/do/psl/?so=score&vi=1&sb=all&word=',
                    'url_column': None
                }
            ]
            
            def find_cheapest_price_and_url(row):
                min_price = float('inf')
                min_price_site = None
                min_price_url = None
                
                for site in sourcing_sites:
                    price_column = site['price_column']
                    
                    if price_column in row and pd.notna(row[price_column]):
                        try:
                            if isinstance(row[price_column], str):
                                price_str = re.search(r'\d+', row[price_column])
                                if price_str:
                                    price = float(price_str.group())
                                else:
                                    continue
                            else:
                                price = float(row[price_column])
                            
                            if not site['is_tax_included']:
                                price = price * 1.1
                            
                            if price < min_price:
                                min_price = price
                                min_price_site = site
                                
                                if site['url_column'] and site['url_column'] in row and pd.notna(row[site['url_column']]):
                                    min_price_url = row[site['url_column']]
                                elif 'JAN' in row and pd.notna(row['JAN']):
                                    min_price_url = site['url_prefix'] + str(row['JAN'])
                        except (ValueError, TypeError):
                            continue
                
                if min_price_site is None:
                    return pd.Series([None, None])
                
                return pd.Series([round(min_price), min_price_url])
            
            # 仕入れ価格計算
            if 'JAN' in result_df.columns:
                existing_sites = []
                for site in sourcing_sites:
                    if site['price_column'] in result_df.columns:
                        existing_sites.append(site)
                
                if existing_sites:
                    result_df[['JAN価格_JAN価格下代(税込)', 'JAN価格_商品URL']] = result_df.apply(
                        find_cheapest_price_and_url, axis=1
                    )
                    logger.info("仕入れ価格計算完了")
            
            return result_df
            
        except Exception as e:
            logger.error(f"仕入れ価格計算エラー: {str(e)}")
            return df

    def add_yahoo_rakuten_calculations(self, df):
        """ヤフー・楽天情報処理"""
        try:
            result_df = df.copy()
            
            # ポイント率設定
            yahoo_point_rate = self.config['calculator']['point_rate']['yahoo']
            rakuten_point_rate = self.config['calculator']['point_rate']['rakuten']
            
            # ポイント加味価格計算
            for i in range(1, 4):
                yahoo_price_col = f'ヤフー_価格_{i}'
                if yahoo_price_col in result_df.columns:
                    result_df[f'ヤフー_価格_ポイント加味_{i}'] = result_df[yahoo_price_col].apply(
                        lambda x: int(round(x * (1 - yahoo_point_rate))) if pd.notna(x) else None
                    )
                
                rakuten_price_col = f'楽天_価格_{i}'
                if rakuten_price_col in result_df.columns:
                    result_df[f'楽天_価格_ポイント加味_{i}'] = result_df[rakuten_price_col].apply(
                        lambda x: int(round(x * (1 - rakuten_point_rate))) if pd.notna(x) else None
                    )
            
            # 送料条件付き表示
            for i in range(1, 4):
                yahoo_price_col = f'ヤフー_価格_ポイント加味_{i}'
                yahoo_shipping_col = f'ヤフー_送料条件_{i}'
                
                if yahoo_price_col in result_df.columns and yahoo_shipping_col in result_df.columns:
                    result_df[f'ヤフー_価格_ポイント加味_送料条件_{i}'] = result_df.apply(
                        lambda row: f"【{int(row[yahoo_price_col])}】" if pd.notna(row[yahoo_price_col]) and pd.notna(row[yahoo_shipping_col]) and 
                                    (row[yahoo_shipping_col] == '送料無料') else
                                    (f"〈{int(row[yahoo_price_col])}〉" if pd.notna(row[yahoo_price_col]) and pd.notna(row[yahoo_shipping_col]) and 
                                    (row[yahoo_shipping_col] == '条件付き送料無料') else
                                    (str(int(row[yahoo_price_col])) if pd.notna(row[yahoo_price_col]) else None)),
                        axis=1
                    )
                
                rakuten_price_col = f'楽天_価格_ポイント加味_{i}'
                rakuten_shipping_col = f'楽天_送料条件_{i}'
                
                if rakuten_price_col in result_df.columns and rakuten_shipping_col in result_df.columns:
                    result_df[f'楽天_価格_ポイント加味_送料条件_{i}'] = result_df.apply(
                        lambda row: f"【{int(row[rakuten_price_col])}】" if pd.notna(row[rakuten_price_col]) and pd.notna(row[rakuten_shipping_col]) and 
                                    (row[rakuten_shipping_col] == '送料込み') else
                                    (str(int(row[rakuten_price_col])) if pd.notna(row[rakuten_price_col]) else None),
                        axis=1
                    )

            # URL生成
            if 'JAN' in result_df.columns:
                result_df['楽天_価格ナビURL'] = result_df['JAN'].apply(
                    lambda x: f"https://search.rakuten.co.jp/search/mall/{x}/?s=2" if pd.notna(x) else None
                )
                
                result_df['ヨリヤス_比較URL'] = result_df['JAN'].apply(
                    lambda x: f"https://yoriyasu.jp/products?keyword={x}&sort=priceLow&page=1" if pd.notna(x) and str(x).strip() != '' else None
                )

            # 実質最安値計算
            yahoo_min_col = 'ヤフー_価格_ポイント加味_1'
            rakuten_min_col = '楽天_価格_ポイント加味_1'

            if yahoo_min_col in result_df.columns and rakuten_min_col in result_df.columns:
                result_df['ネット価格_実質最安値'] = result_df.apply(
                    lambda row: min(row[yahoo_min_col], row[rakuten_min_col]) 
                                if pd.notna(row[yahoo_min_col]) and pd.notna(row[rakuten_min_col]) 
                                else (row[yahoo_min_col] if pd.notna(row[yahoo_min_col]) 
                                    else (row[rakuten_min_col] if pd.notna(row[rakuten_min_col]) 
                                            else None)),
                    axis=1
                )
            
            logger.info("ヤフー・楽天情報処理完了")
            return result_df
            
        except Exception as e:
            logger.error(f"ヤフー・楽天情報処理エラー: {str(e)}")
            return df

    def add_profit_calculations(self, df):
        """利益計算処理"""
        try:
            result_df = df.copy()
            
            # 手数料合計計算
            fee_columns = [
                '手数料・利益_販売手数料(税込)',
                '手数料・利益_発送代行手数料',
                '手数料・利益_メディア手数料',
                '手数料・利益_月額保管料'
            ]
            
            existing_fee_columns = [col for col in fee_columns if col in result_df.columns]
            
            if existing_fee_columns:
                def calculate_total_fee(row):
                    total = 0
                    for col in existing_fee_columns:
                        value = row[col]
                        if pd.notna(value):
                            total += value
                    return total
                
                result_df['手数料・利益_手数料合計'] = result_df.apply(calculate_total_fee, axis=1)
            
            # 実質最安値計算
            def calculate_real_cost(row):
                jan_price = None
                if 'JAN価格_JAN価格下代(税込)' in row and pd.notna(row['JAN価格_JAN価格下代(税込)']):
                    jan_price = float(row['JAN価格_JAN価格下代(税込)'])
                
                net_price = None
                if 'ネット価格_実質最安値' in row and pd.notna(row['ネット価格_実質最安値']):
                    net_price = float(row['ネット価格_実質最安値'])
                
                if jan_price is not None and net_price is not None:
                    return min(jan_price, net_price)
                elif jan_price is not None:
                    return jan_price
                elif net_price is not None:
                    return net_price
                else:
                    return None
            
            # 利益計算
            if '販売価格_設定販売額' in result_df.columns and '手数料・利益_手数料合計' in result_df.columns:
                result_df['仕入価格_実質最安値'] = result_df.apply(calculate_real_cost, axis=1)
                
                result_df['手数料・利益_利益額'] = result_df.apply(
                    lambda row: round(
                        row['販売価格_設定販売額'] - row['仕入価格_実質最安値'] + row['手数料・利益_手数料合計']
                    ) if pd.notna(row['販売価格_設定販売額']) and pd.notna(row['仕入価格_実質最安値']) and row['仕入価格_実質最安値'] > 0 else None, 
                    axis=1
                )
                
                result_df['手数料・利益_利益率'] = result_df.apply(
                    lambda row: f"{round((row['手数料・利益_利益額'] / row['販売価格_設定販売額']) , 3)}" 
                    if pd.notna(row['手数料・利益_利益額']) and pd.notna(row['販売価格_設定販売額']) and row['販売価格_設定販売額'] > 0 else None,
                    axis=1
                )
                
                logger.info("利益計算処理完了")
            
            return result_df
            
        except Exception as e:
            logger.error(f"利益計算処理エラー: {str(e)}")
            return df

    def add_expected_sales_calculations(self, df):
        """期待販売数・利益計算処理"""
        try:
            result_df = df.copy()
            
            # 期待販売数(1ヶ月)
            if '30日間_新品販売数' in result_df.columns and 'FBA数' in result_df.columns:
                result_df['期待販売数・利益_販売期待数(1ヶ月)'] = result_df.apply(
                    lambda row: round(
                        row['30日間_新品販売数'] / (row['FBA数'] + 1)
                    ) if pd.notna(row['30日間_新品販売数']) and pd.notna(row['FBA数']) and (row['FBA数'] + 1) > 0 else 0,
                    axis=1
                )
            
            # 期待利益(1ヶ月)
            if '期待販売数・利益_販売期待数(1ヶ月)' in result_df.columns and '手数料・利益_利益額' in result_df.columns:
                result_df['期待販売数・利益_期待利益(1ヶ月)'] = result_df.apply(
                    lambda row: round(
                        row['期待販売数・利益_販売期待数(1ヶ月)'] * row['手数料・利益_利益額']
                    ) if pd.notna(row['期待販売数・利益_販売期待数(1ヶ月)']) and pd.notna(row['手数料・利益_利益額']) else None,
                    axis=1
                )
            
            # 期待利益(3ヶ月)
            if '90日間_新品販売数' in result_df.columns and '手数料・利益_利益額' in result_df.columns:
                result_df['期待販売数・利益_期待利益(3ヶ月)'] = result_df.apply(
                    lambda row: round(
                        (row['90日間_新品販売数'] / 4) * row['手数料・利益_利益額']
                    ) if pd.notna(row['90日間_新品販売数']) and pd.notna(row['手数料・利益_利益額']) else None,
                    axis=1
                )
            
            logger.info("期待販売数・利益計算処理完了")
            return result_df
            
        except Exception as e:
            logger.error(f"期待販売数・利益計算処理エラー: {str(e)}")
            return df