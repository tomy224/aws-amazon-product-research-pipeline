#!/usr/bin/env python
# coding: utf-8

"""
Keepa APIを使用して商品情報を1ASINずつ取得・分析するモジュール

このモジュールは、Keepa APIを使用して商品情報を1ASINずつ取得・分析します。
バッチ処理ではなく単一ASIN処理に特化しており、AWS Lambdaなどのサーバーレス環境に適しています。
"""

import keepa
import pandas as pd
from datetime import datetime
import logging
import os
import yaml
from pathlib import Path
import dotenv
import traceback
import time

from modules.utils.logger_utils import get_logger, log_function_call
from modules.utils.file_utils import find_project_root, load_yaml_config, save_to_csv

# ロガーの取得
logger = get_logger(__name__)

class BaseKeepaAPISingle:
    """
    Keepa APIとの通信や基本的なデータ処理を行うベースクラス（1ASINずつ処理）
    
    このクラスは、Keepa APIの初期化、API呼び出し（単一ASIN）、基本的なデータ処理を担当します。
    """
    
    def __init__(self, config_path=None):
        """
        BaseKeepaAPISingleクラスの初期化
        
        Args:
            config_path (str, optional): 設定ファイルのパス（省略時はデフォルト値を使用）
        """
        # プロジェクトルートディレクトリの検出
        self.root_dir = find_project_root()
        
        # 環境変数の読み込み
        dotenv.load_dotenv(os.path.join(self.root_dir, '.env'))
        
        # ディレクトリパスの設定
        self.data_dir = os.path.join(self.root_dir, 'data')
        self.log_dir = os.path.join(self.root_dir, 'logs')
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 設定ファイルの読み込み
        self.config = self._load_config(config_path)
        
        try:
            # Keepa APIの初期化（環境変数から取得したAPIキーを使用）
            api_key = os.getenv('KEEPA_API_KEY') or self.config['keepa_api'].get('api_key')
            if not api_key:
                raise ValueError("KEEPA_API_KEYが設定されていません。環境変数または設定ファイルで指定してください。")
                
            self.api = keepa.Keepa(api_key, timeout=60)  # 単一ASINの場合はタイムアウトを60秒に設定
            logger.info("Keepa APIの初期化に成功しました")
        except Exception as e:
            logger.error(f"Keepa APIの初期化に失敗: {str(e)}")
            raise
    
    def _load_config(self, config_path=None):
        """
        設定ファイルを読み込む
        
        Args:
            config_path (str, optional): 設定ファイルのパス
            
        Returns:
            dict: 設定データ
        """
        if config_path is None:
            config_path = os.path.join(self.root_dir, 'config', 'settings.yaml')
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            # Keepa API設定の存在確認
            if 'keepa_api' not in config:
                raise ValueError("設定ファイルにkeepa_apiセクションが見つかりません")
                
            # 出力設定の初期化（なければデフォルト値を設定）
            if 'output' not in config['keepa_api']:
                config['keepa_api']['output'] = {
                    'input_file': os.path.join(self.data_dir, 'sp_api_output_filtered.csv'),
                    'output_file': os.path.join(self.data_dir, 'keepa_output_single.csv')
                }
            else:
                # 相対パスを絶対パスに変換
                for key in ['input_file', 'output_file']:
                    if key in config['keepa_api']['output']:
                        rel_path = config['keepa_api']['output'][key]
                        if not os.path.isabs(rel_path):
                            config['keepa_api']['output'][key] = os.path.join(self.data_dir, rel_path)
                    
            logger.info(f"設定ファイルの読み込みに成功: {config_path}")
            return config
                
        except Exception as e:
            logger.error(f"設定ファイルの読み込みに失敗: {str(e)}")
            raise
    
    @log_function_call
    def _call_api_single(self, asin):
        """
        Keepa APIを呼び出して単一ASINの情報を取得する（タイムアウト対策とトークンチェック機能付き）
        
        Args:
            asin (str): 単一のASIN
        
        Returns:
            list or None: Keepa APIからのレスポンス、エラー時はNone
        """
        MAX_RETRIES = 3  # 最大再試行回数
        
        for retry in range(MAX_RETRIES):
            try:
                # API呼び出し前にトークン残量を確認
                tokens_left = self.api.tokens_left
                logger.info(f"API呼び出し前のトークン残量: {tokens_left} (ASIN: {asin})")
                
                # 必要なトークン数を計算（ASINあたり約1トークン消費）
                estimated_tokens = 1  # 単一ASINの処理なので1トークン
                
                # トークンが不足している場合は中止
                if tokens_left < estimated_tokens or tokens_left <= 0:
                    error_msg = f"トークン不足のため処理を中止します。残トークン: {tokens_left}、必要トークン: {estimated_tokens} (ASIN: {asin})"
                    logger.error(error_msg)
                    print(f"❌ {error_msg}")
                    return None
                
                # タイムアウト設定を変更
                if hasattr(self.api, 'session') and hasattr(self.api.session, 'request'):
                    # 元のメソッドをバックアップ
                    original_request = self.api.session.request
                    
                    # タイムアウト付きのリクエスト関数で置き換え
                    def request_with_timeout(*args, **kwargs):
                        kwargs['timeout'] = 30  # 30秒タイムアウト
                        return original_request(*args, **kwargs)
                    
                    # 一時的にリクエストメソッドを置き換え
                    self.api.session.request = request_with_timeout
                
                # API呼び出し（単一ASINなのでリストに包む）
                products = self.api.query(
                    [asin],  # リストとして渡す
                    domain=self.config['keepa_api'].get('domain', 5),    # デフォルトは日本（5）
                    stats=self.config['keepa_api'].get('stats_days', 180),  # デフォルトは180日
                    days=self.config['keepa_api'].get('stats_days', 180),
                    update=1
                )
                
                # リクエストメソッドを元に戻す
                if hasattr(self.api, 'session') and hasattr(self.api.session, 'request'):
                    self.api.session.request = original_request
                
                # 呼び出し後のトークン残量をログに記録
                tokens_after = self.api.tokens_left
                tokens_used = tokens_left - tokens_after
                logger.info(f"API呼び出し後のトークン残量: {tokens_after} (消費: {tokens_used}) (ASIN: {asin})")
                
                # 結果の確認
                if products:
                    logger.info(f"API呼び出し成功: {len(products)}件のデータを取得 (ASIN: {asin})")
                    return products
                else:
                    logger.warning(f"API呼び出し結果が空です（試行 {retry+1}/{MAX_RETRIES}） (ASIN: {asin})")
                    
            except Exception as e:
                # タイムアウトエラーかどうかを確認
                if "timeout" in str(e).lower() or "read timed out" in str(e).lower():
                    logger.warning(f"タイムアウトが発生しました（試行 {retry+1}/{MAX_RETRIES}） (ASIN: {asin}): {str(e)}")
                    # 最終試行でなければ再試行
                    if retry < MAX_RETRIES - 1:
                        wait_time = (retry + 1) * 5  # 再試行ごとに待機時間を長くする
                        logger.info(f"{wait_time}秒後に再試行します... (ASIN: {asin})")
                        print(f"⏱️ タイムアウトが発生しました。{wait_time}秒後に再試行します... (ASIN: {asin})")
                        time.sleep(wait_time)
                        continue
                
                # その他のエラー
                logger.error(f"API呼び出しエラー (ASIN: {asin}): {str(e)}")
                print(f"❌ API呼び出しエラー (ASIN: {asin}): {str(e)}")
                return None
        
        # すべての再試行が失敗した場合
        logger.error(f"{MAX_RETRIES}回の試行後もAPI呼び出しに失敗しました (ASIN: {asin})")
        return None
    
    @staticmethod
    def safe_get(data, *keys, default=None):
        """
        基本的なデータ取得用のヘルパー関数
        
        Args:
            data: 対象データ（dict or list）
            *keys: 順番に取得するキー
            default: 取得できなかった場合のデフォルト値
            
        Returns:
            取得したデータまたはデフォルト値
        """
        for key in keys:
            try:
                data = data[key]
            except (KeyError, TypeError, IndexError):
                return default
        return data
    
    @log_function_call
    def load_asins_from_csv(self, input_file=None, asin_column='ASIN'):
        """
        CSVファイルからASINリストを読み込む
        
        Args:
            input_file (str, optional): 入力CSVファイル名（省略時は設定ファイルの値を使用）
            asin_column (str): ASIN列の名前
            
        Returns:
            list: ASINのリスト
        """
        try:
            # 入力ファイル名の設定
            if input_file is None:
                input_file = self.config['keepa_api']['output']['input_file']
            elif not os.path.isabs(input_file):
                # 相対パスの場合はdataディレクトリを基準にする
                input_file = os.path.join(self.data_dir, input_file)
                
            # CSVファイルの存在確認
            if not os.path.exists(input_file):
                error_msg = f"入力ファイルが見つかりません: {input_file}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
                
            # CSVファイルの読み込み
            df = pd.read_csv(input_file, encoding='utf-8-sig')
            
            # ASIN列の存在確認
            if asin_column not in df.columns:
                error_msg = f"'{asin_column}'列が見つかりません"
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            # ASINリストの取得
            asins = df[asin_column].dropna().unique().tolist()
            logger.info(f"{len(asins)}件のASINを読み込みました")
            print(f"📝 {len(asins)}件のASINを読み込みました")
            print(f"📄 入力ファイル: {input_file}")
            
            return asins
            
        except Exception as e:
            error_msg = f"ASINの読み込み中にエラーが発生: {str(e)}"
            logger.error(error_msg)
            raise
    
    @log_function_call
    def save_to_csv(self, df, output_file=None, encoding='utf-8-sig', append=False):
        """
        DataFrameをCSVファイルとして保存する
        
        Args:
            df (pandas.DataFrame): 保存するデータフレーム
            output_file (str, optional): 出力ファイル名（省略時は設定ファイルの値を使用）
            encoding (str): 文字エンコーディング（デフォルト: 'utf-8-sig'）
            append (bool): 追記モードで保存するかどうか（デフォルト: False）
        """
        try:
            # 出力ファイル名の設定
            if output_file is None:
                output_file = self.config['keepa_api']['output']['output_file']
            elif not os.path.isabs(output_file):
                # 相対パスの場合はdataディレクトリを基準にする
                output_file = os.path.join(self.data_dir, output_file)
                
            # 出力ディレクトリの作成
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            # CSVとして保存（追記モードに対応）
            mode = 'a' if append else 'w'
            header = not append or not os.path.exists(output_file)
            
            df.to_csv(output_file, index=False, encoding=encoding, mode=mode, header=header)
            
            action = "追記" if append else "保存"
            logger.info(f"データを{action}しました: {output_file} ({len(df)}件)")
            print(f"✅ {len(df)}件のデータを {output_file} に{action}しました")
            
        except Exception as e:
            error_msg = f"データの保存中にエラーが発生: {str(e)}"
            logger.error(error_msg)
            print(f"❌ {error_msg}")
            raise


class ProductAnalyzerSingle(BaseKeepaAPISingle):
    """
    Keepa APIを使用して商品情報を1ASINずつ分析するクラス
    
    このクラスは、商品情報の取得・解析に特化した機能を1ASINずつ提供します。
    """
    
    def __init__(self, config_path=None):
        """
        ProductAnalyzerSingleクラスの初期化
        
        Args:
            config_path (str, optional): 設定ファイルのパス（省略時はデフォルト値を使用）
        """
        # 親クラスの初期化
        super().__init__(config_path)
        
        # 商品分析用の設定
        # （追加の設定が必要な場合はここに記述）
    
    @log_function_call
    def _get_basic_info(self, product):
        """
        基本的な商品情報を取得
    
        Args:
            product (dict): 商品情報を含む辞書
    
        Returns:
            dict: 基本商品情報を含む辞書
        """
        try:
            # 画像URL生成
            image_url = ("https://images-na.ssl-images-amazon.com/images/I/" + 
                        product.get('imagesCSV', '').split(',')[0]) if product.get('imagesCSV') else ''
    
            # バリエーションASINの処理（5個に制限）
            variation_csv = product.get('variationCSV', '')
            if variation_csv:
                variations = variation_csv.split(',')[:5]  # 最初の5個を取得
                variation_limited = ','.join(variations)   # カンマで結合
            else:
                variation_limited = ''
    
        except Exception as e:
            logger.warning(f"画像URL生成エラー: {str(e)}")
            image_url = ''
            variation_limited = ''
    
        return {
            # 基本情報
            "ASIN": product.get('asin', ''),
            "JAN": self.safe_get(product, 'eanList', 0, default=''),
            "商品名": product.get('title', ''),
            "カテゴリーID": product.get('rootCategory', ''),
            "メーカー型番": product.get('model', ''),
            "メーカー名": product.get('manufacturer', ''),
            "ブランド名": product.get('brand', ''),
            "セット数(Q)": product.get('packageQuantity', 0),
            "セット数(N)": product.get('numberOfItems', 0),
            "レビュー有無": product.get('lastRatingUpdate', ''),
            "アダルト商品対象": product.get('isAdultProduct', False),
            "画像URL": image_url,
            "バリエーションASIN": variation_limited,
    
            # URL情報
            "amazonURL": f"https://www.amazon.co.jp/dp/{product.get('asin', '')}",
            "KeepaURL": f"https://keepa.com/#!product/5-{product.get('asin', '')}"
        }
    
    @log_function_call
    def _safe_get_price(self, stats, index, sub_index=None):
        """
        価格データを安全に取得するヘルパーメソッド
        
        Args:
            stats (dict): 統計情報を含む辞書
            index (str): 取得したい統計情報のキー（例: 'max', 'min', 'avg90'）
            sub_index (int, optional): 配列内のインデックス（Amazon価格は0, 新品価格は1）
        
        Returns:
            int or None: 価格データ。取得できない場合はNone
        """
        try:
            if not stats or index not in stats:
                return None
                
            data = stats[index]
            if not data or not isinstance(data, list):
                return None
                
            # 最高値・最安値の場合は特別な処理
            if index in ['max', 'min']:
                if len(data) <= sub_index or not data[sub_index]:
                    return None
                # 価格データは[時刻, 価格]の形式で格納されている
                return data[sub_index][1] if len(data[sub_index]) > 1 else None
                
            # 通常の価格データの場合
            if sub_index is not None:
                if len(data) <= sub_index:
                    return None
                return data[sub_index]
                
            return data
        except Exception as e:
            logger.debug(f"価格データ取得エラー: {str(e)}")
            return None
    
    @log_function_call
    def _get_price_info(self, product):
        """
        価格関連情報を取得する
        
        Args:
            product (dict): 商品情報を含む辞書
        
        Returns:
            dict: 価格関連情報を含む辞書
        """
        # statsの取得
        stats = product.get('stats', {})
        if not stats:
            logger.warning(f"価格データなし (ASIN: {product.get('asin', '不明')})")
            return {}
            
        # 価格情報の取得
        price_info = {
            # Amazon価格履歴
            "amazon価格_現在価格": self._safe_get_price(stats, 'current', 0),
            "amazon価格_最高価格": self._safe_get_price(stats, 'max', 0),
            "amazon価格_最低価格": self._safe_get_price(stats, 'min', 0),
            "amazon価格_30日平均価格": self._safe_get_price(stats, 'avg30', 0),
            "amazon価格_90日平均価格": self._safe_get_price(stats, 'avg90', 0),
            "amazon価格_180日平均価格": self._safe_get_price(stats, 'avg180', 0),
    
            # 新品価格履歴
            "新品価格_現在価格": self._safe_get_price(stats, 'current', 1),
            "新品価格_最高価格": self._safe_get_price(stats, 'max', 1),
            "新品価格_最低価格": self._safe_get_price(stats, 'min', 1),
            "新品価格_30日平均価格": self._safe_get_price(stats, 'avg30', 1),
            "新品価格_90日平均価格": self._safe_get_price(stats, 'avg90', 1),
            "新品価格_180日平均価格": self._safe_get_price(stats, 'avg180', 1),
        }
        
        logger.debug(f"価格情報の取得成功: {product.get('asin', '不明')}")
        return price_info

    @log_function_call
    def _get_rank_and_stock_info(self, product):
        """
        ランキングと在庫情報を取得
        
        Args:
            product (dict): 商品情報を含む辞書
        
        Returns:
            dict: ランキングと在庫情報を含む辞書
        """
        stats = product.get('stats', {})
        
        return {
            "総出品者数": self.safe_get(product, 'stats', 'totalOfferCount', default=0),
            "30日間平均ランキング": self.safe_get(product, 'stats', 'avg30', default=[0, 0, 0, 0])[3],
            "90日間平均ランキング": self.safe_get(product, 'stats', 'avg90', default=[0, 0, 0, 0])[3],
            "180日間平均ランキング": self.safe_get(product, 'stats', 'avg180', default=[0, 0, 0, 0])[3],
            "amazon本体有無": product.get('availabilityAmazon', -1),
            "amazon_30日間在庫切れ率": self.safe_get(stats, 'outOfStockPercentage30', default=[0])[0] / 100,
            "amazon_90日間在庫切れ率": self.safe_get(stats, 'outOfStockPercentage90', default=[0])[0] / 100,
        }
    
    @log_function_call
    def parse_history(self, history):
        """
        履歴データを辞書形式に変換
        
        Args:
            history (list): Keepa APIから取得した履歴データ
            
        Returns:
            dict: タイムスタンプをキー、値をバリューとする辞書
        """
        if history is None:
            return {}  # Noneの場合は空の辞書を返す
        return {history[i]: history[i + 1] for i in range(0, len(history), 2)}

    @log_function_call
    def calculate_sales(self, product, days):
        """
        指定期間の販売数を計算
        
        Args:
            product (dict): 商品情報
            days (int): 計算対象期間（日数）
            
        Returns:
            tuple: (総販売数, 新品販売数, 中古販売数, コレクター販売数)
        """
        try:
            # 販売ランキング、出品者数の履歴データを取得
            sales_rank_history = product['csv'][3]   # 販売ランキング履歴
            new_count_history = product['csv'][11]   # 新品出品者数履歴
            used_count_history = product['csv'][12]  # 中古出品者数履歴
            collectible_count_history = product['csv'][14]  # コレクターアイテム出品数履歴

            # 履歴データを辞書形式に変換
            sales_rank_dict = self.parse_history(sales_rank_history)
            used_count_dict = self.parse_history(used_count_history)
            collectible_count_dict = self.parse_history(collectible_count_history)

            if not sales_rank_dict:
                return 0, 0, 0, 0  # データがない場合は0を返す

            # カウンター初期化
            used_sales_count = 0
            collectible_sales_count = 0
            total_sales_count = 0

            # 計算範囲の設定
            latest_time = max(sales_rank_dict.keys())
            start_time = latest_time - (days * 24 * 60)  # days日分の時間（分単位）
            timestamps = sorted([t for t in sales_rank_dict.keys() if t >= start_time])

            # 販売数の計算
            for i in range(1, len(timestamps)):
                t1, rank1 = timestamps[i - 1], sales_rank_dict[timestamps[i - 1]]
                t2, rank2 = timestamps[i], sales_rank_dict[timestamps[i]]

                # ランキングが上昇（数値が減少）した場合
                if rank1 * 1.00 > rank2:  # 0.1%でも上昇したらカウント
                    total_sales_count += 1

                    # 中古商品の販売判定
                    if used_count_dict:
                        used1 = used_count_dict.get(min(used_count_dict.keys(), key=lambda t: abs(t - t1)), 0)
                        used2 = used_count_dict.get(min(used_count_dict.keys(), key=lambda t: abs(t - t2)), 0)
                        if used1 > used2:
                            used_sales_count += 1

                    # コレクターアイテムの販売判定
                    if collectible_count_dict:
                        coll1 = collectible_count_dict.get(min(collectible_count_dict.keys(), key=lambda t: abs(t - t1)), 0)
                        coll2 = collectible_count_dict.get(min(collectible_count_dict.keys(), key=lambda t: abs(t - t2)), 0)
                        if coll1 > coll2:
                            collectible_sales_count += 1

            # 新品販売数の計算
            new_sales_count = total_sales_count - used_sales_count - collectible_sales_count
            
            return total_sales_count, new_sales_count, used_sales_count, collectible_sales_count

        except Exception as e:
            logger.error(f"販売数計算エラー: {str(e)}")
            return 0, 0, 0, 0

    @log_function_call
    def get_sales_data(self, product):
        """
        商品の販売数データを取得
        
        Args:
            product (dict): 商品情報
            
        Returns:
            dict: 販売数情報を含む辞書
        """
        try:
            # 30日、90日、180日の販売数を計算
            sales_30 = self.calculate_sales(product, 30)
            sales_90 = self.calculate_sales(product, 90)
            sales_180 = self.calculate_sales(product, 180)

            # Keepa APIの統計情報も取得（比較用）
            stats = product.get('stats', {})
            
            return {
                # 30日データ
                "30日間_総販売数": sales_30[0],
                "30日間_新品販売数": sales_30[1],
                "30日間_中古販売数": sales_30[2],
                "30日間_コレクター販売数": sales_30[3],
                "Keepa30日間販売数": stats.get('salesRankDrops30', 0),

                # 90日データ
                "90日間_総販売数": sales_90[0],
                "90日間_新品販売数": sales_90[1],
                "90日間_中古販売数": sales_90[2],
                "90日間_コレクター販売数": sales_90[3],
                "Keepa90日間販売数": stats.get('salesRankDrops90', 0),

                # 180日データ
                "180日間_総販売数": sales_180[0],
                "180日間_新品販売数": sales_180[1],
                "180日間_中古販売数": sales_180[2],
                "180日間_コレクター販売数": sales_180[3],
                "Keepa180日間販売数": stats.get('salesRankDrops180', 0)
            }

        except Exception as e:
            logger.error(f"販売データ取得エラー: {str(e)}")
            return {}
    
    @log_function_call
    def get_single_product_data(self, asin):
        """
        単一ASINの商品情報を取得する
        
        Args:
            asin (str): 単一のASIN
            
        Returns:
            pandas.DataFrame: 商品情報のデータフレーム（1行のみ）
                              エラー時は空のデータフレーム
        """
        logger.info(f"商品情報の取得を開始: {asin}")
        
        # 1. 単一ASINに対するAPI呼び出し
        products = self._call_api_single(asin)
        if products is None or len(products) == 0:
            logger.warning(f"商品情報を取得できませんでした: {asin}")
            return pd.DataFrame()
        
        # 2. 単一商品データの処理
        product_data = []
        try:
            # リストから対象商品を取得（通常は1件のみ）
            product = products[0]
            
            # 基本的なエラーチェック
            if not product.get('stats'):
                logger.warning(f"商品データなし (ASIN: {asin})")
                return pd.DataFrame()

            # 商品情報の取得と統合
            product_info = self._get_basic_info(product)
            price_info = self._get_price_info(product)
            rank_stock_info = self._get_rank_and_stock_info(product)
            sales_info = self.get_sales_data(product)
            
            # 全ての情報を統合
            product_info.update(price_info)
            product_info.update(rank_stock_info)
            product_info.update(sales_info)
            
            # 日付情報の追加
            product_info["商品追跡日"] = product.get('trackingSince', '')
            product_info["商品発売日"] = None if product.get('releaseDate', -1) == -1 else product['releaseDate']
            
            tracking_since = product.get('trackingSince')
            if tracking_since:
                try:
                    unix_timestamp = (tracking_since + 21564000) * 60
                    tracking_date = datetime.fromtimestamp(unix_timestamp)
                    product_info["追跡開始からの経過日数"] = (datetime.today() - tracking_date).days
                except Exception as e:
                    logger.warning(f"経過日数の計算エラー: {str(e)}")
                    product_info["追跡開始からの経過日数"] = None
            else:
                product_info["追跡開始からの経過日数"] = None
            
            product_data.append(product_info)
            logger.debug(f"商品データ処理成功: {asin}")
            
        except Exception as e:
            logger.error(f"商品データ処理エラー (ASIN: {asin}): {str(e)}")
            return pd.DataFrame()

        # 希望する列の順序を定義
        desired_columns = [
            # 基本情報
            "ASIN", "JAN", "商品名", "カテゴリーID", "メーカー型番", "レビュー有無", 
            "メーカー名", "ブランド名", "総出品者数", "セット数(Q)", "セット数(N)", "商品追跡日", 
            "商品発売日", "追跡開始からの経過日数", "アダルト商品対象", "画像URL",
            
            # ランキング・URL情報
            "30日間平均ランキング", "90日間平均ランキング", "180日間平均ランキング",
            "amazonURL", "KeepaURL", "バリエーションASIN",
            
            # Amazon・在庫情報
            "amazon本体有無", "amazon_30日間在庫切れ率", "amazon_90日間在庫切れ率",
            
            # 価格情報
            "amazon価格_現在価格", "amazon価格_最高価格", "amazon価格_最低価格",
            "amazon価格_30日平均価格", "amazon価格_90日平均価格", "amazon価格_180日平均価格",
            "新品価格_現在価格", "新品価格_最高価格", "新品価格_最低価格",
            "新品価格_30日平均価格", "新品価格_90日平均価格", "新品価格_180日平均価格",
            
            # 販売数情報
            "30日間_総販売数", "30日間_新品販売数", "30日間_中古販売数", "30日間_コレクター販売数", "Keepa30日間販売数",
            "90日間_総販売数", "90日間_新品販売数", "90日間_中古販売数", "90日間_コレクター販売数", "Keepa90日間販",
            "90日間_総販売数", "90日間_新品販売数", "90日間_中古販売数", "90日間_コレクター販売数", "Keepa90日間販売数",
            "180日間_総販売数", "180日間_新品販売数", "180日間_中古販売数", "180日間_コレクター販売数", "Keepa180日間販売数"
        ]
        
        # DataFrameの列を指定した順序に並び替え
        df = pd.DataFrame(product_data)
        
        # 存在する列のみを抽出（エラー防止のため）
        valid_columns = [col for col in desired_columns if col in df.columns]
        df = df[valid_columns]
        
        logger.info(f"データ処理完了: ASIN {asin}")
        return df

    @log_function_call
    def process_multiple_asins(self, asin_list, output_file=None, interval=1.0):
        """
        複数のASINを1つずつ順番に処理し、結果を1つのCSVファイルに保存する
        
        Args:
            asin_list (list): 処理するASINのリスト
            output_file (str, optional): 出力ファイルパス
            interval (float): ASINごとの処理間隔（秒）
            
        Returns:
            int: 処理に成功したASINの数
        """
        # 実行時間の計測開始
        start_time = time.time()
        
        # 出力ファイルの準備
        if output_file is None:
            output_file = self.config['keepa_api']['output']['output_file']
        
        # 成功件数のカウンター
        success_count = 0
        
        # 最初のASINのデータフレームを初期化用に使用
        first_time = True
        
        try:
            # 各ASINを1つずつ処理
            for i, asin in enumerate(asin_list, 1):
                logger.info(f"処理中 ({i}/{len(asin_list)}): ASIN {asin}")
                print(f"🔍 処理中 ({i}/{len(asin_list)}): ASIN {asin}")
                
                # 単一ASIN処理
                df = self.get_single_product_data(asin)
                
                # データフレームが空でなければ保存
                if not df.empty:
                    # 最初のASINの場合は新規ファイル作成、以降は追記
                    append_mode = not first_time
                    self.save_to_csv(df, output_file=output_file, append=append_mode)
                    first_time = False
                    success_count += 1
                
                # 処理間隔の待機（最後のASIN以外）
                if i < len(asin_list):
                    time.sleep(interval)
                    
            # 実行時間の計測終了
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            # 結果の出力
            logger.info(f"処理完了: {success_count}/{len(asin_list)}件のASINを処理しました")
            print(f"\n✅ 処理完了: {success_count}/{len(asin_list)}件のASINを処理しました")
            print(f"⏱️ 実行時間: {elapsed_time:.2f}秒 (平均: {elapsed_time/len(asin_list):.2f}秒/ASIN)")
            
            return success_count
            
        except Exception as e:
            logger.error(f"複数ASIN処理中にエラーが発生: {str(e)}")
            traceback.print_exc()
            return success_count