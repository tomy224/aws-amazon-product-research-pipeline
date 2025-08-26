# modules/apis/sp_api.py
#!/usr/bin/env python
# coding: utf-8
"""
Amazon Selling Partner API (SP-API) ラッパーモジュール

このモジュールは、Amazon SP-APIへのアクセスを容易にするための
ラッパークラスとユーティリティを提供します。

主な機能:
- レート制限の管理
- アクセストークンの取得と更新
- 商品情報の取得（カタログ、価格など）
- バッチ処理によるパフォーマンス最適化
- エラーハンドリングとリトライロジック
"""

import requests
import time
import logging
import os
import yaml
import json
import traceback
import csv
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
from pathlib import Path

# 内部モジュールのインポート
from modules.utils.logger_utils import get_logger, log_function_call
from modules.utils.file_utils import find_project_root, load_yaml_config

# ロガーの設定
logger = get_logger(__name__)

class EnhancedAPIRateLimiter:
    """
    SP-APIのレート制限を管理するためのクラス
    
    複数のAPIエンドポイントに対して異なるレート制限を適用し、
    APIの使用量を監視します。
    """
    
    def __init__(self, requests_per_second=2.0):
        """
        レート制限管理クラスの初期化
        
        Args:
            requests_per_second (float): 1秒あたりの最大リクエスト数
        """
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.last_request_times = []  # 直近のリクエスト時間を記録
        self.window_size = int(requests_per_second * 2)  # 監視するリクエスト履歴のサイズ
    
    def wait_if_needed(self):
        """
        必要に応じて待機してレート制限に適合させる
        
        Returns:
            float: 現在のタイムスタンプ
        """
        current_time = time.time()
        
        # 古すぎるリクエスト履歴を削除（1秒以上前のもの）
        while self.last_request_times and current_time - self.last_request_times[0] > 1.0:
            self.last_request_times.pop(0)
        
        # 現在の履歴サイズがウィンドウサイズ以上なら待機
        if len(self.last_request_times) >= self.window_size:
            # 最も古いリクエストから1秒経過するまで待機
            oldest_request = self.last_request_times[0]
            wait_time = max(0, 1.0 - (current_time - oldest_request))
            if wait_time > 0:
                time.sleep(wait_time)
                current_time = time.time()  # 待機後の時間を更新
        
        # 直近のリクエスト間隔が最小間隔より小さい場合も待機
        if self.last_request_times and (current_time - self.last_request_times[-1] < self.min_interval):
            wait_time = self.min_interval - (current_time - self.last_request_times[-1])
            time.sleep(wait_time)
            current_time = time.time()  # 待機後の時間を更新
        
        # 現在のリクエスト時間を記録
        self.last_request_times.append(current_time)
        return current_time


class AmazonSPAPI:
    """
    Amazon Selling Partner API (SP-API)の基本機能を提供するクラス
    
    認証、設定の管理、基本的なAPI呼び出しなどの機能を実装します。
    """
    
    def __init__(self, config_path: str = None):
        """
        SP-APIクライアントの初期化
        
        Args:
            config_path (str, optional): 設定ファイルのパス
        """
        # プロジェクトルートディレクトリの検出
        self.root_dir = find_project_root()
        
        # 環境変数の読み込み
        self._load_env_vars()
        
        # # ディレクトリパスの設定　Lambda上ではdataフォルダに書き込めず、/tmp/に書き込む必要があるため、下の分岐コードにした
        # self.data_dir = os.path.join(self.root_dir, 'data')
        # self.log_dir = os.path.join(self.root_dir, 'logs')

        # ディレクトリパスの設定
        is_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        if is_lambda:
            self.data_dir = '/tmp'
            self.log_dir = '/tmp'
        else:
            self.data_dir = os.path.join(self.root_dir, 'data')
            self.log_dir = os.path.join(self.root_dir, 'logs')
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # ログ設定
        self._setup_logging()
        
        # 設定ファイルの読み込み
        self.config = self._load_config(config_path)
        
        # 環境変数から認証情報を取得して設定ファイルにマージ
        self._merge_env_variables()
        
        # アクセストークン取得
        self.access_token = self.get_access_token()
        
        # トークン取得時刻を記録
        self.token_timestamp = time.time()

        self.rate_limiter = EnhancedAPIRateLimiter(8.0)  # 0.125秒間隔 = 1秒あたり8リクエスト ※sp-api側のレートが適用されるからこの値は念の為
    
    def _load_env_vars(self):
        """環境変数の読み込み"""
        try:
            import dotenv
            dotenv_path = os.path.join(self.root_dir, '.env')
            if os.path.exists(dotenv_path):
                dotenv.load_dotenv(dotenv_path)
                logger.debug("環境変数を.envファイルから読み込みました")
        except ImportError:
            logger.warning("python-dotenvがインストールされていません。環境変数の自動読み込みをスキップします。")
    
    def _setup_logging(self):
        """ログ設定"""
        log_file = os.path.join(self.log_dir, f'sp_api_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        
        # すでに存在するハンドラをすべて削除（重複を防ぐため）
        logger_instance = logging.getLogger('')
        while logger_instance.handlers:
            logger_instance.removeHandler(logger_instance.handlers[0])
        
        # 基本設定
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8-sig'  # エンコーディングを明示的に指定
        )
        
        # コンソールにもログを出力
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)  
        console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger('').addHandler(console)
        
        # ログファイルの場所を明示的に表示
        logger.info(f"ログファイル: {log_file}")
    
    # def _load_config(self, config_path: str = None) -> dict: 設定ファイルはlambda上で読み込めないので、環境変数から読み込む形式で固定する。
    #     """
    #     設定ファイルの読み込み
        
    #     Args:
    #         config_path (str, optional): 設定ファイルのパス
            
    #     Returns:
    #         dict: 設定データ
    #     """
    #     try:
    #         if config_path is None:
    #             config_path = os.path.join(self.root_dir, 'config', 'settings.yaml')
                
    #         with open(config_path, 'r', encoding='utf-8-sig') as f:
    #             config = yaml.safe_load(f)
    #             logger.info(f"設定ファイルを読み込みました: {config_path}")
    #             return config
    #     except Exception as e:
    #         logger.error(f"設定ファイルの読み込みに失敗: {str(e)}")
    #         raise

    def _load_config(self, config_path: str = None) -> dict:
        """
        Lambda用に、設定ファイルではなく環境変数から構成を作成
        """
        try:
            config = {
                "sp_api": {
                    "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN", ""),
                    "lwa_app_id": os.getenv("SPAPI_LWA_APP_ID", ""),
                    "lwa_client_secret": os.getenv("SPAPI_LWA_CLIENT_SECRET", ""),
                    "aws_access_key": os.getenv("SPAPI_AWS_ACCESS_KEY", ""),
                    "aws_secret_key": os.getenv("SPAPI_AWS_SECRET_KEY", ""),
                    "role_arn": os.getenv("SPAPI_ROLE_ARN", ""),
                    "marketplace_ids": os.getenv("SPAPI_MARKETPLACE_IDS", "A1VC38T7YXB528").split(","),
                    "region": os.getenv("SPAPI_REGION", "jp"),
                    "requests_per_second": float(os.getenv("SPAPI_RPS", 2.0))
                }
            }
            return config
        except Exception as e:
            logger.error(f"環境変数からの設定取得に失敗: {str(e)}")
            raise

    
    def _merge_env_variables(self):
        """環境変数から認証情報を取得し、設定ファイルにマージする"""
        # SP APIの認証情報
        if 'sp_api' not in self.config:
            self.config['sp_api'] = {}
        
        # 環境変数から認証情報を取得
        env_vars = {
            'client_id': os.getenv('SP_API_CLIENT_ID'),
            'client_secret': os.getenv('SP_API_CLIENT_SECRET'),
            'refresh_token': os.getenv('SP_API_REFRESH_TOKEN'),
            'marketplace_id': os.getenv('SP_API_MARKETPLACE_ID')
        }
        
        # 設定ファイルに環境変数の値をマージ（環境変数が設定されている場合のみ）
        for key, value in env_vars.items():
            if value is not None:
                self.config['sp_api'][key] = value
        
        # リクエスト間隔の設定（デフォルト値）
        if 'request_delay' not in self.config['sp_api']:
            self.config['sp_api']['request_delay'] = 1.0  # デフォルトは1秒
            
        # 出力設定の初期化（なければデフォルト値を設定）
        if 'output' not in self.config['sp_api']:
            self.config['sp_api']['output'] = {
                'input_file': os.path.join(self.data_dir, 'keepa_seller_asin.csv'),
                'output_file': os.path.join(self.data_dir, 'sp_api_output.csv')
            }
        else:
            # 相対パスを絶対パスに変換
            for key in ['input_file', 'output_file']:
                if key in self.config['sp_api']['output']:
                    rel_path = self.config['sp_api']['output'][key]
                    if not os.path.isabs(rel_path):
                        self.config['sp_api']['output'][key] = os.path.join(self.data_dir, rel_path)
    
    def get_access_token(self) -> str:
        """
        アクセストークンの取得
        
        Returns:
            str: API呼び出しに使用するアクセストークン
        """
        token_url = 'https://api.amazon.com/auth/o2/token'
        token_data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.config['sp_api']['refresh_token'],
            'client_id': self.config['sp_api']['client_id'],
            'client_secret': self.config['sp_api']['client_secret']
        }
        try:
            response = requests.post(token_url, data=token_data).json()
            logger.info("アクセストークンを取得しました")
            return response['access_token']
        except Exception as e:
            logger.error(f"アクセストークンの取得に失敗: {str(e)}")
            raise
    
    def refresh_token_if_needed(self):
        """アクセストークンの有効期限をチェックし、必要に応じて更新する"""
        # 現在の時刻とトークン取得時刻の差を計算（秒）
        current_time = time.time()
        elapsed_time = current_time - self.token_timestamp
        
        # トークンの有効期限（55分=3300秒と設定）
        # 60分ではなく余裕を持たせて55分に設定
        token_lifetime = 3300  # 55分 * 60秒
        
        # 有効期限が近づいていれば更新
        if elapsed_time > token_lifetime:
            logger.info(f"アクセストークンの有効期限が近いため更新します (経過時間: {elapsed_time/60:.1f}分)")
            self.access_token = self.get_access_token()
            self.token_timestamp = time.time()
            logger.info("アクセストークンを更新しました")
            
    def make_request(self, url, method="GET", headers=None, params=None, json_data=None, max_retries=5, base_delay=2):
        """
        APIリクエストの実行（リトライロジック付き）
        
        Args:
            url (str): APIエンドポイントURL
            method (str): HTTPメソッド
            headers (dict): HTTPヘッダー
            params (dict): クエリパラメータ
            json_data (dict): JSONリクエストボディ
            max_retries (int): 最大リトライ回数
            base_delay (int): リトライ間の基本待機時間（秒）
            
        Returns:
            dict: APIレスポンス（JSONデコード済み）
        """
        # ヘッダーのデフォルト値
        if headers is None:
            headers = {}
            
        # アクセストークンを追加
        headers['x-amz-access-token'] = self.access_token
        
        # リトライループ
        for attempt in range(max_retries):
            try:
                # トークンの更新チェック
                self.refresh_token_if_needed()
                
                # レート制限に従って待機
                self.rate_limiter.wait_if_needed()
                
                # リクエスト実行
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data
                )
                
                # レート制限エラーの処理
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                    logger.warning(f"レート制限に達しました。{retry_after}秒待機します... (試行 {attempt+1}/{max_retries})")
                    print(f"⚠️ レート制限に達しました。{retry_after}秒待機します...")
                    time.sleep(retry_after)
                    continue
                
                # トークン期限切れの処理
                if response.status_code == 403:
                    response_text = response.text
                    if "expired" in response_text or "Unauthorized" in response_text:
                        logger.warning("トークン期限切れを検出。トークンを更新します。")
                        self.access_token = self.get_access_token()
                        self.token_timestamp = time.time()
                        headers['x-amz-access-token'] = self.access_token  # ヘッダーを更新
                        time.sleep(base_delay)
                        continue
                
                # その他のエラー
                if response.status_code != 200:
                    logger.error(f"APIエラー: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt)  # 指数バックオフ
                        logger.info(f"リトライ待機中... {wait_time}秒 (試行 {attempt+1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    return None
                
                # 成功したレスポンスをJSON形式で返す
                return response.json()
                
            except Exception as e:
                logger.error(f"リクエストエラー: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt)
                    logger.info(f"リトライ待機中... {wait_time}秒 (試行 {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error("最大リトライ回数に達しました")
                    raise
        
        return None


class AmazonProductAPI(AmazonSPAPI):
    """
    Amazon商品情報を取得するためのクラス
    
    SP-APIを使用して商品カタログ情報や価格情報を取得します。
    """

    # カタログAPI用フィルター
    MAX_RANKING = 80000
    
    # プライシングAPI用フィルター
    PRICE_MIN = 300
    PRICE_MAX = 20000
    TOTAL_SELLERS_MIN = 2
    TOTAL_SELLERS_MAX = 10
    FBA_SELLERS_MAX = 5
    AMAZON_SELLER_ALLOWED = False  # Amazonの出品がない場合のみ
    
    
    def __init__(self, config_path: str = None):
        """
        Amazon商品API初期化
        
        Args:
            config_path (str, optional): 設定ファイルのパス
        """
        super().__init__(config_path)
        
        # フィルター設定の上書き
        self.config['filters'] = {
            'price': {'min': self.PRICE_MIN, 'max': self.PRICE_MAX},
            'ranking': {'min': 1, 'max': self.MAX_RANKING},
            'sellers': {
                'total': {'min': self.TOTAL_SELLERS_MIN, 'max': self.TOTAL_SELLERS_MAX},
                'fba': {'min': None, 'max': self.FBA_SELLERS_MAX}
            }
        }
    
    def identify_code_type(self, code: str) -> Tuple[str, str]:
        """
        入力コードがJAN/EANかASINかを判定し、必要に応じて形式を修正する
        
        Args:
            code (str): 判定するコード
            
        Returns:
            tuple: (code_type, normalized_code)
                code_type: コードタイプ（'EAN'または'ASIN'）
                normalized_code: 正規化されたコード（EANの場合は13桁になるよう0埋め）
        """
        code = str(code).strip()
        
        # JANコードまたはEANコードの判定（数字のみで構成されている）
        if code.isdigit():
            # 既に13桁の場合はそのまま
            if len(code) == 13:
                return 'EAN', code
            # 12桁以下の場合は先頭に0を追加して13桁にする
            elif 5 <= len(code) <= 12:
                normalized_code = code.zfill(13)  # 0埋めして13桁にする
                print(f"コードを正規化: {code} → {normalized_code} (13桁EAN)")
                return 'EAN', normalized_code
        
        # ASINの判定（10桁の英数字 かつ 最初がB0で始まる）
        if (len(code) == 10 and 
            all(c.isalnum() for c in code) and 
            code.startswith('B0')):
            return 'ASIN', code
        
        # どちらにも当てはまらない場合
        raise ValueError(f"無効なコード形式: {code} - JAN/EANコード(5-13桁の数字)またはASIN(10桁かつB0で始まる英数字)である必要があります")
    
    def get_catalog_item_data(self, code: str, code_type: str) -> Dict:
        """
        コード（JANまたはASIN）から商品情報を取得
        
        Args:
            code (str): コード値
            code_type (str): コードタイプ ('EAN' または 'ASIN')
            
        Returns:
            dict or None: 商品情報。取得できない場合はNone
        """
        # APIエンドポイント（v2022-04-01）
        url = 'https://sellingpartnerapi-fe.amazon.com/catalog/2022-04-01/items'
        
        headers = {
            'x-amz-access-token': self.access_token,
            'Accept': 'application/json'
        }
        
        # クエリパラメータ
        params = {
            'marketplaceIds': self.config['sp_api']['marketplace_id'],
            'identifiers': code,
            'identifiersType': code_type,
            'includedData': 'attributes,dimensions,identifiers,images,productTypes,relationships,salesRanks,summaries'
        }
        
        logger.info(f"Catalog API v2022-04-01 リクエスト: {code_type} {code}")
        
        # APIリクエスト実行
        response_data = self.make_request(url, headers=headers, params=params)
        
        if not response_data:
            logger.warning(f"{code_type} {code} の商品情報が見つかりませんでした")
            return None
        
        # items配列が存在し、要素があるか確認
        if 'items' not in response_data or len(response_data['items']) == 0:
            logger.warning(f"{code_type} {code} の商品情報が見つかりませんでした")
            return None
        
        # 商品情報の取得（最初の要素を返す）
        item_data = response_data['items'][0]
        
        # ASIN情報を追加（JAN検索時に必要）
        if code_type == 'EAN' and 'asin' not in item_data:
            identifiers = item_data.get('identifiers', [])
            for identifier_set in identifiers:
                if identifier_set.get('identifierType') == 'ASIN':
                    identifier_values = identifier_set.get('identifiers', [])
                    for id_value in identifier_values:
                        if id_value.get('marketplaceId') == self.config['sp_api']['marketplace_id']:
                            item_data['asin'] = id_value.get('identifier')
                            break
        
        logger.info(f"Catalog API v2022-04-01 成功: {code_type} {code}")
        return item_data
    
    def parse_catalog_data(self, item: Dict) -> Dict:
        """
        Catalog APIのレスポンスから必要な情報を抽出して整形
        
        Args:
            item (dict): Catalog APIから取得した商品情報
            
        Returns:
            dict: 整形された商品基本情報
        """
        # 結果の初期化
        result = {
            '参考価格': None,
            'パッケージ最長辺': None,
            'パッケージ中辺': None,
            'パッケージ最短辺': None,
            'パッケージ重量': None,
            '現在ランキング': None
        }
        
        try:
            # 属性データの取得
            attributes = item.get('attributes', {})
            
            # 参考価格の取得
            try:
                list_price_attrs = attributes.get('list_price', [])
                
                if list_price_attrs:
                    for attr in list_price_attrs:
                        if isinstance(attr, dict) and attr.get('marketplace_id') == self.config['sp_api']['marketplace_id']:
                            if 'value' in attr and isinstance(attr['value'], (int, float)):
                                result['参考価格'] = float(attr['value'])
                                break
            except Exception as e:
                logger.error(f"参考価格の解析エラー: {str(e)}")
            
            # パッケージサイズと重量の取得
            dimensions = {}
            
            try:
                # dimensionsセクションを取得
                dimensions_data = item.get('dimensions', [])
                
                # マーケットプレースIDに一致するデータを探す
                for dim_obj in dimensions_data:
                    if dim_obj.get('marketplaceId') != self.config['sp_api']['marketplace_id']:
                        continue
                        
                    # まずpackageデータを確認し、なければitemデータを使用
                    for container_type in ['package', 'item']:
                        container = dim_obj.get(container_type, {})
                        if not container:
                            continue
                            
                        # 寸法情報（高さ、長さ、幅）を取得
                        for dim_type in ['height', 'length', 'width']:
                            dim_data = container.get(dim_type, {})
                            if dim_data and 'value' in dim_data and 'unit' in dim_data:
                                value = float(dim_data['value'])
                                unit = dim_data['unit'].lower()
                                # インチからcmへの変換
                                if unit in ['inches', 'inch']:
                                    value *= 2.54
                                dimensions[dim_type] = value
                        
                        # 重量情報を取得
                        weight_data = container.get('weight', {})
                        if weight_data and 'value' in weight_data and 'unit' in weight_data:
                            value = float(weight_data['value'])
                            unit = weight_data['unit'].lower()
                            # 単位変換
                            if unit in ['pounds', 'pound', 'lb', 'lbs']:
                                value *= 453.592  # ポンドからグラム
                            elif unit in ['kilograms', 'kg']:
                                value *= 1000  # キログラムからグラム
                            result['パッケージ重量'] = round(value, 2)
                        
                        # 寸法を取得できたら、次のマーケットプレースへ
                        if dimensions:
                            break
                    
                    # 何かしらのデータが取得できたらループを抜ける
                    if dimensions or result['パッケージ重量'] is not None:
                        break
                
                # パッケージサイズの計算（値を降順にソート）
                if dimensions:
                    dim_values = sorted([value for value in dimensions.values()], reverse=True)
                    if len(dim_values) >= 3:
                        result['パッケージ最長辺'] = round(dim_values[0], 2)
                        result['パッケージ中辺'] = round(dim_values[1], 2)
                        result['パッケージ最短辺'] = round(dim_values[2], 2)
                    elif len(dim_values) == 2:
                        result['パッケージ最長辺'] = round(dim_values[0], 2)
                        result['パッケージ中辺'] = round(dim_values[1], 2)
                    elif len(dim_values) == 1:
                        result['パッケージ最長辺'] = round(dim_values[0], 2)
            except Exception as e:
                logger.error(f"パッケージサイズの解析エラー: {str(e)}")
                
            # ランキング情報の取得
            try:
                sales_ranks = item.get('salesRanks', [])
                if sales_ranks:
                    for rank_category in sales_ranks:
                        # displayGroupRanksを確認
                        if 'displayGroupRanks' in rank_category:
                            display_ranks = rank_category.get('displayGroupRanks', [])
                            for rank_info in display_ranks:
                                if 'rank' in rank_info:
                                    try:
                                        result['現在ランキング'] = int(rank_info['rank'])
                                        break
                                    except (ValueError, TypeError):
                                        pass
                        
                        if result['現在ランキング'] is not None:
                            break
            except Exception as e:
                logger.error(f"ランキング情報の解析エラー: {str(e)}")
            
            # その他の必要な情報を追加
            try:
                # 商品サマリー情報の取得
                summaries = item.get('summaries', [])
                item_info = summaries[0] if summaries else {}
                
                # 商品名
                if '商品名' not in result and item_info.get('itemName'):
                    result['商品名'] = item_info.get('itemName')
                    
                # 商品画像
                if '画像URL' not in result and item_info.get('mainImage', {}).get('link'):
                    result['画像URL'] = item_info.get('mainImage', {}).get('link')

                # JAN(EAN)コードの取得
                if 'JAN' not in result:
                    identifiers = item.get('identifiers', [])
                    for identifier_set in identifiers:
                        if identifier_set.get('marketplaceId') == self.config['sp_api']['marketplace_id']:
                            for id_data in identifier_set.get('identifiers', []):
                                if id_data.get('identifierType') == 'EAN':
                                    result['JAN'] = id_data.get('identifier')
                                    break
                    
                # ブランド名を取得
                brand_attrs = attributes.get('brand', [])
                for attr in brand_attrs:
                    if isinstance(attr, dict) and attr.get('marketplace_id') == self.config['sp_api']['marketplace_id']:
                        if 'value' in attr:
                            result['ブランド名'] = attr['value']
                            break
                            
                # メーカー名を取得
                manufacturer_attrs = attributes.get('manufacturer', [])
                for attr in manufacturer_attrs:
                    if isinstance(attr, dict) and attr.get('marketplace_id') == self.config['sp_api']['marketplace_id']:
                        if 'value' in attr:
                            result['メーカー名'] = attr['value']
                            break
                            
                # カテゴリ情報
                if item.get('productTypes'):
                    for product_type in item.get('productTypes', []):
                        if product_type.get('marketplaceId') == self.config['sp_api']['marketplace_id']:
                            result['カテゴリー'] = product_type.get('productType', '')
                            break
            except Exception as e:
                logger.error(f"追加情報の解析エラー: {str(e)}")
            
        except Exception as e:
            logger.error(f"カタログデータの解析エラー: {str(e)}")
            logger.error(traceback.format_exc())
        
        return result
    
    def process_catalog_data(self, input_file=None, output_file=None, batch_size=50):
        """
        カタログAPI専用処理メソッド
        
        Args:
            input_file: 入力CSVファイル
            output_file: 出力CSVファイル
            batch_size: バッチサイズ
            
        Returns:
            list: カタログ情報を含む商品データのリスト
        """
        # 実行時間計測開始
        start_time = time.time()
        
        # 入出力ファイル名の設定
        if input_file is None:
            input_file = self.config.get('catalog_api', {}).get('input_file', 'default_input.csv')
        
        if output_file is None:
            output_file = self.config.get('catalog_api', {}).get('output_file', 'catalog_output.csv')
        
        # CSVファイルからコードを読み込む
        codes, duplicates_count = self.load_codes_from_file(input_file)
        total_codes = len(codes)
        
        print(f"\n全{total_codes}件のカタログ情報取得を開始します...")
        
        # コード変換とCatalog情報取得
        catalog_data = self.process_codes_and_get_catalog_data(codes, batch_size, self.MAX_RANKING)
        print(f"✅ カタログAPI処理完了: {len(catalog_data)}/{total_codes}件の商品情報を取得")
        
        # 結果の保存
        if catalog_data:
            self.save_results(catalog_data, output_file)
            print(f"カタログデータを保存しました: {output_file} ({len(catalog_data)}件)")
        
        # 処理時間の表示
        elapsed = time.time() - start_time
        print(f"\nカタログAPI処理完了！実行時間: {elapsed:.2f}秒")
        
        return catalog_data

    def process_pricing_data(self, catalog_data=None, input_file=None, output_file=None, batch_size=20):
        """
        プライシングAPI専用処理メソッド
        
        Args:
            catalog_data: カタログAPIで取得した商品データ（直接渡す場合）
            input_file: カタログデータを読み込むファイル（catalog_dataがNoneの場合）
            output_file: 出力CSVファイル
            batch_size: バッチサイズ
            
        Returns:
            tuple: (全商品データ, フィルタリング済みデータ)
        """
        # 実行時間計測開始
        start_time = time.time()
        
        # 入出力ファイル名の設定
        if input_file is None:
            input_file = self.config.get('pricing_api', {}).get('input_file', 'catalog_output.csv')
        
        if output_file is None:
            output_file = self.config.get('pricing_api', {}).get('output_file', 'pricing_output.csv')
        
        # catalog_dataが指定されていない場合は、input_fileから読み込む
        if catalog_data is None:
            print(f"カタログデータをファイルから読み込みます: {input_file}")
            try:
                # CSVからカタログデータを読み込む
                catalog_data = []
                with open(input_file, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        catalog_data.append(row)
                
                print(f"読み込み成功: {len(catalog_data)}件のカタログデータを取得")
                
                if not catalog_data:
                    raise ValueError("カタログデータが空です")
            except Exception as e:
                logger.error(f"カタログデータの読み込みに失敗: {str(e)}")
                print(f"❌ エラー: カタログデータの読み込みに失敗しました - {str(e)}")
                return [], []
        
        # ASINリスト抽出
        asins = [item['ASIN'] for item in catalog_data if 'ASIN' in item]
        total_asins = len(asins)
        
        if total_asins == 0:
            logger.error("有効なASINがありません")
            print("❌ エラー: 有効なASINがありません")
            return [], []
        
        print(f"\n全{total_asins}件の価格情報取得を開始します...")
        
        # Pricing APIで価格情報を取得
        pricing_data = self.get_pricing_data_batch(asins, batch_size)
        
        # 結果を結合
        final_results = []
        for catalog_item in catalog_data:
            if 'ASIN' not in catalog_item:
                continue
                
            asin = catalog_item['ASIN']
            pricing_item = next((p for p in pricing_data if p['ASIN'] == asin), None)
            
            if pricing_item:
                # カタログデータと価格情報を結合
                result = {**catalog_item, **pricing_item}
                final_results.append(result)
                print(f"✓ {asin}: 商品情報の結合に成功")
            else:
                # 価格情報がない場合はカタログデータのみ使用
                print(f"⚠️ {asin}: 価格情報がありません")
                final_results.append(catalog_item)
        
        print(f"✅ Pricing API処理完了: {len(final_results)}/{len(catalog_data)}件の商品情報を取得")
        
        # フィルタリング
        filtered_data = self.filter_products(final_results)
        
        # 結果の保存
        if final_results:
            self.save_results(final_results, output_file)
            print(f"全商品データを保存しました: {output_file} ({len(final_results)}件)")
            
            # フィルタリング後の出力ファイル名
            filtered_output = self.config.get('pricing_api', {}).get('filtered_output_file', 
                                            output_file.replace('.csv', '_filtered.csv'))
            
            if filtered_data:
                self.save_results(filtered_data, filtered_output)
                print(f"フィルタリング後のデータを保存しました: {filtered_output} ({len(filtered_data)}件)")
        
        # 処理時間の表示
        elapsed = time.time() - start_time
        print(f"\nプライシングAPI処理完了！実行時間: {elapsed:.2f}秒")
        
        return final_results, filtered_data

    def get_pricing_data_batch(self, asins: list, batch_size: int = 20) -> list:
        """
        商品価格設定API v2022-05-01を使用してバッチ処理で複数ASINの価格情報を一度に取得する
        
        Args:
            asins (list): 処理するASINのリスト
            batch_size (int): 1回のAPIリクエストで処理するASIN数（最大20）
            
        Returns:
            list: 商品価格情報のリスト
        """
        # トークンの更新チェック
        self.refresh_token_if_needed()
        
        # バッチサイズの上限を20に制限（API制限）
        batch_size = min(batch_size, 20)
        
        results = []
        
        # ASINをバッチに分割
        asin_batches = [asins[i:i+batch_size] for i in range(0, len(asins), batch_size)]
        
        for batch_idx, batch in enumerate(asin_batches, 1):
            print(f"Pricing APIバッチ処理中: {batch_idx}/{len(asin_batches)} ({len(batch)}件)")
            
            # このバッチが成功するまで繰り返す
            retry_count = 0
            max_retries = 5  # 最大再試行回数
            batch_success = False
            
            while not batch_success and retry_count < max_retries:
                try:
                    # バッチリクエストの構築
                    requests_data = []
                    for asin in batch:
                        requests_data.append({
                            "asin": asin,
                            "marketplaceId": self.config['sp_api']['marketplace_id'],
                            "includedData": [
                                "featuredBuyingOptions",
                                "referencePrices",
                                "lowestPricedOffers"
                            ],
                            "lowestPricedOffersInputs": [{
                                "itemCondition": "New",
                                "offerType": "Consumer"
                            }],
                            "uri": "/products/pricing/2022-05-01/items/competitiveSummary",
                            "method": "GET"
                        })
                    
                    # API エンドポイントとヘッダーの設定
                    url = 'https://sellingpartnerapi-fe.amazon.com/batches/products/pricing/2022-05-01/items/competitiveSummary'
                    headers = {
                        'x-amz-access-token': self.access_token,
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                    
                    # バッチリクエストの送信
                    response = requests.post(url, headers=headers, json={"requests": requests_data})
                    
                    # レート制限の場合は待機して再試行(30.3秒待機の根拠は1秒あたりのレート制限：0.033req/1sから逆算　※待機時間 = 1/0.033 = 30.3秒)
                    if response.status_code == 429:
                        wait_time = int(response.headers.get('Retry-After', 31))
                        logger.warning(f"レート制限に達しました。{wait_time}秒待機して再試行します... (試行 {retry_count+1}/{max_retries})")
                        print(f"⚠️ レート制限に達しました。{wait_time}秒待機して再試行します...")
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                        
                    # その他のエラー処理
                    if response.status_code != 200:
                        logger.error(f"Pricing API バッチリクエストエラー: {response.status_code} - {response.text}")
                        # トークン期限切れの場合はトークンを更新して再試行
                        if response.status_code == 403:
                            response_text = response.text
                            if "expired" in response_text or "Unauthorized" in response_text:
                                logger.warning("トークン期限切れを検出。トークンを更新します。")
                                self.access_token = self.get_access_token()
                                self.token_timestamp = time.time()
                                headers['x-amz-access-token'] = self.access_token  # ヘッダーを更新
                                retry_count += 1
                                continue
                        
                        # その他のエラーは最大再試行回数まで試す
                        retry_count += 1
                        time.sleep(2)  # エラー時の待機
                        continue
                    
                    # 成功した場合の処理
                    response_data = response.json()
                    batch_results = self.parse_pricing_batch_response(response_data, batch)
                    results.extend(batch_results)
                    
                    # このバッチは成功
                    batch_success = True
                    
                except Exception as e:
                    logger.error(f"Pricing APIバッチ処理エラー: {str(e)}")
                    logger.error(traceback.format_exc())
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(2)  # 例外発生時の待機
                    
            # バッチが最大再試行回数を超えても成功しなかった場合
            if not batch_success:
                logger.error(f"バッチ {batch_idx}/{len(asin_batches)} は最大再試行回数を超えても成功しませんでした")
                print(f"❌ バッチ {batch_idx}/{len(asin_batches)} の処理に失敗しました")
            
            # バッチ間の待機（レート制限対策）- 最後のバッチ以外かつLambda環境でない場合
            if batch_idx < len(asin_batches):
                # Lambda環境かどうかをチェック
                is_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
                
                if not is_lambda:  # Lambda環境でない場合のみ待機
                    batch_wait_time = self.config['sp_api'].get('batch_delay', 31.0)
                    print(f"{batch_wait_time}秒間待機中...")
                    time.sleep(batch_wait_time)
                else:
                    print("Lambda環境のため待機をスキップします")
        
        return results
    
    def parse_pricing_batch_response(self, response_data, asins):
        """
        バッチ処理のレスポンスから価格情報を解析する
        
        Args:
            response_data (dict): APIレスポンスデータ
            asins (list): リクエストに含まれていたASINのリスト（順序の対応付けのため）
            
        Returns:
            list: 解析された価格情報のリスト
        """
        results = []
        
        # レスポンスに "responses" キーがあることを確認
        if "responses" not in response_data:
            logger.error("Pricing APIレスポンスに 'responses' キーがありません")
            return results
            
        responses = response_data["responses"]
        
        # ASINとレスポンスの対応付け
        asin_to_response = {}
        for i, response in enumerate(responses):
            if i < len(asins):
                asin_to_response[asins[i]] = response
            else:
                logger.warning(f"レスポンス数がASIN数を超えています: {len(responses)} > {len(asins)}")
        
        # 各ASINに対する結果を処理
        for asin in asins:
            result = {
                'ASIN': asin,
                'Amazon価格': None,
                'カート価格': None,
                'カート価格送料': None,
                'カート価格のポイント': None,
                'カートセラーID': None,
                'FBA最安値': None,
                'FBA最安値のポイント': None,
                '自己発送最安値': None,
                '自己発送最安値の送料': None,
                '自己発送最安値のポイント': None,
                'Amazon本体有無1': False,
                'FBA数': 0,
                '自己発送数': 0,
                '新品総出品者数': 0,
                'FBA最安値出品者数': 0,
                '自己発送最安値出品者数': 0
            }
            
            # 対応するレスポンスが存在するか確認
            if asin not in asin_to_response:
                logger.warning(f"ASIN {asin} に対応するレスポンスが見つかりません")
                results.append(result)
                continue
                
            response = asin_to_response[asin]
            
            # レスポンスにエラーがないか確認
            if "statusCode" in response and response["statusCode"] != 200:
                logger.warning(f"ASIN {asin} のレスポンスにエラー: {response.get('body', {}).get('errors', [])}")
                results.append(result)
                continue
                
            # レスポンスボディを取得
            if "body" not in response:
                logger.warning(f"ASIN {asin} のレスポンスに 'body' キーがありません")
                results.append(result)
                continue
                
            body = response["body"]
            
            # featuredBuyingOptions（カート価格情報）の処理
            if "featuredBuyingOptions" in body:
                featured_options = body["featuredBuyingOptions"]
                for option in featured_options:
                    # 新品のみを対象
                    if option.get("buyingOptionType") == "New":
                        # segmentedFeaturedOffers から情報を取得
                        segmented_offers = option.get("segmentedFeaturedOffers", [])
                        if segmented_offers:
                            # 最初のオファーを使用（通常はカートボックス）
                            featured_offer = segmented_offers[0]
                            
                            # カートセラーID
                            result["カートセラーID"] = featured_offer.get("sellerId", "")
                            
                            # カート価格
                            listing_price = featured_offer.get("listingPrice", {})
                            result["カート価格"] = listing_price.get("amount", None)
                            
                            # 送料情報
                            shipping_options = featured_offer.get("shippingOptions", [])
                            if shipping_options:
                                # デフォルトの送料を探す
                                for ship_option in shipping_options:
                                    if ship_option.get("shippingOptionType") == "DEFAULT":
                                        # 0の場合はNoneに変更
                                        shipping_amount = ship_option.get("price", {}).get("amount", 0)
                                        result["カート価格送料"] = shipping_amount if shipping_amount != 0 else None
                                        break
                            else:
                                result["カート価格送料"] = None  # 送料情報がない場合もNone

                            # ポイント情報
                            points = featured_offer.get("points", {})
                            points_value = points.get("pointsNumber", 0)
                            # 0の場合はNoneに変更
                            result["カート価格のポイント"] = -points_value if points_value != 0 else None
                            
                            break
            
            # lowestPricedOffers（最安値情報）の処理
            if "lowestPricedOffers" in body:
                offers_list = body["lowestPricedOffers"]
                
                # FBA出品とマーチャント出品をグループ化
                fba_offers = []
                merchant_offers = []
                
                # レスポンスからlowestPricedOffersを抽出（リスト形式）
                for offer_group in offers_list:
                    # "New"条件の商品だけを処理
                    if "lowestPricedOffersInput" in offer_group and offer_group["lowestPricedOffersInput"]["itemCondition"] == "New":
                        condition_offers = offer_group.get("offers", [])
                        for offer in condition_offers:
                            # 販売者情報
                            seller_info = offer.get("seller", {})
                            seller_id = offer.get("sellerId", "")
                            is_amazon = seller_id == "AN1VRQENFRJN5"  # Amazonの販売者ID
                            
                            # 配送タイプの確認
                            is_fba = offer.get("fulfillmentType") == "AFN"
                            
                            # 価格情報
                            price_info = offer.get("listingPrice", {})
                            offer_price = price_info.get("amount", 0)
                            
                            # 送料情報
                            shipping_price = 0
                            shipping_options = offer.get("shippingOptions", [])
                            if shipping_options:
                                # デフォルトの送料を探す
                                for option in shipping_options:
                                    if option.get("shippingOptionType") == "DEFAULT":
                                        shipping_price = option.get("price", {}).get("amount", 0)
                                        break
                            
                            # ポイント情報
                            points = offer.get("points", {})
                            points_value = points.get("pointsNumber", 0)
                            
                            # Amazon情報
                            if is_amazon:
                                result["Amazon本体有無1"] = True
                                result["Amazon価格"] = offer_price
                            
                            # FBA/自己発送の分類
                            if is_fba:
                                result["FBA数"] += 1
                                fba_offers.append({
                                    "price": offer_price,
                                    "points": points_value,
                                    "seller_id": seller_id
                                })
                            else:
                                result["自己発送数"] += 1
                                merchant_offers.append({
                                    "price": offer_price,
                                    "shipping": shipping_price,
                                    "points": points_value,
                                    "seller_id": seller_id
                                })
                
                # 出品者数の合計
                result["新品総出品者数"] = result["FBA数"] + result["自己発送数"]
                
                # FBA最安値の計算
                if fba_offers:
                    min_fba_price = min(offer["price"] for offer in fba_offers)
                    min_fba_offers = [offer for offer in fba_offers if offer["price"] == min_fba_price]
                    result["FBA最安値"] = min_fba_price
                    # ポイント値が0の場合はNone
                    points_value = min_fba_offers[0]["points"]
                    result["FBA最安値のポイント"] = -points_value if points_value != 0 else None
                    result["FBA最安値出品者数"] = len(min_fba_offers)
                
                # 自己発送最安値の計算
                if merchant_offers:
                    min_merchant_total = min(offer["price"] + offer["shipping"] for offer in merchant_offers)
                    min_merchant_offers = [offer for offer in merchant_offers if offer["price"] + offer["shipping"] == min_merchant_total]
                    result["自己発送最安値"] = min_merchant_offers[0]["price"]
                    # 送料が0の場合はNone
                    shipping_value = min_merchant_offers[0]["shipping"]
                    result["自己発送最安値の送料"] = shipping_value if shipping_value != 0 else None
                    # ポイント値が0の場合はNone
                    points_value = min_merchant_offers[0]["points"]
                    result["自己発送最安値のポイント"] = -points_value if points_value != 0 else None
                    result["自己発送最安値出品者数"] = len(min_merchant_offers)
            
            results.append(result)
        
        return results
    
    def process_codes_and_get_catalog_data(self, codes: list, batch_size: int = 50, max_ranking: int = None) -> list:
        """
        コード変換とCatalog情報取得を一度のAPI呼び出しで行う統合メソッド
        
        Args:
            codes (list): 処理するコードのリスト（JANまたはASIN）
            batch_size (int): バッチサイズ（デフォルト: 50）
            max_ranking (int): 処理する最大ランキング値（これより大きいランキングの商品は除外）
            
        Returns:
            list: カタログ情報を含む商品データのリスト（ランキングフィルター適用済み）
        """
        # max_rankingパラメータが指定されていない場合はクラス変数を使用
        if max_ranking is None:
            max_ranking = self.MAX_RANKING

        catalog_results = []
        filtered_results = []  # ランキングでフィルターした結果を格納
        
        # コードのバッチ処理
        code_batches = [codes[i:i+batch_size] for i in range(0, len(codes), batch_size)]
        
        for batch_idx, batch in enumerate(code_batches, 1):
            print(f"コード処理バッチ: {batch_idx}/{len(code_batches)} ({len(batch)}件)")
            
            # トークンの更新チェック
            self.refresh_token_if_needed()
            
            # バッチ内の各コードを処理
            batch_results = []
            for code in batch:
                try:
                    # コードタイプの判定
                    code_type, normalized_code = self.identify_code_type(code)
                    
                    if code_type == 'EAN':
                        print(f"JAN/EAN: {code}の処理中...")
                        # JAN/EANコードでCatalog API v2022-04-01を呼び出す
                        item_data = self.get_catalog_item_data(normalized_code, 'EAN')
                        
                        if not item_data:
                            print(f"⚠️ JAN/EAN {code} の商品情報が見つかりません")
                            continue
                            
                        # ASINを取得
                        asin = item_data.get('asin')
                        if not asin:
                            print(f"⚠️ JAN/EAN {code} に対応するASINが見つかりません")
                            continue
                            
                        print(f"✓ JAN/EAN {code} → ASIN: {asin} 変換成功")
                            
                    else:
                        # 既にASINの場合は直接Catalog API v2022-04-01を呼び出す
                        print(f"ASIN: {code}の処理中...")
                        asin = normalized_code
                        item_data = self.get_catalog_item_data(asin, 'ASIN')
                        
                        if not item_data:
                            print(f"⚠️ ASIN {asin} の商品情報が見つかりません")
                            continue
                    
                    # カタログデータを解析してフォーマット
                    catalog_data = self.parse_catalog_data(item_data)
                    
                    # 基本情報を追加
                    result = {
                        'ASIN': asin,
                        '元コード': code,
                        'コードタイプ': code_type,
                        # catalog_dataの内容をマージ
                        **catalog_data
                    }
                    
                    batch_results.append(result)
                    
                    # ランキングが指定の値以下かチェック
                    ranking = result.get('現在ランキング')
                    if ranking is not None and ranking <= max_ranking:
                        filtered_results.append(result)
                        print(f"✅ {code}: 商品情報取得成功 (ランキング: {ranking})")
                    else:
                        print(f"⏭️ {code}: ランキング条件を満たさないため除外 (ランキング: {ranking})")
                    
                except Exception as e:
                    print(f"❌ {code}: 処理エラー - {str(e)}")
                    logger.error(f"コード処理エラー ({code}): {str(e)}")
                    logger.error(traceback.format_exc())
            
            # 結果を追加
            catalog_results.extend(batch_results)
        
        total_success = len(catalog_results)
        total_filtered = len(filtered_results)
        total_codes = len(codes)
        logger.info(f"コード処理とカタログ情報取得完了: {total_success}/{total_codes}件 (成功率: {total_success/total_codes*100:.1f}%)")
        
        percentage = (total_filtered/total_success*100) if total_success > 0 else 0
        logger.info(f"ランキングフィルター適用後: {total_filtered}/{total_success}件 (通過率: {percentage:.1f}%)")
        
        return filtered_results  # ランキングフィルター適用後の結果を返す
    
    def filter_product(self, product_data: Dict) -> Optional[Dict]:
        """商品データのフィルタリング（ハードコーディング版）"""
        try:
            conditions = {}
            
            # 価格条件 - 型変換追加
            conditions['価格範囲'] = lambda p: (
                p['カート価格'] is not None and 
                self.PRICE_MIN <= float(p['カート価格']) <= self.PRICE_MAX
            )
            
            # ランキング条件 - 型変換追加
            conditions['ランキング'] = lambda p: (
                p['現在ランキング'] is not None and 
                1 <= int(p['現在ランキング']) <= self.MAX_RANKING
            )
            
            # Amazonの出品有無条件
            conditions['Amazon出品なし'] = lambda p: not p['Amazon本体有無1']
            
            # 出品者数条件 - 型変換追加 (すでに修正済み)
            conditions['出品者数'] = lambda p: (
                p['新品総出品者数'] is not None and
                self.TOTAL_SELLERS_MIN <= int(p['新品総出品者数']) <= self.TOTAL_SELLERS_MAX
            )
            
            # FBA出品者数条件 - 型変換追加
            conditions['FBA出品者数'] = lambda p: (
                p['FBA数'] is not None and
                int(p['FBA数']) <= self.FBA_SELLERS_MAX
            )
            
            # 各条件をチェック - エラーハンドリング追加
            for condition_name, check in conditions.items():
                try:
                    if not check(product_data):
                        logger.info(f"フィルター除外 - {condition_name}: {product_data['ASIN']}")
                        return None
                except (ValueError, TypeError) as e:
                    # 型変換エラーなどが発生した場合はログ出力
                    logger.error(f"フィルター条件エラー: {condition_name} - {str(e)} - ASIN: {product_data.get('ASIN', 'Unknown')}")
                    # エラーの場合は条件を満たさないとする
                    return None
            
            return product_data
        except Exception as e:
            logger.error(f"フィルタリング中にエラー: {str(e)}")
            return None

    def filter_products(self, products: list) -> list:
        """
        商品リストをフィルタリングする
        
        Args:
            products (list): フィルタリング対象の商品リスト
            
        Returns:
            list: フィルタリング後の商品リスト
        """
        filtered_products = []
        
        for product in products:
            filtered_product = self.filter_product(product)
            if filtered_product:
                filtered_products.append(filtered_product)
        
        logger.info(f"フィルタリング: {len(products)}件中{len(filtered_products)}件が条件を満たしました")
        return filtered_products

    def load_codes_from_file(self, input_file: str) -> Tuple[List[str], int]:
        """
        CSVファイルからコードを読み込む（重複を除去し、価格が低い方を優先）
        
        Args:
            input_file (str): 入力CSVファイルのパス
            
        Returns:
            tuple: (codes, duplicates_count)
                codes: 処理対象のコードリスト
                duplicates_count: 除外された重複の数
        """
        try:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"入力ファイルが見つかりません: {input_file}")
                
            # CSVファイルの読み込み
            with open(input_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                # コード列の特定
                code_column = None
                for name in reader.fieldnames:
                    if name.upper() in ['CODE', 'JAN', 'EAN', 'ASIN', 'PRODUCT_CODE', 'JANコード']:
                        code_column = name
                        break
                        
                # 価格列の特定
                price_column = None
                for name in reader.fieldnames:
                    if name in ['価格', 'PRICE', '仕入価格', '仕入れ価格', 'COST']:
                        price_column = name
                        break
                
                if not code_column:
                    raise ValueError("有効なコード列(CODE/JAN/EAN/ASIN)がCSVに見つかりません")
                
                # すべての行を読み込む
                all_rows = []
                for row in reader:
                    if row[code_column].strip():
                        # 価格列がない場合や価格が空の場合は無限大とする
                        price = float('inf')
                        if price_column and row[price_column].strip():
                            try:
                                price = float(row[price_column].strip().replace(',', ''))
                            except (ValueError, TypeError):
                                pass
                        
                        all_rows.append({
                            'code': row[code_column].strip(),
                            'price': price,
                            'original_row': row
                        })
            
            # 重複を除去し、価格が小さい方を残す
            code_price_dict = {}  # {コード: (価格, 行インデックス)}
            
            for i, row_data in enumerate(all_rows):
                code = row_data['code']
                price = row_data['price']
                
                if code in code_price_dict:
                    current_price, _ = code_price_dict[code]
                    if price < current_price:  # 価格が小さい方を優先
                        code_price_dict[code] = (price, i)
                else:
                    code_price_dict[code] = (price, i)
            
            # 価格が小さい方を残したユニークなコードリスト
            unique_codes = []
            for code, (_, index) in code_price_dict.items():
                unique_codes.append(all_rows[index]['code'])
            
            # 重複数の計算
            duplicates_count = len(all_rows) - len(unique_codes)
            
            logger.info(f"{len(unique_codes)}件のコードを読み込みました（重複除外: {duplicates_count}件）")
            return unique_codes, duplicates_count
            
        except Exception as e:
            logger.error(f"コード読み込みエラー: {str(e)}")
            raise


    def save_results(self, results: List[Dict], output_file: str, append: bool = False) -> None:
        """
        結果をCSVファイルに保存する
        
        Args:
            results (List[Dict]): 保存する結果
            output_file (str): 出力ファイル名
            append (bool): Trueの場合、既存ファイルに追記する（初回以降のバッチで使用）
        """
        try:
            if not results:
                logger.warning(f"保存する結果がありません: {output_file}")
                return
                    
            if not os.path.isabs(output_file):
                output_file = os.path.join(self.data_dir, output_file)
                    
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            all_fields = set()
            for row in results:
                all_fields.update(row.keys())
            
            priority_fields = [
                "ASIN", "JAN", "商品名", "カテゴリー", "メーカー型番", "レビュー有無", 
                "メーカー名", "ブランド名", "総出品者数", "商品追跡日", 
                "商品発売日", "追跡開始からの経過日数", "アダルト商品対象", "画像URL",
                "30日間平均ランキング", "90日間平均ランキング", "180日間平均ランキング",
                "amazonURL", "KeepaURL", "バリエーションASIN",
                "参考価格", "パッケージ最長辺", "パッケージ中辺", "パッケージ最短辺", "パッケージ重量",
                "現在ランキング",
                "Amazon価格", "カート価格", "カート価格送料", "カート価格のポイント",
                "FBA最安値", "FBA最安値のポイント", "自己発送最安値", "自己発送最安値の送料", "自己発送最安値のポイント",
                "Amazon本体有無1", "FBA数", "自己発送数", "新品総出品者数", 
                "FBA最安値出品者数", "自己発送最安値出品者数",
                "元コード", "コードタイプ"
            ]
            
            fieldnames = []
            for field in priority_fields:
                if field in all_fields:
                    fieldnames.append(field)
                    all_fields.remove(field)
            remaining_fields = sorted(list(all_fields))
            fieldnames.extend(remaining_fields)
            
            normalized_results = []
            for row in results:
                normalized_row = {field: row.get(field, None) for field in fieldnames}
                if normalized_row.get('パッケージ重量') is not None:
                    try:
                        normalized_row['パッケージ重量'] = round(float(normalized_row['パッケージ重量']), 2)
                    except (ValueError, TypeError):
                        # 数値に変換できない場合はそのまま
                        pass
                normalized_results.append(normalized_row)
                
            
            mode = 'a' if append and os.path.exists(output_file) else 'w'
            write_header = (mode == 'w')
            with open(output_file, mode, encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                for row in normalized_results:
                    writer.writerow(row)
                        
            logger.info(f"結果を保存しました: {output_file} ({len(results)}件)")
            
        except Exception as e:
            logger.error(f"結果の保存中にエラー: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def process_and_analyze(self, input_file=None, output_file=None, batch_size=20, max_ranking=None):
        """
        商品データを処理・分析する統合メソッド
        
        Args:
            input_file (str, optional): 入力ファイルパス
            output_file (str, optional): 出力ファイルパス
            batch_size (int): APIリクエストのバッチサイズ
            max_ranking (int): 対象とする最大ランキング
            
        Returns:
            tuple: (全商品データ, フィルタリング済みデータ)
        """

        # max_rankingパラメータが指定されていない場合はクラス変数を使用
        if max_ranking is None:
            max_ranking = self.MAX_RANKING

        # 設定ファイルからデフォルト値を読み込む
        if input_file is None:
            input_file = self.config['sp_api']['output']['input_file']
        
        if output_file is None:
            output_file = self.config['sp_api']['output']['output_file']
        
        # 実行時間計測開始
        start_time = time.time()
        
        # CSVファイルからコードを読み込む
        codes, duplicates_count = self.load_codes_from_file(input_file)
        total_codes = len(codes)
        
        if duplicates_count > 0:
            print(f"ℹ️ 入力ファイル内の重複: {duplicates_count}件をスキップしました")
        
        print(f"\n全{total_codes}件の処理を開始します...")
        
        # ステップ1+2: コード変換とCatalog情報取得を一度に実行（統合）- ランキングフィルター付き
        print(f"\n📌 ステップ1+2: コード変換とCatalog情報取得を統合処理（最大ランキング: {max_ranking}）")
        catalog_data = self.process_codes_and_get_catalog_data(codes, batch_size, max_ranking)
        print(f"✅ 統合処理完了: {len(catalog_data)}/{total_codes}件の商品情報を取得（ランキング条件適用済み）")
        
        if not catalog_data:
            print("\n⚠️ ランキング条件を満たす商品がありませんでした。処理を終了します。")
            return [], []
        
        # ステップ3: Pricing APIで価格情報を取得して結合
        print("\n📌 ステップ3: Pricing APIで価格情報を取得して結合")
        complete_data = self.get_pricing_data_batch(
            [item['ASIN'] for item in catalog_data],
            batch_size
        )
        
        # 結果を結合
        final_results = []
        for catalog_item in catalog_data:
            asin = catalog_item['ASIN']
            
            # 対応する価格情報を探す
            pricing_data = next((p for p in complete_data if p['ASIN'] == asin), None)
            
            if pricing_data:
                # カタログデータと価格情報を結合
                result = {**catalog_item, **pricing_data}
                final_results.append(result)
                print(f"✅ {asin}: 商品情報の結合に成功")
            else:
                # 価格情報がない場合はカタログデータのみ使用
                print(f"⚠️ {asin}: 価格情報がありません")
                final_results.append(catalog_item)
        
        print(f"✅ Pricing API処理完了: {len(final_results)}/{len(catalog_data)}件の商品情報を取得")
        
        # ステップ4: フィルタリングと保存
        print("\n📌 ステップ4: フィルタリングと結果の保存")
        filtered_data = self.filter_products(final_results)
        
        # 結果の保存
        if final_results:
            self.save_results(final_results, output_file)
            print(f"全商品データを保存しました: {output_file} ({len(final_results)}件)")
            
            filtered_output = output_file.replace('.csv', '_filtered.csv')
            if filtered_data:
                self.save_results(filtered_data, filtered_output)
                print(f"フィルタリング後のデータを保存しました: {filtered_output} ({len(filtered_data)}件)")
        
        # 処理時間の表示
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"\n処理完了！実行時間: {elapsed:.2f}秒")
        
        # サマリーの表示
        print("\n==== 処理結果サマリー ====")
        print(f"総処理件数: {total_codes}")
        print(f"ランキング条件通過: {len(catalog_data)}")
        print(f"価格情報取得成功: {len(final_results)}")
        print(f"最終フィルタリング後: {len(filtered_data)}")
        
        return final_results, filtered_data