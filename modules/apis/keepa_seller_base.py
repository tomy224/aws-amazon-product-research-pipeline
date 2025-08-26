#!/usr/bin/env python
# coding: utf-8

"""
Keepa API アクセスの共通基底クラス

このモジュールは、Keepa APIへのアクセスに関する共通機能を提供する
基底クラスを実装します。セラー情報取得やASIN一覧取得などの
具体的な機能は、このクラスを継承して実装します。
"""

import requests
import pandas as pd
import time
import os
import yaml
import logging
from datetime import datetime
import traceback
import dotenv
from pathlib import Path

# モジュールのインポート
from modules.utils.logger_utils import get_logger, log_function_call
from modules.utils.file_utils import find_project_root, load_yaml_config

# ロガーの取得
logger = get_logger(__name__)


class BaseKeepaApi:
    """
    Keepa APIへのアクセスに関する共通機能を提供する基底クラス
    
    このクラスは、以下の共通機能を提供します：
    - 初期化と設定ファイルの読み込み
    - API制限の監視と制御
    - 共通ユーティリティ関数（時間変換など）
    - ファイル入出力
    """
    
    def __init__(self, module_name="keepa", config_path=None):
        """
        BaseKeepaApiの初期化
        
        Args:
            module_name (str): モジュール名（ログファイル名のプレフィックスに使用）
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
        
        # モジュール名を保存（ログファイル名などに使用）
        self.module_name = module_name
        
        # 設定ファイルの読み込み
        self.config = load_yaml_config(config_path)
        
        # 環境変数から認証情報を取得して設定ファイルにマージ
        self._merge_env_variables()
        
        # Keepa APIキーの設定
        self.api_key = self.config['keepa_api']['api_key']
        
        # API URLの設定
        self.api_url = "https://api.keepa.com"
        
        # ログ設定
        self._setup_logging()
        
        # API制限用のカウンター
        self.token_counter = 0
        self.last_request_time = None

    def _merge_env_variables(self):
        """環境変数から認証情報を取得し、設定ファイルにマージする"""
        # Keepa APIキーを環境変数から取得
        api_key = os.getenv('KEEPA_API_KEY')
        
        if api_key:
            self.config['keepa_api']['api_key'] = api_key
            print("Keepa APIキーを環境変数から設定しました")
        else:
            # 環境変数に設定されていない場合は設定ファイルの値を使用
            print("環境変数からKeepa APIキーが取得できません。設定ファイルの値を使用します。")
        
        # keepa_seller 設定の初期化（なければデフォルト値を設定）
        if 'keepa_seller' not in self.config:
            self.config['keepa_seller'] = {}
                
        if 'output' not in self.config['keepa_seller']:
            self.config['keepa_seller']['output'] = {
                'input_file': 'seller_ids.csv',
                'output_file': 'keepa_seller_output.csv'
            }
            
    def _setup_logging(self):
        """ログ機能のセットアップ"""
        # すでに存在するハンドラを削除（重複を防ぐため）
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # ログファイルパスの設定
        log_filename = f'{self.module_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        log_file = os.path.join(self.log_dir, log_filename)
        
        # 基本設定
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )
        
        # コンソールにもログを出力
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        
        # ログファイルの場所を明示的に表示
        print(f"ログファイル: {log_file}")
        logging.info(f"ログ機能の初期化が完了しました: {log_file}")

    def _check_api_tokens(self, required_tokens=1):
        """
        API制限をチェックし、必要なトークン数が利用可能かどうかを確認する
        
        Args:
            required_tokens (int): 必要なトークン数
            
        Returns:
            bool: トークンが利用可能な場合はTrue
        """
        # 前回のリクエストから経過した時間を計算
        current_time = time.time()
        if self.last_request_time:
            elapsed_seconds = current_time - self.last_request_time
            # 1分あたり100トークンが回復する（Keepa APIの仕様）
            # 1秒あたり約1.67トークン
            recovered_tokens = int(elapsed_seconds * 1.67)
            self.token_counter = max(0, self.token_counter - recovered_tokens)
        
        # 現在のトークン数を取得
        try:
            response = requests.get(
                f"{self.api_url}/token",
                params={"key": self.api_key}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.token_counter = data.get('tokensLeft', 0)
                logging.info(f"残りトークン数: {self.token_counter}")
                
                # 十分なトークンがあるかチェック
                if self.token_counter < required_tokens:
                    wait_time = int((required_tokens - self.token_counter) / 1.67) + 1
                    logging.warning(f"トークン不足。{wait_time}秒待機します...")
                    print(f"⚠️ トークン不足。{wait_time}秒待機します...")
                    time.sleep(wait_time)
                    # 再チェック
                    return self._check_api_tokens(required_tokens)
                
                self.last_request_time = current_time
                return True
            else:
                logging.error(f"トークン取得エラー: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"トークンチェックエラー: {str(e)}")
            return False

    def _format_keepa_time(self, keepa_time):
        """
        Keepaのタイムスタンプをフォーマットする
        
        Args:
            keepa_time (int): Keepaのタイムスタンプ（分単位）
            
        Returns:
            str: フォーマットした日時文字列
        """
        if not keepa_time:
            return ''
            
        try:
            # Keepaの時間はUnixエポックからの分数で表される
            # Keepaのベースタイム（2011年1月1日）をミリ秒に変換
            keepa_base_time = 1293840000  # 2011年1月1日の秒数
            unix_time = (keepa_time * 60) + keepa_base_time
            dt = datetime.fromtimestamp(unix_time)
            return dt.strftime('%Y-%m-%d %H:%M')
        except Exception as e:
            logging.error(f"日付変換エラー: {str(e)}")
            return str(keepa_time)

    @log_function_call
    def load_seller_ids(self, input_file=None):
        """
        セラーIDリストを読み込む
        
        Args:
            input_file (str): 入力ファイルのパス（指定なしの場合は設定ファイルから読み込み）
            
        Returns:
            list: セラーIDのリスト（正規表現でフィルタリング済み）
        """
        # 入力ファイル名の設定
        if input_file is None:
            input_file = os.path.join(
                self.data_dir, 
                self.config['keepa_seller']['output']['input_file']
            )
            
        seller_ids = []
        
        try:
            # CSVファイルの存在確認
            if not os.path.exists(input_file):
                error_msg = f"入力ファイルが見つかりません: {input_file}"
                logging.error(error_msg)
                raise FileNotFoundError(error_msg)
                
            # CSVファイルからセラーIDを読み込み
            df = pd.read_csv(input_file, encoding='utf-8-sig')
            
            # セラーID列の特定
            seller_column = None
            
            # 検索対象の列名リスト（「カートセラーID」を追加）
            column_candidates = ['セラーID', 'カートセラーID']
            
            # まず特定の列名を探す
            for col_name in column_candidates:
                if col_name in df.columns:
                    seller_column = col_name
                    break
                    
            # 特定の列名が見つからなければ、seller や id を含む列名を探す
            if not seller_column:
                for col in df.columns:
                    if 'seller' in col.lower() or 'id' in col.lower():
                        seller_column = col
                        break
                    
            # それでも見つからなければ最初の列を使用
            if not seller_column:
                # 最初の列をセラーID列として使用
                seller_column = df.columns[0]
                logging.warning(f"セラーID列が特定できなかったため、最初の列 '{seller_column}' を使用します")
            
            # セラーIDリストの取得と正規表現フィルタリング
            import re
            
            # 有効なセラーIDパターン（A + 12-14桁の英数字）
            seller_pattern = re.compile(r'^A[0-9A-Z]{12,14}$')
            
            # Amazonのセラーid
            amazon_seller_id = 'A1VC38T7YXB528'
            
            # 列からセラーIDを抽出し、正規表現でフィルタリング
            valid_seller_ids = []
            for seller_id in df[seller_column].dropna().unique():
                seller_id = str(seller_id).strip()
                # 正規表現にマッチし、かつAmazonのセラーIDでない場合のみ追加
                if seller_pattern.match(seller_id) and seller_id != amazon_seller_id:
                    valid_seller_ids.append(seller_id)
            
            # 重複を排除
            seller_ids = list(set(valid_seller_ids))
            
            logging.info(f"{len(seller_ids)}件の有効なセラーIDを読み込みました（元の候補: {len(df[seller_column].dropna().unique())}件）")
            print(f"📝 {len(seller_ids)}件の有効なセラーIDを読み込みました（列名: {seller_column}）")
            
            return seller_ids
            
        except Exception as e:
            error_msg = f"セラーIDの読み込み中にエラーが発生: {str(e)}"
            logging.error(error_msg)
            raise

    @log_function_call
    def save_to_csv(self, data, output_file, columns=None):
        """
        データをCSVファイルに保存する
        
        Args:
            data (list): 保存するデータ（辞書のリスト）
            output_file (str): 出力ファイルのパス
            columns (list, optional): 出力するカラムのリスト。指定がない場合はすべてのカラムを出力
        """
        try:
            if not data:
                logging.warning("保存するデータがありません")
                return
            
            # 出力ディレクトリの確認
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                
            # DataFrameに変換
            df = pd.DataFrame(data)
            
            # 指定されたカラムがあれば、そのカラムだけを選択
            if columns and all(col in df.columns for col in columns):
                df = df[columns]
            
            # CSVに保存
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logging.info(f"データを保存しました: {output_file} ({len(df)}件)")
            print(f"✅ {len(df)}件のデータを {output_file} に保存しました")
            
        except Exception as e:
            error_msg = f"データの保存中にエラーが発生: {str(e)}"
            logging.error(error_msg)
            print(f"❌ {error_msg}")
            raise