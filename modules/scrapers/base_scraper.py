# modules/scrapers/base_scraper.py
"""
BaseScraper - スクレイピングの基本機能を提供する基底クラス

このモジュールはスクレイピングの共通機能を持つ基底クラスを提供します。
設定ファイルの読み込み、ブラウザの初期化、ログ機能などの共通機能を集約しています。
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import logging
from dotenv import load_dotenv
from pathlib import Path
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import yaml
import json


class BaseScraper:
    """
    スクレイピングの基本機能を提供する基底クラス
    
    このクラスはスクレイピングに必要な以下の共通機能を提供します:
    - 設定ファイルの読み込み
    - ブラウザの初期化と設定
    - ログ機能
    - CSVファイル操作
    """

    def _load_config(self, config_path=None):
        """
        設定ファイルを読み込みます
        
        Args:
            config_path (str): 設定ファイルのパス
        
        Returns:
            dict: 設定データ
        """
        # デフォルトのパス
        if config_path is None:
            config_path = os.path.join(self.root_dir, 'config', 'settings.yaml')
        
        print(f"設定ファイルパス: {config_path}")
        
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path}")
                
            # YAMLファイルを読み込む
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # 環境変数から認証情報を取得して設定ファイルにマージ
            self._merge_env_variables(config)
            
            print("設定ファイルを正常に読み込みました")
            return config
        except Exception as e:
            print(f"設定ファイルの読み込みエラー: {str(e)}")
            # エラーを上位に伝播させる
            raise

    def _merge_env_variables(self, config):
        """環境変数から認証情報を取得し、設定ファイルにマージする"""
        # このメソッドは子クラスでオーバーライドして実装します
        pass

    def setup_logging(self):
        """ログ機能のセットアップ"""
        # すでに存在するハンドラを削除（重複を防ぐため）
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # ログファイルパスの設定
        log_filename = f'{self.site_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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
        print(f"ログファイル出力先: {log_file}")
        logging.info(f"ログ機能の初期化が完了しました: {log_file}")

    def _find_project_root(self):
        """
        プロジェクトのルートディレクトリを検出する
        """
        # 現在のファイルの絶対パスを取得
        current_dir = os.path.abspath(os.getcwd())
        
        # 親ディレクトリを探索
        path = Path(current_dir)
        while True:
            # .gitディレクトリがあればそれをルートとみなす
            if (path / '.git').exists():
                return str(path)
            
            # プロジェクトのルートを示す他のファイル/ディレクトリの存在チェック
            if (path / 'setup.py').exists() or (path / 'README.md').exists():
                return str(path)
            
            # これ以上上の階層がない場合は現在のディレクトリを返す
            if path.parent == path:
                return str(path)
            
            # 親ディレクトリへ
            path = path.parent

    def _setup_browser(self):
        """
        Seleniumのブラウザを設定します
        
        ChromeOptionsの設定とWebDriverの起動を行います
        """
        # Chromeオプション設定
        chrome_options = Options()
        if self.headless_mode:
            chrome_options.add_argument("--headless")  # ヘッドレスモード（画面表示なし）
        chrome_options.add_argument("--disable-gpu")  # GPU無効化（ヘッドレス時に推奨）
        chrome_options.add_argument("--window-size=1920x1080")  # ウィンドウサイズ設定
        
        try:
            # ドライバーパスが指定されている場合は直接使用
            if hasattr(self, 'driver_path') and self.driver_path:
                self.browser = webdriver.Chrome(service=Service(self.driver_path), options=chrome_options)
            # 指定がなければWebDriverManagerを使用
            else:
                self.browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
            
            self.wait = WebDriverWait(self.browser, 10)  # 要素が見つかるまで最大10秒待機
            print("ブラウザの初期化に成功しました")
        except Exception as e:
            print(f"ブラウザの初期化に失敗しました: {str(e)}")
            raise

    def _setup_session(self):
        """
        requestsのセッションを設定します
        
        Seleniumのブラウザから取得したCookieをrequestsセッションに設定します
        これによりログイン状態を維持したままBeautifulSoupでページ取得できます
        """
        # ブラウザのCookieを取得
        cookies = self.browser.get_cookies()
        
        # requestsセッションを作成
        self.session = requests.Session()
        
        # Seleniumから取得したCookieをrequestsセッションに追加
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'])

    def prepare_csv(self):
        """
        CSVファイルを初期化します
        
        既存のファイルがある場合は削除し、新しいヘッダー付きのCSVを作成します
        """
        # 既存ファイルがあれば削除
        if os.path.exists(self.csv_filename):
            os.remove(self.csv_filename)
        
        # カラム構成でCSVを初期化
        df = pd.DataFrame(columns=self.columns)
        df.to_csv(self.csv_filename, index=False, encoding="utf-8-sig")
        
        print(f"CSVファイル {self.csv_filename} を初期化しました")

    def save_to_csv(self, data):
        """
        データをCSVファイルに保存します
        
        Args:
            data (list): 保存するデータ（リストのリスト形式）
            
        Returns:
            bool: 保存成功時はTrue、失敗時はFalse
        """
        try:
            if data:
                # データフレームに変換
                df = pd.DataFrame(data, columns=self.columns)
                
                # CSVに追記（ヘッダーなし）
                df.to_csv(self.csv_filename, mode="a", index=False, header=False, encoding="utf-8-sig")
                return True
            else:
                print("保存するデータがありません")
                return False
        except Exception as e:
            print(f"CSV保存エラー: {str(e)}")
            return False

    def __init__(self, site_name, config_path=None, headless_mode=False):
        """
        BaseScraperの初期化
        
        Args:
            site_name (str): スクレイピングするサイト名（ログファイル名などに使用）
            config_path (str): 設定ファイルのパス（指定しない場合はデフォルト値を使用）
            headless_mode (bool): ブラウザを画面に表示せずに実行する場合はTrue
        """
        # サイト名の設定
        self.site_name = site_name
        
        # プロジェクトルートディレクトリの検出
        self.root_dir = self._find_project_root()
        
        # 環境変数の読み込み
        load_dotenv(os.path.join(self.root_dir, '.env'))
        
        # ディレクトリパスの設定
        self.data_dir = os.path.join(self.root_dir, 'data')
        self.log_dir = os.path.join(self.root_dir, 'logs')
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 設定ファイルの読み込み
        self.config = self._load_config(config_path)
        
        # 基本設定
        self.headless_mode = headless_mode
        self.browser = None
        self.wait = None
        self.session = None
        
        # 実行時間計測用
        self.start_time = None
        self.end_time = None

        # ログ設定を初期化時に行う
        self.setup_logging()

    def close(self):
        """
        ブラウザを閉じてリソースを解放します
        """
        if self.browser:
            self.browser.quit()
            print("ブラウザを閉じました")