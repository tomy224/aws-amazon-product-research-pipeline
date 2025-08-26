# modules/scrapers/netsea_scraper.py
"""
NetseaScraper - ネッシーから商品情報を取得するスクレイピングモジュール

このモジュールはネッシーのウェブサイトから商品情報を取得するための
クラスを提供します。BaseScraper を継承し、NETSEA固有の機能を実装しています。
"""
import os
import time
import json
import re
import logging
from .base_scraper import BaseScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


class NetseaScraper(BaseScraper):
    """
    ネッシーのWebサイトから商品情報をスクレイピングするクラス
    
    このクラスはSeleniumでログインと商品一覧ページの処理を行い、
    BeautifulSoupで個別商品ページの詳細情報を取得するハイブリッド方式を採用しています。
    """
    
    def _merge_env_variables(self, config):
        """環境変数から認証情報を取得し、設定ファイルにマージする"""
        # ネッシーの認証情報
        username = os.getenv('NETSEA_USERNAME')
        password = os.getenv('NETSEA_PASSWORD')
        
        if username and password:
            if 'scrapers' not in config:
                config['scrapers'] = {}
            if 'netsea' not in config['scrapers']:
                config['scrapers']['netsea'] = {}
            if 'login' not in config['scrapers']['netsea']:
                config['scrapers']['netsea']['login'] = {}
            
            config['scrapers']['netsea']['login']['username'] = username
            config['scrapers']['netsea']['login']['password'] = password
            print("ネッシーのログイン情報を環境変数から設定しました")
        else:
            print("警告: 環境変数からネッシーのログイン情報を取得できませんでした")
            
        # デフォルトの出力設定（なければ設定）
        if 'output' not in config['scrapers']['netsea']:
            config['scrapers']['netsea']['output'] = {
                'csv_filename': 'netsea_scraping.csv',
                'log_dir': 'logs'
            }
    
    def __init__(self, config_path=None, headless_mode=False):
        """
        NetseaScraperの初期化
        
        Args:
            config_path (str): 設定ファイルのパス（指定しない場合はデフォルト値を使用）
            headless_mode (bool): ブラウザを画面に表示せずに実行する場合はTrue
        """
        # 親クラス(BaseScraper)の初期化
        super().__init__('netsea', config_path, headless_mode)
        
        # NETSEA固有の設定
        self.netsea_config = self.config['scrapers']['netsea']
        self.base_url = "https://www.netsea.jp"
        
        # 出力設定（設定ファイルから読み込み）
        output_config = self.netsea_config.get('output', {})
        csv_filename = output_config.get('csv_filename', "netsea_scraping.csv")
        
        # CSVのフルパスを設定
        self.csv_filename = os.path.join(self.data_dir, csv_filename)
        print(f"CSVファイル出力先: {self.csv_filename}")
        
        # カラム設定（NETSEA固有）
        self.columns = ["卸業者名", "商品名", "JANコード", "価格", "セット数"]
        
        # ブラウザ設定
        self._setup_browser()
    
    def login(self, username=None, password=None):
        """
        ネッシーにログインします
        
        Args:
            username (str): ネッシーのログインユーザー名（指定なしの場合は設定ファイルから読み込み）
            password (str): ネッシーのパスワード（指定なしの場合は設定ファイルから読み込み）
            
        Returns:
            bool: ログイン成功時はTrue、失敗時はFalse
        """
        # ユーザー名とパスワードが指定されていない場合は設定ファイルから読み込む
        if username is None or password is None:
            username = self.netsea_config['login']['username']
            password = self.netsea_config['login']['password']
            
        try:
            # ログインページにアクセス
            self.browser.get(f"{self.base_url}/login")
            
            # ログインフォームに入力
            self.wait.until(EC.presence_of_element_located((By.NAME, "login_id"))).send_keys(username)
            self.browser.find_element(By.NAME, "password").send_keys(password)
            self.browser.find_element(By.NAME, "submit").click()
            
            # ログイン成功の確認（ログイン後ページの特定要素が表示されるか）
            self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "globalNav")))
            
            # ログイン後、Cookieを取得してrequestsセッションを準備
            self._setup_session()
            
            return True
        except Exception as e:
            print(f"ログインエラー: {str(e)}")
            return False
    
    def get_product_urls(self, page):
        """
        商品一覧ページから個別商品ページのURLリストを取得します
        
        Args:
            page (int): 取得する商品一覧ページの番号
            
        Returns:
            list: 個別商品ページのURLリスト
        """
        product_urls = []
        print(f"現在 {page} ページ目をスクレイピング中...")
        
        try:
            # 商品一覧ページにアクセス（ここではショップID 5984 を例として使用）
            url = f"{self.base_url}/shop/5984?sort=sales&page={page}"
            self.browser.get(url)
            
            # 商品リストが表示されるまで待機
            self.wait.until(EC.visibility_of_element_located((By.ID, "searchResultsArea")))
            
            # 商品リスト領域を取得
            product_grid = self.browser.find_element(By.ID, "searchResultsArea")
            
            # 各商品要素を取得
            products = product_grid.find_elements(By.CLASS_NAME, "showcaseType01")
            
            # 各商品のURLを取得
            for product in products:
                try:
                    title_block = product.find_element(By.CLASS_NAME, "showcaseHd")
                    product_url = title_block.find_element(By.TAG_NAME, "a").get_attribute("href")
                    product_urls.append(product_url)
                except Exception as e:
                    print(f"商品URL取得エラー（ページ {page}）: {str(e)}")
        
        except Exception as e:
            print(f"ページ {page} の商品リスト取得エラー: {str(e)}")
        
        return product_urls
    
    def get_product_urls_from_url_bs4(self, url, page_number):
        """
        指定されたURLから商品URLリストをBeautifulSoupで取得します
        
        Args:
            url (str): 商品一覧ページのURL
            page_number (int): ページ番号（表示用）
        
        Returns:
            list: 商品URLのリスト
        """
        product_urls = []
        print(f"現在 {page_number} ページ目をスクレイピング中... ({url})")
        
        try:
            # requestsを使ってページを取得
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 商品リスト領域を探す
            search_results = soup.find(id="searchResultsArea")
            if not search_results:
                print(f"ページ {page_number} の商品リスト領域が見つかりません")
                return product_urls
                
            # 各商品要素を取得
            products = search_results.find_all(class_="showcaseType01")
            
            # 各商品のURLを取得
            for product in products:
                try:
                    title_block = product.find(class_="showcaseHd")
                    if title_block and title_block.a:
                        product_url = title_block.a.get('href')
                        product_urls.append(product_url)
                except Exception as e:
                    print(f"商品URL取得エラー（ページ {page_number}）: {str(e)}")
        
        except Exception as e:
            print(f"ページ {page_number} の商品リスト取得エラー: {str(e)}")
        
        return product_urls
    
    def get_product_data(self, product_urls):
        """
        商品URLリストから詳細データを取得します
        
        Args:
            product_urls (list): 個別商品ページのURLリスト
            
        Returns:
            list: 各商品の詳細データのリスト
        """
        page_data = []
        
        for product_url in product_urls:
            try:
                # requestsとBeautifulSoupを使用してページを取得（高速化のため）
                response = self.session.get(product_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 卸業者名（ブランド）を抽出
                brand = "不明"  # デフォルト値
                for script in soup.find_all('script'):
                    if script.string and 'brand' in script.string:
                        brand_match = re.search(r'brand: [\\\'"](.+?)[\\\'"]', script.string)
                        if brand_match:
                            brand = brand_match.group(1)
                            break
                
                # JANコード（gtin13）を抽出 - 修正版
                jan_code = ""  # デフォルト値
                ld_json_scripts = soup.find_all('script', type='application/ld+json')
                for script in ld_json_scripts:
                    if script.string:
                        try:
                            # JSONデータをパース
                            product_data = json.loads(script.string)
                            # Product タイプのJSONで、gtin13が含まれている場合のみ
                            if product_data.get('@type') == 'Product' and 'gtin13' in product_data:
                                jan_code = product_data.get('gtin13', '')
                                break  # 見つかったら終了
                        except json.JSONDecodeError:
                            # JSON解析エラーの場合は正規表現で抽出を試みる
                            jan_match = re.search(r'"gtin13":"(\d+)"', script.string)
                            if jan_match:
                                jan_code = jan_match.group(1)
                                break  # 見つかったら終了
                
                # JANコードが取得できなかった場合、URLから抽出する従来の方法をフォールバックとして使用
                if not jan_code:
                    jan_code = product_url.split('/')[-1]
                    print(f"警告: gtin13が見つからないため、URL末尾をJANコードとして使用: {jan_code}")
                
                # 商品データを含むスクリプトを探す
                script_text = None
                for script in soup.find_all('script'):
                    if script.string and 'ecItemSetList' in script.string:
                        script_text = script.string
                        break
                
                if script_text:
                    # 正規表現で商品情報を抽出（商品名、価格、セット数）
                    items = re.findall(r'_id: .*?_name: [\'"](.+?)[\'"].*?_priceExcTax: [\'"](\d+)[\'"].*?_numInSet: [\'"](\d+)[\'"]', 
                                    script_text, re.DOTALL)
                    
                    # 抽出した情報を整形して追加
                    for name, price, num_in_set in items:
                        row_data = [
                            brand,          # 卸業者名
                            name,           # 商品名
                            jan_code,       # JANコード
                            price,          # 価格
                            num_in_set      # セット数
                        ]
                        page_data.append(row_data)
                else:
                    print(f"商品データが見つかりませんでした: {product_url}")
                    
            except Exception as e:
                print(f"個別ページ {product_url} のデータ取得エラー: {str(e)}")
        
        return page_data
    
    def scrape_pages(self, start_page=1, end_page=1):
        """
        指定したページ範囲の商品情報をスクレイピングします
        
        Args:
            start_page (int): 開始ページ番号
            end_page (int): 終了ページ番号
            
        Returns:
            int: 取得した商品データの総数
        """
        # 実行時間測定開始
        self.start_time = time.time()
        
        # CSVを初期化
        self.prepare_csv()
        
        total_items = 0
        
        # 指定ページ範囲をスクレイピング
        for page in range(start_page, end_page + 1):
            # 商品URLを取得
            product_urls = self.get_product_urls(page)
            
            # 商品データを取得
            page_data = self.get_product_data(product_urls)
            
            # 1ページごとにCSVに保存
            if page_data:
                self.save_to_csv(page_data)
                total_items += len(page_data)
                print(f"ページ {page} のデータ ({len(page_data)}件) をCSVに保存しました")
        
        # 実行時間測定終了
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        
        print(f"スクレイピング完了！ 合計{total_items}件のデータを取得しました")
        print(f"実行時間: {elapsed_time:.2f} 秒")
        
        return total_items
    
    def scrape_all_targets(self):
            """
            設定ファイルに指定されたすべてのターゲットページをスクレイピングします
            
            Returns:
                int: 取得した商品データの総数
            """
            # 実行時間測定開始
            self.start_time = time.time()
            
            # CSVを初期化
            self.prepare_csv()
            
            # ログイン（設定ファイルの情報を使用）
            if not self.login():
                print("ログインに失敗しました")
                return 0
            
            # 各ターゲットページの処理
            total_items = 0
            
            for target in self.netsea_config['target_pages']:
                print(f"\n===== {target['name']} の処理を開始 =====")
                
                # URLとページ範囲を取得
                base_url = target['url']
                sort = target.get('sort', '')  # ソート条件（指定がなければ空文字）
                start_page = target.get('start_page', 1)
                end_page = target.get('end_page', 1)
                
                # 各ページの処理
                for page in range(start_page, end_page + 1):
                    # URLを構築
                    page_url = f"{base_url}?sort={sort}&page={page}"
                    
                    # 商品URLを取得
                    product_urls = self.get_product_urls_from_url_bs4(page_url, page)
                    
                    # 商品データを取得
                    page_data = self.get_product_data(product_urls)
                    
                    # データを保存
                    if page_data:
                        self.save_to_csv(page_data)
                        total_items += len(page_data)
                        print(f"ページ {page} のデータ ({len(page_data)}件) をCSVに保存しました")
            
            # 実行時間測定終了
            self.end_time = time.time()
            elapsed_time = self.end_time - self.start_time
            
            print(f"\n===== スクレイピング完了 - 合計 {total_items} 件 =====")
            print(f"実行時間: {elapsed_time:.2f} 秒")
            
            return total_items