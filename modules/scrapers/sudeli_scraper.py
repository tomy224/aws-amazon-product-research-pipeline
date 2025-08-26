# modules/scrapers/sudeli_scraper.py
"""
SudeliScraper - スーパーデリバリーから商品情報を取得するスクレイピングモジュール

このモジュールはスーパーデリバリーのウェブサイトから商品情報を取得するための
クラスを提供します。BaseScraper を継承し、スーパーデリバリー固有の機能を実装しています。
"""
import os
import time
import re
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper


class SudeliScraper(BaseScraper):
    """
    スーパーデリバリーのWebサイトから商品情報をスクレイピングするクラス
    
    商品一覧ページから直接情報を取得するため、個別の商品ページにアクセスする必要がありません。
    """
    
    def _merge_env_variables(self, config):
        """環境変数から認証情報を取得し、設定ファイルにマージする"""
        # スーパーデリバリーの認証情報
        username = os.getenv('SUDELI_USERNAME')
        password = os.getenv('SUDELI_PASSWORD')
        
        if username and password:
            if 'scrapers' not in config:
                config['scrapers'] = {}
            if 'sudeli' not in config['scrapers']:
                config['scrapers']['sudeli'] = {}
            if 'login' not in config['scrapers']['sudeli']:
                config['scrapers']['sudeli']['login'] = {}
            
            config['scrapers']['sudeli']['login']['username'] = username
            config['scrapers']['sudeli']['login']['password'] = password
            print("スーパーデリバリーのログイン情報を環境変数から設定しました")
        else:
            print("警告: 環境変数からスーパーデリバリーのログイン情報を取得できませんでした")
            
        # デフォルトの出力設定（なければ設定）
        if 'output' not in config['scrapers']['sudeli']:
            config['scrapers']['sudeli']['output'] = {
                'csv_filename': 'sudeli_scraping.csv',
                'log_dir': 'logs'
            }
    
    def __init__(self, config_path=None, headless_mode=False):
        """
        SudeliScraperの初期化
        
        Args:
            config_path (str): 設定ファイルのパス（指定しない場合はデフォルト値を使用）
            headless_mode (bool): ブラウザを画面に表示せずに実行する場合はTrue
        """
        # 親クラス(BaseScraper)の初期化
        super().__init__('sudeli', config_path, headless_mode)
        
        # SUDELI固有の設定
        self.sudeli_config = self.config['scrapers']['sudeli']
        self.base_url = "https://www.superdelivery.com"
        
        # 出力設定（設定ファイルから読み込み）
        output_config = self.sudeli_config.get('output', {})
        csv_filename = output_config.get('csv_filename', "sudeli_scraping.csv")
        
        # CSVのフルパスを設定
        self.csv_filename = os.path.join(self.data_dir, csv_filename)
        print(f"CSVファイル出力先: {self.csv_filename}")
        
        # カラム設定
        self.columns = ["卸業者名", "商品名", "JANコード", "価格", "セット数"]
        
        # ブラウザ設定
        self._setup_browser()
            
    def login(self, username=None, password=None):
        """
        スーパーデリバリーにログインします
        
        Args:
            username (str): スーパーデリバリーのログインユーザー名（指定なしの場合は設定ファイルから読み込み）
            password (str): スーパーデリバリーのパスワード（指定なしの場合は設定ファイルから読み込み）
            
        Returns:
            bool: ログイン成功時はTrue、失敗時はFalse
        """
        # ユーザー名とパスワードが指定されていない場合は設定ファイルから読み込む
        if username is None or password is None:
            username = self.sudeli_config['login']['username']
            password = self.sudeli_config['login']['password']
            
        try:
            # ログインページにアクセス
            self.browser.get(f"{self.base_url}/p/do/clickMemberLogin")
            
            # ログインフォームに入力
            self.wait.until(EC.presence_of_element_located((By.NAME, "identification"))).send_keys(username)
            self.browser.find_element(By.NAME, "password").send_keys(password)
            
            # ログインボタンのクリック - input[type="submit"]を選択
            self.browser.find_element(By.CSS_SELECTOR, "input[type='submit'][value='ログイン']").click()
            
            # ログイン成功の確認（ログイン後ページの特定要素が表示されるか）
            self.wait.until(EC.presence_of_element_located((By.ID, "loading-contents")))
            
            # ログイン後、Cookieを取得してrequestsセッションを準備
            self._setup_session()
            
            return True
        except Exception as e:
            print(f"ログインエラー: {str(e)}")
            return False


    def get_products_from_page(self, url, page_number):
        """
        スーパーデリバリーの商品一覧ページから直接商品情報を取得します
        
        Args:
            url (str): 商品一覧ページのURL
            page_number (int): ページ番号（表示用）
        
        Returns:
            list: 商品データのリスト
        """
        products_data = []
        print(f"現在 {page_number} ページ目をスクレイピング中... ({url})")
        
        try:
            # Seleniumを使ってページを取得
            print("Seleniumでページの取得を開始...")
            self.browser.get(url)
            # 商品ボックスが表示されるまで待機（最大10秒）
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.plist-detail-box")))
            html_content = self.browser.page_source
            print("ページの取得完了")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 卸業者名をページヘッダーから取得
            supplier_name = "不明"  # デフォルト値
            supplier_element = soup.select_one("h1.dl-name a.dl-name-txt")
            if supplier_element:
                supplier_name = supplier_element.text.strip()
                print(f"卸業者名: {supplier_name}")
            else:
                print("卸業者名の要素が見つかりません")
            
            # 各商品ボックスを探す
            print("商品ボックスを検索中...")
            product_boxes = soup.select("div.plist-detail-box")
            print(f"商品ボックスの数: {len(product_boxes)}")
            
            if not product_boxes:
                print(f"ページ {page_number} の商品リストが見つかりません")
                return products_data
            
            # 各商品ボックスからデータを抽出
            for box in product_boxes:
                try:
                    # 商品情報が入っているテーブルを取得
                    set_list_table = box.select_one("table.set-list")
                    
                    if not set_list_table:
                        continue
                    
                    # テーブル内の全ての行を取得（見出し行を除く）
                    rows = set_list_table.select("tbody > tr:not(:first-child)")
                    
                    # 3行ごとにグループ化処理
                    i = 0
                    while i < len(rows):
                        # エラー行や空の行をスキップ
                        if "errors_" in rows[i].get("id", "") or not rows[i].get("class"):
                            i += 1
                            continue
                        
                        # 商品情報を持つ行が3つ揃っているか確認
                        if i+2 < len(rows):
                            name_row = rows[i]      # 商品名とJANコード
                            price_row = rows[i+1]   # 価格
                            set_row = rows[i+2]     # セット数
                            
                            # 商品名を取得 (F12で確認した要素を使用)
                            name_cell = name_row.select_one("td.border-rt.co-p5.co-align-left")
                            if not name_cell:
                                i += 3
                                continue
                                
                            name_text = name_cell.get_text(strip=True)
                            product_name = name_text.split("JAN：")[0].strip() if "JAN：" in name_text else name_text
                            
                            # JANコードを取得
                            jan_element = name_cell.select_one("div.co-fcgray.co-fs12")
                            jan_code = ""
                            if jan_element:
                                jan_text = jan_element.get_text(strip=True)
                                jan_match = re.search(r'JAN：(\d+)', jan_text)
                                if jan_match:
                                    jan_code = jan_match.group(1)
                            
                            # 価格を取得
                            price_cell = price_row.select_one("td.border-r.co-align-right.co-pr5")
                            price = ""
                            if price_cell:
                                # まず割引価格（cmp-price）があるか確認
                                cmp_price_element = price_cell.select_one("p.cmp-price")
                                if cmp_price_element:
                                    price_text = cmp_price_element.get_text(strip=True)
                                    price_match = re.search(r'¥([\d,]+)', price_text)
                                    if price_match:
                                        price = price_match.group(1).replace(',', '')
                                
                                # 割引価格が見つからなければ通常価格を取得
                                if not price:
                                    # 通常の価格取得（list-priceまたは直接のテキスト）
                                    list_price_element = price_cell.select_one("p.list-price span")
                                    if list_price_element:
                                        price_text = list_price_element.get_text(strip=True)
                                    else:
                                        price_text = price_cell.get_text(strip=True)
                                    
                                    price_match = re.search(r'¥([\d,]+)', price_text)
                                    if price_match:
                                        price = price_match.group(1).replace(',', '')
                            
                            # セット数を取得
                            set_cell = set_row.select_one("td.set-num span.text-newline")
                            set_number = "1"  # デフォルト値
                            if set_cell:
                                set_text = set_cell.get_text(strip=True)
                                set_match = re.search(r'（(\d+)点）', set_text)
                                if set_match:
                                    set_number = set_match.group(1)
                            
                            # 商品データをリストに追加
                            if product_name and jan_code and price:
                                products_data.append([
                                    supplier_name,     # 卸業者名
                                    product_name,      # 商品名
                                    jan_code,          # JANコード
                                    price,             # 価格
                                    set_number         # セット数
                                ])
                            
                            # 次のグループへ
                            i += 3
                        else:
                            # 残りの行が3つ未満なら終了
                            break
                    
                except Exception as e:
                    print(f"商品データ取得エラー: {str(e)}")
                    continue
        
        except Exception as e:
            print(f"ページ {page_number} の取得エラー: {str(e)}")
        
        print(f"取得した商品データ数: {len(products_data)}")
        return products_data

    
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
        
        for target in self.sudeli_config['target_pages']:
            print(f"\n===== {target['name']} の処理を開始 =====")
            
            # URLとページ範囲を取得
            base_url = target['url']
            sort = target.get('sort', '')  # ソート条件（指定がなければ空文字）
            start_page = target.get('start_page', 1)
            end_page = target.get('end_page', 1)
            
            # 各ページの処理
            for page in range(start_page, end_page + 1):
                # URLを構築（スーパーデリバリーの形式に合わせる）
                # ソートパラメータが指定されている場合のみURLに追加
                if sort:
                    page_url = f"{base_url}/all/{page}/?so={sort}&vi=3"
                else:
                    page_url = f"{base_url}/all/{page}/?vi=3"
                
                # 商品データを直接取得
                page_data = self.get_products_from_page(page_url, page)
                
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