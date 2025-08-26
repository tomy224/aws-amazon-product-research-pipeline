# modules/apis/base_api.py
import requests
import time
import logging
from typing import Dict, Any, Optional

from modules.utils.logger_utils import get_logger, log_function_call

logger = get_logger(__name__)

class BaseAPI:
    """
    API呼び出しの基底クラス
    
    共通のAPI機能（リクエスト送信、レート制限、エラーハンドリングなど）を提供します。
    """
    
    def __init__(self, base_url: str, api_name: str = "API"):
        """
        初期化
        
        Args:
            base_url: APIのベースURL
            api_name: API名（ログ出力用）
        """
        self.base_url = base_url
        self.api_name = api_name
        self.session = requests.Session()
        self.last_request_time = 0
        self.min_request_interval = 1.0  # デフォルトのリクエスト間隔（秒）
    
    @log_function_call
    def make_request(self, 
                     endpoint: str = "", 
                     method: str = "GET", 
                     params: Optional[Dict[str, Any]] = None, 
                     headers: Optional[Dict[str, str]] = None, 
                     data: Any = None, 
                     json_data: Any = None, 
                     timeout: int = 30, 
                     max_retries: int = 3, 
                     retry_delay: int = 2) -> Dict:
        """
        APIリクエストを送信する
        
        Args:
            endpoint: エンドポイントパス
            method: HTTPメソッド
            params: クエリパラメータ
            headers: HTTPヘッダー
            data: フォームデータ
            json_data: JSONデータ
            timeout: タイムアウト（秒）
            max_retries: 最大リトライ回数
            retry_delay: リトライ間隔（秒）
            
        Returns:
            Dict: APIレスポンス（JSON）
            
        Raises:
            requests.exceptions.RequestException: リクエストに失敗した場合
        """
        # リクエストURLの構築
        url = f"{self.base_url}/{endpoint}" if endpoint else self.base_url
        
        # デフォルト値の設定
        params = params or {}
        headers = headers or {}
        
        # レート制限用の待機
        self._wait_for_rate_limit()
        
        for attempt in range(max_retries):
            try:
                logger.info(f"{self.api_name}リクエスト: {method} {url}")
                logger.debug(f"パラメータ: {params}")
                
                # リクエスト実行
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    data=data,
                    json=json_data,
                    timeout=timeout
                )
                
                # 最終リクエスト時間の更新
                self.last_request_time = time.time()
                
                # レスポンスステータスコードのチェック
                response.raise_for_status()
                
                # JSONレスポンスの解析
                response_data = response.json()
                logger.info(f"{self.api_name}リクエスト成功")
                return response_data
                
            except requests.exceptions.HTTPError as e:
                # レート制限エラーの処理
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', retry_delay))
                    logger.warning(f"レート制限に達しました。{retry_after}秒待機します...")
                    time.sleep(retry_after)
                    continue
                
                # その他のHTTPエラー
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"HTTPエラー: {e}. {wait_time}秒後にリトライします ({attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"HTTPエラー: {e}")
                    raise
                    
            except requests.exceptions.RequestException as e:
                # ネットワークエラーなど
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"リクエストエラー: {e}. {wait_time}秒後にリトライします ({attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"リクエストエラー: {e}")
                    raise
    
    def _wait_for_rate_limit(self):
        """レート制限に対応した待機を行う"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            logger.debug(f"レート制限のため{wait_time:.2f}秒待機します")
            time.sleep(wait_time)

    @staticmethod
    def map_shipping_condition(api: str, code: int) -> str:
        """
        APIの種類とコードから統一された送料条件テキストに変換
        
        Args:
            api: API種別（'Yahoo'または'Rakuten'）
            code: 送料コード
            
        Returns:
            str: 統一された送料条件テキスト
        """
        if api == 'Yahoo':
            # Yahoo APIの送料コード変換
            if code == 2:
                return "送料込み"  # 送料無料
            elif code == 3:
                return "条件付送料無料"
            else:
                return "送料別"  # code 1または不明
        
        elif api == 'Rakuten':
            # 楽天APIの送料コード変換
            if code == 0:
                return "送料込み"
            else:
                return "送料別"
        
        # 不明なAPIの場合
        return "不明"
    
