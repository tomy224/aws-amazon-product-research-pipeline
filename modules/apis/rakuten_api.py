# modules/apis/rakuten_api.py
from typing import Dict, List, Optional

from modules.utils.logger_utils import get_logger, log_function_call
from modules.utils.data_utils import clean_rakuten_url, format_rakuten_price_with_shipping
from .base_api import BaseAPI

logger = get_logger(__name__)


class RakutenAPI(BaseAPI):
    """
    楽天APIのラッパークラス
    """
    
    def __init__(self, application_id: str):
        """
        初期化
        
        Args:
            application_id: 楽天アプリケーションID
        """
        super().__init__(
            base_url="https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601",
            api_name="楽天API"
        )
        self.application_id = application_id
        self.min_request_interval = 1.0  # 楽天APIのレート制限に合わせて調整
        

    @log_function_call
    def search_by_jan(self, jan_code: str, max_items: int = 3) -> List[Dict]:
        """
        JANコードで商品を検索
        
        Args:
            jan_code: 検索するJANコード
            max_items: 取得する最大商品数
            
        Returns:
            List[Dict]: 商品情報のリスト
        """
        # パラメータの設定
        params = {
            'applicationId': self.application_id,
            'keyword': jan_code,            # JANコードをキーワードとして検索
            'hits': max_items,              # 最大取得件数
            'sort': '+itemPrice',           # 価格の安い順にソート
            'format': 'json',               # レスポンス形式
            'formatVersion': 2,              # API形式バージョン
            'field': 0          # 検索対象を広げる
        }
        
        try:
            # APIリクエストの実行
            response = self.make_request(params=params)
            
            # 結果の整形
            products = []
            
            if 'Items' in response and response['Items']:
                for item in response['Items']:
                    # postageFlag値の取得
                    postage_flag = item.get('postageFlag', 1)  # デフォルトは1（送料別）
                    
                    # --- 変更箇所: BaseAPIクラスのマッピング関数を使用 ---
                    shipping_text = self.map_shipping_condition('Rakuten', postage_flag)
                    
                    product = {
                        'JAN': jan_code,
                        'API': 'Rakuten',
                        '価格': item.get('itemPrice', 0),
                        '送料条件': shipping_text,  # 直接条件判断から統一関数に変更
                        '商品URL': item.get('itemUrl', '')
                    }
                    products.append(product)
            
            logger.info(f"JANコード {jan_code} の検索結果: {len(products)}件")
            return products
            
        except Exception as e:
            logger.error(f"検索エラー (JAN: {jan_code}): {str(e)}")
            return []