# modules/apis/yahoo_api.py
from typing import Dict, List, Optional

from modules.utils.logger_utils import get_logger, log_function_call
from .base_api import BaseAPI

logger = get_logger(__name__)

class YahooShoppingAPI(BaseAPI):
    """
    Yahoo Shopping APIのラッパークラス
    """
    
    def __init__(self, client_id: str):
        """
        初期化
        
        Args:
            client_id: Yahoo Developer Network Client ID
        """
        super().__init__(
            base_url="https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch",
            api_name="Yahoo Shopping API"
        )
        self.client_id = client_id
        self.min_request_interval = 2.0  # Yahoo APIのレート制限に合わせて調整
        

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
            'appid': self.client_id,
            'jan_code': jan_code,  # JANコードで検索
            'in_stock': True,       # 在庫ありのみ
            'condition': 'new',    # 新品
            'sort': '+price',      # 価格の安い順
            'results': max_items   # 取得する商品数
        }
        
        try:
            # APIリクエストの実行
            response = self.make_request(params=params)
            
            # 結果の整形
            products = []
            for hit in response.get('hits', []):
                # 送料情報を取得
                shipping = hit.get('shipping', {})
                shipping_code = shipping.get('code', 1)

                # 静的メソッドとして呼び出し
                shipping_text = self.map_shipping_condition('Yahoo', shipping_code)
                
                product = {
                    'JAN': jan_code,
                    'API': 'Yahoo',
                    '価格': hit.get('price', 0),
                    '送料条件': shipping_text,  # マッピング関数で変換した送料条件
                    '商品URL': hit.get('url', '')
                    # shippingオブジェクト自体は保存しない
                }
                products.append(product)
            
            logger.info(f"JANコード {jan_code} の検索結果: {len(products)}件")
            return products
            
        except Exception as e:
            logger.error(f"検索エラー (JAN: {jan_code}): {str(e)}")
            return []
        
        