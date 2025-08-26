# modules/utils/data_utils.py
import re
from typing import Dict, Any, List, Optional

from .logger_utils import get_logger, log_function_call

# ロガーの取得
logger = get_logger(__name__)

@log_function_call
def format_yahoo_price_with_shipping(price: int, shipping_code: int) -> str:
    """
    Yahoo APIの送料条件コードに応じて価格表示形式を変更する
    
    Args:
        price: 商品価格
        shipping_code: 送料条件コード
            1: 設定無し
            2: 送料無料
            3: 条件付き送料無料
    
    Returns:
        str: フォーマットされた価格文字列
    """
    if shipping_code == 1:  # 設定無し
        return f"【{price}】"
    elif shipping_code == 2:  # 送料無料
        return str(price)  # カッコはつけない
    elif shipping_code == 3:  # 条件付き送料無料
        return f"〈{price}〉"
    else:
        # 不明な条件の場合はそのまま返す
        return str(price)

@log_function_call
def format_rakuten_price_with_shipping(price: int, postage_flag: int) -> str:
    """
    楽天APIの送料フラグに応じて価格表示形式を変更する
    
    Args:
        price: 商品価格
        postage_flag: 送料フラグ
            0: 送料込み
            1: 送料別
    
    Returns:
        str: フォーマットされた価格文字列
    """
    if postage_flag == 0:  # 送料込み
        return str(price)  # カッコはつけない
    else:  # 送料別 (postage_flag == 1)
        return f"【{price}】"

@log_function_call
def clean_rakuten_url(url: str) -> str:
    """
    楽天URLからアフィリエイト部分を削除する
    
    Args:
        url: 元のURL
    
    Returns:
        str: クリーニングされたURL
    """
    # '?'以降を削除
    return url.split('?')[0]

@log_function_call
def enrich_yahoo_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    Yahoo商品データを拡張する
    
    Args:
        product: Yahoo商品情報
    
    Returns:
        Dict[str, Any]: 拡張された商品情報
    """
    # コピーを作成して元のデータを変更しない
    enriched = product.copy()
    
    # 送料条件コードの取得
    shipping = product.get('shipping', {})
    shipping_code = shipping.get('code', 1) if shipping else 1
    price = product.get('価格', 0)
    
    # 価格条件付き表示を追加
    enriched['価格_条件込み'] = format_yahoo_price_with_shipping(price, shipping_code)
    
    return enriched

@log_function_call
def enrich_rakuten_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """
    楽天商品データを拡張する
    
    Args:
        product: 楽天商品情報
    
    Returns:
        Dict[str, Any]: 拡張された商品情報
    """
    # コピーを作成して元のデータを変更しない
    enriched = product.copy()
    
    # 楽天商品のURLクリーニング
    if '商品URL' in product:
        enriched['商品URL'] = clean_rakuten_url(product['商品URL'])
    
    # 送料フラグに基づく価格表示の整形
    # 送料条件から送料フラグを推定
    postage_flag = 1  # デフォルトは送料別
    if product.get('送料条件') == '送料込み':
        postage_flag = 0
    
    price = product.get('価格', 0)
    enriched['価格_条件込み'] = format_rakuten_price_with_shipping(price, postage_flag)
    
    return enriched

@log_function_call
def enrich_product_data(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    商品データを拡張・整形する
    
    Args:
        products: 商品情報のリスト
    
    Returns:
        List[Dict[str, Any]]: 拡張された商品情報のリスト
    """
    enriched_products = []
    
    for product in products:
        # APIに応じた処理
        if product.get('API') == 'Yahoo':
            enriched = enrich_yahoo_product(product)
        elif product.get('API') == 'Rakuten':
            enriched = enrich_rakuten_product(product)
        else:
            # 不明なAPIの場合は元のデータをそのまま使用
            enriched = product.copy()
            
        enriched_products.append(enriched)
    
    return enriched_products