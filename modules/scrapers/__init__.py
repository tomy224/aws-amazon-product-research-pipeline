# modules/scrapers/__init__.py
"""
Scrapers Package - 卸サイトからのスクレイピングを行うモジュール群

このパッケージは、各卸サイト向けのスクレイパークラスと共通の基底クラスを提供します。
"""

from .base_scraper import BaseScraper
from .netsea_scraper import NetseaScraper

__all__ = ['BaseScraper', 'NetseaScraper']