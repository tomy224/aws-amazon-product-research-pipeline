"""
Keepa APIでASINバッチを処理するモジュール

このモジュールは、902_run_data_analysis_calc.pyから呼び出され、
1時間ごとにKeepa APIを使用してASINバッチを処理します。
"""

import os
import time
import logging
import pandas as pd
from typing import List, Optional

from modules.apis.keepa_api import ProductAnalyzer
from modules.utils.logger_utils import get_logger

# ロガーの取得
logger = get_logger(__name__)

class KeepaProcessor:
    """Keepa API処理用クラス"""
    
    def __init__(self, config_path=None, root_dir=None, data_dir=None):
        """
        初期化
        
        Args:
            config_path (str, optional): 設定ファイルのパス
            root_dir (str, optional): プロジェクトルートディレクトリ
            data_dir (str, optional): データディレクトリ
        """
        # Keepa APIアナライザーの初期化
        self.analyzer = ProductAnalyzer(config_path)
        
        # データディレクトリの設定（引数で指定があればそれを使用）
        if root_dir:
            self.root_dir = root_dir
        else:
            self.root_dir = self.analyzer.root_dir
            
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(self.root_dir, 'data')
            
        logger.info(f"KeepaProcessorを初期化しました: データディレクトリ={self.data_dir}")
    
    def process_batch(self, asin_list: List[str], output_file: Optional[str] = None) -> pd.DataFrame:
        """
        ASINリストのバッチ処理を実行
        
        Args:
            asin_list (List[str]): 処理するASINのリスト
            output_file (str, optional): 出力ファイルパス
            
        Returns:
            pd.DataFrame: 処理結果のデータフレーム
        """
        if not asin_list:
            logger.warning("処理するASINがありません")
            return pd.DataFrame()
            
        logger.info(f"Keepa APIバッチ処理を開始: {len(asin_list)}件")
        print(f"🔍 Keepa APIで{len(asin_list)}件のASINを処理します...")
        
        # 処理開始時間
        start_time = time.time()
        
        try:
            # ASINリストを処理
            df = self.analyzer.get_product_data(asin_list)
            
            # 処理結果の保存
            if output_file and not df.empty:
                # 相対パスが指定された場合はデータディレクトリを基準にする
                if not os.path.isabs(output_file):
                    output_file = os.path.join(self.data_dir, output_file)
                    
                # CSVとして保存
                self.analyzer.save_to_csv(df, output_file)
                
                # フィルタリング（amazon_90日間在庫切れ率50%以上）
                # この部分は呼び出し元（process_keepa_batch）で行うように変更
                # フィルタリング済みファイル名は呼び出し元で統一して管理
                    
            # 処理時間の計算
            elapsed_time = time.time() - start_time
            logger.info(f"Keepa APIバッチ処理が完了: {len(df)}件 ({elapsed_time:.1f}秒)")
            print(f"✅ Keepa API処理完了: {len(df)}件のASINを {elapsed_time:.1f}秒で処理しました")
            
            return df
            
        except Exception as e:
            logger.error(f"Keepa APIバッチ処理エラー: {str(e)}")
            print(f"❌ Keepa API処理エラー: {str(e)}")
            return pd.DataFrame()