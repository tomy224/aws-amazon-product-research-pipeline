#!/usr/bin/env python
# coding: utf-8

"""
データ計算モジュール

このモジュールは、統合された商品データに対して各種計算処理を行います。
販売価格、利益、期待販売数などを計算し、結果をCSVファイルとして保存します。
"""

import sys
import os
from pathlib import Path

# プロジェクトルートをモジュール検索パスに追加
project_root = str(Path(__file__).resolve().parents[2])  # modules/integration から2階層上
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
import json
import logging
import re
import traceback
from datetime import datetime

# 自作モジュールのインポート
from modules.utils.file_utils import find_project_root, load_yaml_config
from modules.utils.logger_utils import setup_logging, get_logger

# ロガーの取得
logger = get_logger(__name__)

class ProductCalculator:
    """
    商品データに対して計算処理を行うクラス
    
    統合されたCSVデータを読み込み、追加の計算・分析を行って
    新しい列を追加し、結果を保存します。
    """
    
    def __init__(self, config_path=None):
        """
        ProductCalculatorの初期化
        
        Parameters:
        -----------
        config_path : str, optional
            設定ファイルのパス（指定しない場合はデフォルトパスを使用）
        """
        # プロジェクトルートディレクトリの検出
        self.root_dir = find_project_root()
        
        # ディレクトリパスの設定
        self.data_dir = os.path.join(self.root_dir, 'data')
        self.log_dir = os.path.join(self.root_dir, 'logs')
        
        # ディレクトリが存在しない場合は作成
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # ログの設定
        self._setup_logging()
        
        # 設定ファイルの読み込み
        self.config = load_yaml_config(config_path)
        
        # 計算機能の設定確認
        if 'calculator' not in self.config:
            self.config['calculator'] = {}
        
        # デフォルトの出力設定（なければ設定）
        if 'output' not in self.config['calculator']:
            self.config['calculator']['output'] = {
                'input_file': 'integrated_data.csv',
                'output_file': 'calculated_data.csv'
            }
        
        # 入出力ファイルパスの設定
        self.setup_file_paths()
    
    def _setup_logging(self):
        """ログ機能のセットアップ"""
        log_file = setup_logging(
            self.log_dir, 
            "calculator", 
            console_level=logging.INFO, 
            file_level=logging.DEBUG
        )
        logger.info("計算処理を開始します")
        print(f"📄 ログファイル: {log_file}")
    
    def setup_file_paths(self):
        """入出力ファイルパスの設定"""
        # 設定から入出力ファイル名を取得
        input_filename = self.config['calculator']['output'].get('input_file', 'integrated_data.csv')
        output_filename = self.config['calculator']['output'].get('output_file', 'calculated_data.csv')
        
        # 相対パスを絶対パスに変換
        if not os.path.isabs(input_filename):
            self.input_file = os.path.join(self.data_dir, input_filename)
        else:
            self.input_file = input_filename
            
        if not os.path.isabs(output_filename):
            self.output_file = os.path.join(self.data_dir, output_filename)
        else:
            self.output_file = output_filename
        
        logger.info(f"入力ファイル: {self.input_file}")
        logger.info(f"出力ファイル: {self.output_file}")

    def load_data(self):
        """
        CSVデータを読み込む
        
        Returns:
        --------
        pandas.DataFrame
            読み込んだデータフレーム
        """
        try:
            # ファイルの存在確認
            if not os.path.exists(self.input_file):
                raise FileNotFoundError(f"入力ファイルが見つかりません: {self.input_file}")
            
            # CSVファイルの読み込み
            df = pd.read_csv(self.input_file, encoding='utf-8-sig')
            
            # JANコードを文字列として処理
            if 'JAN' in df.columns:
                df['JAN'] = df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            
            logger.info(f"データを読み込みました: {len(df)}行, {len(df.columns)}列")
            print(f"📊 {len(df)}行のデータを読み込みました")
            
            return df
        except Exception as e:
            logger.error(f"データ読み込みエラー: {str(e)}")
            raise

    def save_data(self, df):
        """
        計算結果をCSVとして保存
        
        Parameters:
        -----------
        df : pandas.DataFrame
            保存するデータフレーム
        """
        try:
            # 出力ディレクトリの確認
            output_dir = os.path.dirname(self.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 元のデータフレームをコピー
            output_df = df.copy()
            
            # 不要な列を除外する部分を削除
            # このコメントを残して、除外処理を削除します
            
            # CSVとして保存
            output_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"データを保存しました: {self.output_file} ({len(output_df)}行, {len(output_df.columns)}列)")
            print(f"✅ {len(output_df)}行のデータを {self.output_file} に保存しました")
        except Exception as e:
            logger.error(f"データ保存エラー: {str(e)}")
            raise

    def load_json_data(self, json_file_path):
        """
        JSONファイルからデータを読み込む
        
        Parameters:
        -----------
        json_file_path : str
            JSONファイルのパス
            
        Returns:
        --------
        dict
            JSONデータ
        """
        try:
            if not os.path.exists(json_file_path):
                logger.warning(f"JSONファイルが見つかりません: {json_file_path}")
                return {}
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info(f"JSONデータを読み込みました: {json_file_path}")
            return data
        except Exception as e:
            logger.error(f"JSONデータの読み込みエラー: {str(e)}")
            return {}

    def add_calculation_columns(self, df):
        """
        計算列を追加する
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()

            # セット数の計算処理を追加
            if 'セット数(Q)' in result_df.columns and 'セット数(N)' in result_df.columns:
                # セット数_不明列を初期化（すべて空文字列）
                result_df['セット数_不明'] = ''
                
                # セット数を計算する関数
                def calculate_set_count(row):
                    # Q値の取得と検証（1以上10未満なら有効、それ以外はNone）
                    q_value = row['セット数(Q)'] if pd.notna(row['セット数(Q)']) and 1 <= row['セット数(Q)'] < 10 else None
                    
                    # N値の取得と検証（1以上10未満なら有効、それ以外はNone）
                    n_value = row['セット数(N)'] if pd.notna(row['セット数(N)']) and 1 <= row['セット数(N)'] < 10 else None
                    
                    # 両方の値がない場合はセット数_不明に「x」を設定
                    if q_value is None and n_value is None:
                        # この行のセット数_不明列に「x」を設定する
                        # apply関数の中では直接DataFrameを更新できないため、
                        # 呼び出し元で後から更新する必要がある
                        return 1  # 両方の値がない場合：1を出力
                    
                    # ロジックに基づいて結果を決定
                    if q_value is not None and n_value is not None:
                        # 両方の値がある場合
                        if q_value == n_value:
                            return int(q_value)  # Q=Nの場合：Qを出力
                        elif q_value < n_value:
                            return int(n_value)  # Q<Nの場合：Nを出力
                        else:
                            return int(q_value)  # それ以外（Q>N）の場合：Qを出力
                    elif q_value is None and n_value is not None:
                        return int(n_value)  # Qの値がなくてNの値がある場合：Nを出力
                    elif q_value is not None and n_value is None:
                        return int(q_value)  # Nの値がなくてQの値がある場合：Qを出力
                
                # 計算を実行して新しい列を追加
                result_df['セット数_セット数'] = result_df.apply(calculate_set_count, axis=1)
                
                # 両方の値がない行に「x」を設定
                result_df.loc[(pd.isna(result_df['セット数(Q)']) | (result_df['セット数(Q)'] < 1) | (result_df['セット数(Q)'] >= 10)) & 
                            (pd.isna(result_df['セット数(N)']) | (result_df['セット数(N)'] < 1) | (result_df['セット数(N)'] >= 10)), 
                            'セット数_不明'] = 'x'
                
                logger.info("セット数表示用の列を追加しました")

            # 「商品名_sp」列から「セット商品」の判定を行う
            if '商品名_sp' in result_df.columns:
                # 新しい列「商品情報_セット商品?」を空文字で初期化
                result_df['商品情報_セット商品?'] = ''
                
                # 「商品名_sp」列に「セット」が含まれている行に「x」を設定
                result_df.loc[result_df['商品名_sp'].str.contains('セット', na=False), '商品情報_セット商品?'] = 'x'
                
                logger.info("セット商品判定の列を追加しました")
                
            # 販売価格の合計計算（修正1: fillna(0)を削除して値がない場合はNaNのままにする）
            # カート販売価格の合計
            if 'カート価格' in result_df.columns:
                result_df['販売価格_カート合計'] = result_df['カート価格'] + result_df['カート価格送料'].fillna(0) + result_df['カート価格のポイント'].fillna(0)
            
            # FBA販売価格の合計
            if 'FBA最安値' in result_df.columns:
                result_df['販売価格_FBA合計'] = result_df['FBA最安値'] + result_df['FBA最安値のポイント'].fillna(0)
            
            # 自己発送販売価格の合計
            if '自己発送最安値' in result_df.columns:
                result_df['販売価格_自己発合計'] = result_df['自己発送最安値'] + result_df['自己発送最安値の送料'].fillna(0) + result_df['自己発送最安値のポイント'].fillna(0)

            # 修正2: 販売価格_設定販売額の計算ロジックを変更
            if all(col in result_df.columns for col in ['販売価格_カート合計', '販売価格_FBA合計', '販売価格_自己発合計']):
                # 各列の有無を判定
                has_cart = result_df['販売価格_カート合計'].notna()
                has_fba = result_df['販売価格_FBA合計'].notna()
                has_self = result_df['販売価格_自己発合計'].notna()
                
                # 自己発送価格に5%加算したもの(FBAアドバンテージ額算定)
                self_price_plus5 = result_df['販売価格_自己発合計'] * 1.05
                
                # 初期値はNaNで設定
                result_df['販売価格_設定販売額'] = pd.NA
                
                # カート価格がある場合
                cart_condition = has_cart
                # 1. FBAあり、自己発なし → カート価格
                result_df.loc[cart_condition & has_fba & (~has_self), '販売価格_設定販売額'] = result_df.loc[cart_condition & has_fba & (~has_self), '販売価格_カート合計']
                # 2. FBAあり、自己発あり → カート価格
                result_df.loc[cart_condition & has_fba & has_self, '販売価格_設定販売額'] = result_df.loc[cart_condition & has_fba & has_self, '販売価格_カート合計']
                # 3. FBAなし、自己発あり → カート価格と（自己発×1.05）の低い方（self価格は四捨五入）
                cart_cond = cart_condition & (~has_fba) & has_self
                for idx in result_df[cart_cond].index:
                    cart_price = result_df.loc[idx, '販売価格_カート合計']
                    self_price_105 = self_price_plus5.loc[idx]
                    if pd.notna(cart_price) and pd.notna(self_price_105):
                        self_price_105_rounded = round(self_price_105)
                        if self_price_105_rounded < cart_price:
                            result_df.loc[idx, '販売価格_設定販売額'] = self_price_105_rounded
                        else:
                            result_df.loc[idx, '販売価格_設定販売額'] = cart_price
                
                # カート価格がない場合
                no_cart_condition = ~has_cart
                # 4. FBAあり、自己発なし → FBA価格
                result_df.loc[no_cart_condition & has_fba & (~has_self), '販売価格_設定販売額'] = result_df.loc[no_cart_condition & has_fba & (~has_self), '販売価格_FBA合計']
                # 5. FBAあり、自己発あり → FBA価格と自己発価格の安い方
                result_df.loc[no_cart_condition & has_fba & has_self, '販売価格_設定販売額'] = result_df.loc[no_cart_condition & has_fba & has_self, ['販売価格_FBA合計', '販売価格_自己発合計']].min(axis=1)
                # 6. FBAなし、自己発あり → 自己発価格
                result_df.loc[no_cart_condition & (~has_fba) & has_self, '販売価格_設定販売額'] = result_df.loc[no_cart_condition & (~has_fba) & has_self, '販売価格_自己発合計']
            
            # 修正1: サイズ不明の判定を最初に行う
            size_columns = ['パッケージ最長辺', 'パッケージ中辺', 'パッケージ最短辺', 'パッケージ重量']
            
            # サイズ不明の列を追加
            result_df['サイズ_サイズ不明'] = np.where(
                result_df[size_columns].isna().any(axis=1),
                '不明',
                ''
            )
            
            # サイズの計算
            # 修正2: サイズ_合計cm（三辺の合計）- サイズ不明の場合は空欄を返す
            if 'パッケージ最長辺' in result_df.columns:
                # サイズ不明の行を特定
                size_unknown = result_df['サイズ_サイズ不明'] == '不明'
                
                # 通常の行（サイズが分かっている行）
                result_df.loc[~size_unknown, 'サイズ_合計cm'] = (
                    result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) + 
                    result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) + 
                    result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0)
                )
                
                # サイズ不明の行（空欄のまま）
                result_df.loc[size_unknown, 'サイズ_合計cm'] = pd.NA
            
            # サイズ_合計cm3（体積）- サイズ不明の場合は空欄を返す
            if 'パッケージ最長辺' in result_df.columns:
                # サイズ不明の行は空欄（NaN）
                result_df.loc[~size_unknown, 'サイズ_合計cm3'] = (
                    result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) * 
                    result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) * 
                    result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0)
                )
                
                # サイズ不明の行（空欄のまま）
                result_df.loc[size_unknown, 'サイズ_合計cm3'] = pd.NA

            # 修正3: サイズ_小型標準判定（小型標準サイズの判定）- サイズ不明の場合は何も出力しない
            if 'パッケージ最長辺' in result_df.columns and 'パッケージ重量' in result_df.columns:
                # サイズ不明でない行のみ判定
                result_df.loc[~size_unknown, 'サイズ_小型標準判定'] = np.where(
                    (result_df.loc[~size_unknown, 'パッケージ最長辺'].fillna(0) <= 25) & 
                    (result_df.loc[~size_unknown, 'パッケージ中辺'].fillna(0) <= 18) & 
                    (result_df.loc[~size_unknown, 'パッケージ最短辺'].fillna(0) <= 2) & 
                    (result_df.loc[~size_unknown, 'パッケージ重量'].fillna(0) <= 250), 
                    '対象', '対象外'
                )
                
                # サイズ不明の行は空文字列
                result_df.loc[size_unknown, 'サイズ_小型標準判定'] = ''

            # 出品者_amazon（Amazonが出品しているかどうかの判定）
            if 'Amazon価格' in result_df.columns:
                result_df['出品者_amazon'] = np.where(result_df['Amazon価格'].fillna(0) >= 1, '有', '無')

            # Amazonなし率が50%未満の判定
            if 'amazon_90日間在庫切れ率' in result_df.columns:
                # 新しい列を空文字で初期化
                result_df['出品者_90日amazonなし率_50%未満'] = ''
                
                # 値が0.5（50%）未満の行に「x」を設定
                result_df.loc[result_df['amazon_90日間在庫切れ率'] < 0.5, '出品者_90日amazonなし率_50%未満'] = 'x'
                        
            logger.info(f"基本計算処理が完了しました: {len(result_df.columns) - len(df.columns)}列追加")
            return result_df
            
        except Exception as e:
            logger.error(f"基本計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df

    def add_size_calculations(self, df):
        """
        サイズに関する計算を行うメソッド
        JSONファイルからサイズ区分データを読み込み、サイズ判定を行います
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # サイズ不明の判定はすでに add_calculation_columns で行われているため省略
            
            # JSONファイルからサイズデータを読み込む
            json_file_path = os.path.join(self.root_dir, 'config', 'shipping_size_data.json')
            size_data = self.load_json_data(json_file_path)
            
            if not size_data:
                logger.warning("サイズデータが見つかりません。サイズ計算をスキップします。")
                return result_df
            
            # サイズ区分データを取得
            size_categories = size_data.get('サイズ区分', {})
            
            # 在庫保管手数料データを取得
            storage_fees = size_data.get('在庫保管手数料', {})
            
            # サイズ判定関数
            def determine_size_category(row):
                # 修正4: サイズ不明の場合は「標準-2」を返す
                if row['サイズ_サイズ不明'] == '不明':
                    return "標準-2"
                    
                # サイズと重量情報を取得
                sum_of_edges = row['サイズ_合計cm'] if pd.notna(row['サイズ_合計cm']) else 0
                longest_edge = row['パッケージ最長辺'] if pd.notna(row['パッケージ最長辺']) else 0
                middle_edge = row['パッケージ中辺'] if pd.notna(row['パッケージ中辺']) else 0
                shortest_edge = row['パッケージ最短辺'] if pd.notna(row['パッケージ最短辺']) else 0
                weight = row['パッケージ重量'] if pd.notna(row['パッケージ重量']) else 0
                
                # サイズ区分上限を取得
                size_limits = size_data.get('サイズ区分上限', {})
                
                # 主要カテゴリの判定（小型から順に判定）
                if (weight <= size_limits['小型']['最大重量'] and 
                    sum_of_edges <= size_limits['小型']['最大寸法']['三辺合計'] and
                    longest_edge <= size_limits['小型']['最大寸法']['最長辺'] and
                    middle_edge <= size_limits['小型']['最大寸法']['中辺'] and
                    shortest_edge <= size_limits['小型']['最大寸法']['最短辺']):
                    main_category = "小型"
                elif (weight <= size_limits['標準']['最大重量'] and 
                    sum_of_edges <= size_limits['標準']['最大寸法']['三辺合計'] and
                    longest_edge <= size_limits['標準']['最大寸法']['最長辺'] and
                    middle_edge <= size_limits['標準']['最大寸法']['中辺'] and
                    shortest_edge <= size_limits['標準']['最大寸法']['最短辺']):
                    main_category = "標準"
                elif (weight <= size_limits['大型']['最大重量'] and 
                    sum_of_edges <= size_limits['大型']['最大寸法']['三辺合計']):
                    main_category = "大型"
                elif (weight <= size_limits['特大型']['最大重量'] and 
                    sum_of_edges <= size_limits['特大型']['最大寸法']['三辺合計']):
                    main_category = "特大型"
                else:
                    return "対象外"
                
                # 詳細サイズ区分の判定
                matching_categories = [name for name, data in size_categories.items() 
                                    if name.startswith(main_category) and
                                    weight <= data.get('重量', float('inf')) and
                                    ((('最長辺' in data.get('寸法', {}) and
                                        longest_edge <= data['寸法']['最長辺'] and
                                        middle_edge <= data['寸法'].get('中辺', float('inf')) and
                                        shortest_edge <= data['寸法'].get('最短辺', float('inf')))) or
                                    (('三辺合計' in data.get('寸法', {}) and
                                        sum_of_edges <= data['寸法']['三辺合計'])))]
                
                # 該当するカテゴリが見つかった場合は最初のものを返す
                return matching_categories[0] if matching_categories else main_category
            
            # 月額保管料を計算する関数
            def calculate_storage_fee(row):
                # 修正4: サイズ不明のチェックを削除

                # サイズ不明の場合は10を返す
                if row['サイズ_サイズ不明'] == '不明':
                    return 10
                
                # 体積情報を取得
                volume_cm3 = row['サイズ_合計cm3'] if pd.notna(row['サイズ_合計cm3']) else 0
                
                # サイズカテゴリを取得
                size_category = row['サイズ_大きさ'] if pd.notna(row['サイズ_大きさ']) else "対象外"
                
                # サイズカテゴリが対象外または未定義の場合
                if size_category == "対象外":
                    return None
                
                # メインカテゴリ（小型、標準、大型、特大型）を抽出
                main_category = size_category.split('-')[0] if '-' in size_category else size_category
                
                # 該当するカテゴリの保管料単価を取得
                if main_category in storage_fees:
                    fee_rate = storage_fees[main_category].get('単価', 0)
                    
                    # 1000cm3あたりの料金で計算
                    storage_fee = fee_rate * (volume_cm3 / 1000)
                    
                    # 小数点以下を四捨五入して整数に
                    return round(storage_fee)
                
                return None
            
            # サイズ区分を判定して列に追加
            result_df['サイズ_大きさ'] = result_df.apply(determine_size_category, axis=1)
            
            # 月額保管料を計算して列に追加
            result_df['手数料・利益_月額保管料'] = result_df.apply(calculate_storage_fee, axis=1).apply(
                lambda x: -x if pd.notna(x) else None
            )

            # 配送代行手数料計算
            if 'サイズ_大きさ' in result_df.columns and '販売価格_設定販売額' in result_df.columns:
                # 手数料を計算する関数
                def calculate_shipping_fee(row):
                    # 修正4: サイズ不明のチェックを削除
                    
                    size_category = row['サイズ_大きさ']
                    price = row['販売価格_設定販売額'] if pd.notna(row['販売価格_設定販売額']) else 0
                    
                    # サイズカテゴリが対象外または存在しない場合
                    if size_category == "対象外" or size_category not in size_categories:
                        return None
                    
                    # 価格に応じた手数料を取得
                    fee_data = size_categories[size_category].get('配送代行手数料', {})
                    if price <= 1000:
                        return fee_data.get('1000円以下', None)
                    else:
                        return fee_data.get('1000円超', None)
                
                # 配送代行手数料を計算して列に追加
                result_df['手数料・利益_発送代行手数料'] = result_df.apply(calculate_shipping_fee, axis=1).apply(
                    lambda x: -x if pd.notna(x) else None
                )
            
            logger.info("サイズ計算処理が完了しました")
        
            # 列の順序を調整（サイズ_サイズ不明をサイズ_大きさの前に配置）
            columns = list(result_df.columns)
            size_idx = columns.index('サイズ_大きさ')
            unknown_idx = columns.index('サイズ_サイズ不明')
            
            columns.pop(unknown_idx)
            columns.insert(size_idx, 'サイズ_サイズ不明')
            result_df = result_df[columns]
            
            return result_df
            
        except Exception as e:
            logger.error(f"サイズ計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df
            

    def add_category_calculations(self, df):
        """カテゴリに関する計算を行うメソッド"""
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # JSONファイルからカテゴリデータを読み込む
            json_file_path = os.path.join(self.root_dir, 'config', 'category_data.json')
            category_data = self.load_json_data(json_file_path)
            
            if not category_data:
                logger.warning("カテゴリデータが見つかりません。カテゴリ計算をスキップします。")
                return result_df
            
            # カテゴリマッピングデータを取得
            category_mapping = category_data.get('カテゴリマッピング', {})
            
            # カテゴリIDからカテゴリ名へのマッピング作成（逆引き用）
            category_id_to_name = {}
            for category_name, info in category_mapping.items():
                category_id = info.get('keepaカテゴリID')
                if category_id:
                    category_id_to_name[str(category_id)] = category_name
            
            # カテゴリ情報とカテゴリ名から販売手数料率を計算する関数
            def get_category_info_and_fee_rate(row):
                # カテゴリーIDを取得して整形
                if pd.notna(row['カテゴリーID']):
                    # 浮動小数点数から整数に変換し、その後文字列に変換
                    try:
                        category_id = str(int(row['カテゴリーID']))
                    except (ValueError, TypeError):
                        # 変換できない場合はそのまま文字列にする
                        category_id = str(row['カテゴリーID'])
                else:
                    category_id = ''
                
                # カテゴリ名の取得（マッピングにない場合は「不明」）
                category_name = category_id_to_name.get(category_id, '不明')
                
                # 販売価格を取得
                price = row['販売価格_設定販売額'] if pd.notna(row['販売価格_設定販売額']) else 0
                
                # デフォルト値設定
                fee_rate = None
                fee_category = "不明"
                media_fee = None  # メディア手数料の初期値
                
                # カテゴリ名に該当する情報がある場合
                if category_name in category_mapping:
                    category_info = category_mapping[category_name]
                    fee_category = category_info.get('販売手数料カテゴリ', "不明")
                    fee_rates = category_info.get('販売手数料率', [])
                    
                    # メディア手数料を取得し、あれば消費税(10%)を加算
                    base_media_fee = category_info.get('メディア手数料')
                    if base_media_fee is not None:
                        media_fee = -(base_media_fee * 1.1)  # 消費税を加算
                        media_fee = round(media_fee)  # 四捨五入して整数に
                    
                    # 価格に応じた手数料率を決定
                    if isinstance(fee_rates, list):
                        # 配列形式の場合（新形式）
                        for rate_info in fee_rates:
                            upper_limit = rate_info.get('上限金額')
                            if upper_limit is None or price <= upper_limit:
                                fee_rate = rate_info.get('料率')
                                break
                    elif isinstance(fee_rates, dict):
                        # 辞書形式の場合（旧形式 - 互換性のため）
                        if price <= 750 and '750円以下' in fee_rates:
                            fee_rate = fee_rates['750円以下']
                        elif 750 < price <= 1500 and '750円超 1500円以下' in fee_rates:
                            fee_rate = fee_rates['750円超 1500円以下']
                        elif price > 1500 and '1500円超' in fee_rates:
                            fee_rate = fee_rates['1500円超']
                        elif '750円超' in fee_rates and price > 750:
                            fee_rate = fee_rates['750円超']
                        elif 'default' in fee_rates:
                            fee_rate = fee_rates['default']
                    else:
                        # 数値の場合（旧旧形式 - さらなる互換性のため）
                        fee_rate = fee_rates
                
                return pd.Series([category_name, fee_category, fee_rate, media_fee])
            
            # カテゴリ情報と手数料率を列に追加
            # 修正: カテゴリー列の名前を「カテゴリーID」に変更
            if 'カテゴリーID' in result_df.columns and '販売価格_設定販売額' in result_df.columns:
                # apply関数で複数の値を同時に返す
                result_df[['商品情報_カテゴリ', '販売手数料カテゴリ', '手数料・利益_販売手数料率', '手数料・利益_メディア手数料']] = (
                    result_df.apply(get_category_info_and_fee_rate, axis=1)
                )
                
                # 手数料率をパーセント表示用に変換（例: 0.15 → 15%）
                result_df['手数料・利益_販売手数料率_表示用'] = result_df['手数料・利益_販売手数料率'].apply(
                    lambda x: f"{x*100:.1f}%" if pd.notna(x) else "対象外"
                )
                
                # 販売手数料の計算（最低販売手数料を考慮）
                def calculate_fee(row):
                    if pd.isna(row['手数料・利益_販売手数料率']) or pd.isna(row['販売価格_設定販売額']):
                        return None
                    
                    category_name = row['商品情報_カテゴリ']
                    min_fee = 0
                    if category_name in category_mapping:
                        min_fee = category_mapping[category_name].get('最低販売手数料', 0)
                    
                    calculated_fee = row['販売価格_設定販売額'] * row['手数料・利益_販売手数料率']
                    
                    # 最低手数料がnullの場合は最低料金の制約なし
                    if min_fee is None:
                        return calculated_fee
                    
                    # 最低手数料と計算手数料の大きい方を採用
                    return max(calculated_fee, min_fee)
                
                # 販売手数料を計算して列に追加（小数点第一位で四捨五入）
                result_df['手数料・利益_販売手数料'] = result_df.apply(calculate_fee, axis=1).apply(
                    lambda x: -round(x) if pd.notna(x) else None
                )
                
                # 販売手数料（税込）を計算して列に追加（手数料に10%の消費税を加算し、小数点第一位で四捨五入）
                result_df['手数料・利益_販売手数料(税込)'] = result_df['手数料・利益_販売手数料'].apply(
                    lambda x: round(x * 1.1) if pd.notna(x) else None
                )
                
                logger.info("カテゴリ情報と販売手数料率の列を追加しました")
            else:
                missing_cols = []
                if 'カテゴリーID' not in result_df.columns:
                    missing_cols.append('カテゴリーID')
                if '販売価格_設定販売額' not in result_df.columns:
                    missing_cols.append('販売価格_設定販売額')
                
                logger.warning(f"カテゴリ計算に必要な列がありません: {', '.join(missing_cols)}")
                print(f"⚠️ カテゴリ計算に必要な列がありません: {', '.join(missing_cols)}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"カテゴリ計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df
        

    # # def add_category_calculations(self, df):
    # #     """カテゴリに関する計算を行うメソッド"""
    # #     try:
    # #         # 元のデータフレームのコピーを作成
    # #         result_df = df.copy()
            
    # #         # デバッグ: 列名の確認
    # #         print("\n=== カテゴリー計算のデバッグ情報 ===")
    # #         print(f"データフレームの列名一覧: {', '.join(result_df.columns)}")
    # #         print(f"'カテゴリー_keepa'の有無: {'カテゴリー_keepa' in result_df.columns}")
            
    # #         # 通常の「カテゴリー」列の確認
    # #         if 'カテゴリー' in result_df.columns:
    # #             print(f"'カテゴリー'列が存在します。最初の5つの値: {result_df['カテゴリー'].head(5).tolist()}")
                
    #             # デバッグ: もし「カテゴリー_keepa」がないが「カテゴリー」列がある場合に備えて、列のコピーを作成
    #             if 'カテゴリー_keepa' not in result_df.columns:
    #                 print("'カテゴリー_keepa'列がないので、'カテゴリー'列からコピーを作成します")
    #                 result_df['カテゴリー_keepa'] = result_df['カテゴリー']
            
    #         # JSONファイルからカテゴリデータを読み込む
    #         json_file_path = os.path.join(self.root_dir, 'config', 'category_data.json')
    #         category_data = self.load_json_data(json_file_path)
            
    #         if not category_data:
    #             logger.warning("カテゴリデータが見つかりません。カテゴリ計算をスキップします。")
    #             return result_df
            
    #         # カテゴリマッピングデータを取得
    #         category_mapping = category_data.get('カテゴリマッピング', {})
            
    #         # デバッグ: category_mappingの内容を確認
    #         print(f"カテゴリマッピングのキー数: {len(category_mapping)}")
    #         print(f"カテゴリマッピングの最初の3つのキー: {list(category_mapping.keys())[:3]}")
            
    #         # カテゴリIDからカテゴリ名へのマッピング作成（逆引き用）
    #         category_id_to_name = {}
    #         for category_name, info in category_mapping.items():
    #             category_id = info.get('keepaカテゴリID')
    #             if category_id:
    #                 category_id_to_name[str(category_id)] = category_name
            
    #         # デバッグ: category_id_to_nameの内容確認
    #         print(f"カテゴリIDマッピングのキー数: {len(category_id_to_name)}")
    #         print(f"カテゴリIDマッピングの最初の3つのキー: {list(category_id_to_name.keys())[:3]}")
            
    #         # カテゴリー_keepa列のユニーク値を確認
    #         if 'カテゴリー_keepa' in result_df.columns:
    #             unique_categories = result_df['カテゴリー_keepa'].unique()
    #             print(f"'カテゴリー_keepa'列のユニーク値数: {len(unique_categories)}")
    #             print(f"'カテゴリー_keepa'列の最初の5つのユニーク値: {unique_categories[:5]}")
                
    #             # ユニーク値とマッピングの一致確認
    #             match_count = sum(1 for cat in unique_categories if str(cat) in category_id_to_name)
    #             print(f"マッピングに一致するカテゴリID数: {match_count}/{len(unique_categories)}")

    #         # ここから元のコードが続きます...
            
    #         # カテゴリ情報とカテゴリ名から販売手数料率を計算する関数
    #         def get_category_info_and_fee_rate(row):
    #             # カテゴリーIDを取得して整形
    #             if pd.notna(row['カテゴリー_keepa']):
    #                 # 浮動小数点数から整数に変換し、その後文字列に変換
    #                 # 160384011.0 → 160384011 → '160384011'
    #                 try:
    #                     category_id = str(int(row['カテゴリー_keepa']))
    #                 except (ValueError, TypeError):
    #                     # 変換できない場合はそのまま文字列にする
    #                     category_id = str(row['カテゴリー_keepa'])
    #             else:
    #                 category_id = ''
                
    #             # デバッグ: 関数内で値を確認するためのコードを追加
    #             if row.name < 5:  # 最初の5行だけデバッグ出力
    #                 print(f"行 {row.name}: 元のカテゴリーID={row['カテゴリー_keepa']}, 整形後={category_id}, マッピング存在={category_id in category_id_to_name}")
                
    #             # 残りのコードは同じまま
    #             category_name = category_id_to_name.get(category_id, '不明')
                
    #             # 販売価格を取得
    #             price = row['販売価格_設定販売額'] if pd.notna(row['販売価格_設定販売額']) else 0
                
    #             # デフォルト値設定
    #             fee_rate = None
    #             fee_category = "不明"
    #             media_fee = None  # メディア手数料の初期値
                
    #             # カテゴリ名に該当する情報がある場合
    #             if category_name in category_mapping:
    #                 category_info = category_mapping[category_name]
    #                 fee_category = category_info.get('販売手数料カテゴリ', "不明")
    #                 fee_rates = category_info.get('販売手数料率', [])
                    
    #                 # メディア手数料を取得し、あれば消費税(10%)を加算
    #                 base_media_fee = category_info.get('メディア手数料')
    #                 if base_media_fee is not None:
    #                     media_fee = -(base_media_fee * 1.1)  # 消費税を加算
    #                     media_fee = round(media_fee)  # 四捨五入して整数に
                    
    #                 # 価格に応じた手数料率を決定
    #                 if isinstance(fee_rates, list):
    #                     # 配列形式の場合（新形式）
    #                     for rate_info in fee_rates:
    #                         upper_limit = rate_info.get('上限金額')
    #                         if upper_limit is None or price <= upper_limit:
    #                             fee_rate = rate_info.get('料率')
    #                             break
    #                 elif isinstance(fee_rates, dict):
    #                     # 辞書形式の場合（旧形式 - 互換性のため）
    #                     if price <= 750 and '750円以下' in fee_rates:
    #                         fee_rate = fee_rates['750円以下']
    #                     elif 750 < price <= 1500 and '750円超 1500円以下' in fee_rates:
    #                         fee_rate = fee_rates['750円超 1500円以下']
    #                     elif price > 1500 and '1500円超' in fee_rates:
    #                         fee_rate = fee_rates['1500円超']
    #                     elif '750円超' in fee_rates and price > 750:
    #                         fee_rate = fee_rates['750円超']
    #                     elif 'default' in fee_rates:
    #                         fee_rate = fee_rates['default']
    #                 else:
    #                     # 数値の場合（旧旧形式 - さらなる互換性のため）
    #                     fee_rate = fee_rates
                
    #             return pd.Series([category_name, fee_category, fee_rate, media_fee])
            
    #         # カテゴリ情報と手数料率を列に追加
    #         # 修正: カテゴリー列の名前を「カテゴリー_keepa」に変更
    #         if 'カテゴリー_keepa' in result_df.columns and '販売価格_設定販売額' in result_df.columns:
    #             # apply関数で複数の値を同時に返す
    #             result_df[['商品情報_カテゴリ', '販売手数料カテゴリ', '手数料・利益_販売手数料率', '手数料・利益_メディア手数料']] = (
    #                 result_df.apply(get_category_info_and_fee_rate, axis=1)
    #             )
                
    #             # 手数料率をパーセント表示用に変換（例: 0.15 → 15%）
    #             result_df['手数料・利益_販売手数料率_表示用'] = result_df['手数料・利益_販売手数料率'].apply(
    #                 lambda x: f"{x*100:.1f}%" if pd.notna(x) else "対象外"
    #             )
                
    #             # 販売手数料の計算（最低販売手数料を考慮）
    #             def calculate_fee(row):
    #                 if pd.isna(row['手数料・利益_販売手数料率']) or pd.isna(row['販売価格_設定販売額']):
    #                     return None
                    
    #                 category_name = row['商品情報_カテゴリ']
    #                 min_fee = 0
    #                 if category_name in category_mapping:
    #                     min_fee = category_mapping[category_name].get('最低販売手数料', 0)
                    
    #                 calculated_fee = row['販売価格_設定販売額'] * row['手数料・利益_販売手数料率']
                    
    #                 # 最低手数料がnullの場合は最低料金の制約なし
    #                 if min_fee is None:
    #                     return calculated_fee
                    
    #                 # 最低手数料と計算手数料の大きい方を採用
    #                 return max(calculated_fee, min_fee)
                
    #             # 販売手数料を計算して列に追加（小数点第一位で四捨五入）
    #             result_df['手数料・利益_販売手数料'] = result_df.apply(calculate_fee, axis=1).apply(
    #                 lambda x: -round(x) if pd.notna(x) else None
    #             )
                
    #             # 販売手数料（税込）を計算して列に追加（手数料に10%の消費税を加算し、小数点第一位で四捨五入）
    #             result_df['手数料・利益_販売手数料(税込)'] = result_df['手数料・利益_販売手数料'].apply(
    #                 lambda x: round(x * 1.1) if pd.notna(x) else None
    #             )
                
    #             # デバッグ: 結果を確認
    #             print("\n計算結果のサンプル（最初の5行）:")
    #             for col in ['カテゴリー_keepa', '商品情報_カテゴリ', '販売手数料カテゴリ', '手数料・利益_販売手数料率_表示用']:
    #                 if col in result_df.columns:
    #                     print(f"{col}: {result_df[col].head(5).tolist()}")
                
    #             logger.info("カテゴリ情報と販売手数料率の列を追加しました")
    #         else:
    #             missing_cols = []
    #             if 'カテゴリー_keepa' not in result_df.columns:
    #                 missing_cols.append('カテゴリー_keepa')
    #             if '販売価格_設定販売額' not in result_df.columns:
    #                 missing_cols.append('販売価格_設定販売額')
                
    #             logger.warning(f"カテゴリ計算に必要な列がありません: {', '.join(missing_cols)}")
    #             print(f"⚠️ カテゴリ計算に必要な列がありません: {', '.join(missing_cols)}")
            
    #         return result_df
            
    #     except Exception as e:
    #         logger.error(f"カテゴリ計算処理エラー: {str(e)}")
    #         traceback.print_exc()
    #         return df
        

    def add_sourcing_price_calculations(self, df):
        """
        仕入れ情報（ネッシー、スーデリ）から最安値情報を計算するメソッド
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # 仕入れサイト情報の設定（ネッシーとスーデリのみ）
            sourcing_sites = [
                {
                    'name': 'ネッシー',
                    'price_column': 'ネッシー_価格',
                    'is_tax_included': False,  # 税抜き価格の場合はFalse
                    'url_prefix': 'https://www.netsea.jp/search/?keyword=',
                    'url_column': None  # 特定のURL列がない場合はNone
                },
                {
                    'name': 'スーデリ',
                    'price_column': 'スーデリ_価格',
                    'is_tax_included': False,  # 税抜き価格の場合はFalse
                    'url_prefix': 'https://www.superdelivery.com/p/do/psl/?so=score&vi=1&sb=all&word=',
                    'url_column': None
                }
                # ヤフーと楽天の処理はadd_yahoo_rakuten_calculationsメソッドで行うため削除
            ]
            
            # 各行について最安値と対応するURLを計算
            def find_cheapest_price_and_url(row):
                min_price = float('inf')  # 初期値は無限大
                min_price_site = None
                min_price_url = None
                
                for site in sourcing_sites:
                    price_column = site['price_column']
                    
                    # 列が存在し、値がある場合のみ処理
                    if price_column in row and pd.notna(row[price_column]):
                        # 価格を取得
                        try:
                            # 価格が文字列の場合（例：「【3966】」）は数値に変換
                            if isinstance(row[price_column], str):
                                # 数値部分だけを抽出
                                price_str = re.search(r'\d+', row[price_column])
                                if price_str:
                                    price = float(price_str.group())
                                else:
                                    continue
                            else:
                                price = float(row[price_column])
                            
                            # 税抜き価格の場合は税込みに変換
                            if not site['is_tax_included']:
                                price = price * 1.1
                            
                            # 最安値を更新
                            if price < min_price:
                                min_price = price
                                min_price_site = site
                                
                                # URLを取得
                                if site['url_column'] and site['url_column'] in row and pd.notna(row[site['url_column']]):
                                    min_price_url = row[site['url_column']]
                                elif 'JAN' in row and pd.notna(row['JAN']):
                                    min_price_url = site['url_prefix'] + str(row['JAN'])
                        except (ValueError, TypeError):
                            continue
                
                # 最安値が見つからなかった場合
                if min_price_site is None:
                    return pd.Series([None, None])
                
                return pd.Series([round(min_price), min_price_url])
            
            # 最安値とURLを列に追加
            if 'JAN' in result_df.columns:
                # 各サイトの価格列が存在するか確認
                existing_sites = []
                for site in sourcing_sites:
                    if site['price_column'] in result_df.columns:
                        existing_sites.append(site)
                
                if existing_sites:
                    print(f"📊 仕入れ価格計算: {len(existing_sites)}サイトの価格情報があります")
                    logger.info(f"仕入れ価格計算: {len(existing_sites)}サイトの価格情報があります")
                    
                    # サイト情報の表示
                    for site in existing_sites:
                        non_null_count = result_df[site['price_column']].notna().sum()
                        print(f"  - {site['name']}: {non_null_count}件の価格情報")
                    
                    # 最安値とURLを計算
                    result_df[['JAN価格_JAN価格下代(税込)', 'JAN価格_商品URL']] = result_df.apply(
                        find_cheapest_price_and_url, axis=1
                    )
                    
                    # 計算結果の統計
                    non_null_price = result_df['JAN価格_JAN価格下代(税込)'].notna().sum()
                    print(f"✅ JAN価格計算完了: {non_null_price}件の最安値情報を追加しました")
                    logger.info(f"JAN価格計算完了: {non_null_price}件の最安値情報を追加しました")
                else:
                    print("⚠️ 仕入れ価格列が見つかりません")
                    logger.warning("仕入れ価格列が見つかりません")
            else:
                print("⚠️ JAN列が見つからないため、仕入れ価格計算をスキップします")
                logger.warning("JAN列が見つからないため、仕入れ価格計算をスキップします")
            
            return result_df
            
        except Exception as e:
            logger.error(f"仕入れ価格計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df

    def add_yahoo_rakuten_calculations(self, df):
        """
        ヤフー、楽天の仕入れ情報を処理するメソッド
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # ポイント率の設定を取得（デフォルト値を設定）
            yahoo_point_rate = self.config.get('calculator', {}).get('point_rate', {}).get('yahoo', 0.05)
            rakuten_point_rate = self.config.get('calculator', {}).get('point_rate', {}).get('rakuten', 0.02)
            
            # 1. ポイントを加味した価格列の追加
            for i in range(1, 4):  # 1～3の3つを処理
                # ヤフーの価格処理
                yahoo_price_col = f'ヤフー_価格_{i}'
                if yahoo_price_col in result_df.columns:
                    # ポイント加味価格 = 価格 * (1 - ポイント率)
                    result_df[f'ヤフー_価格_ポイント加味_{i}'] = result_df[yahoo_price_col].apply(
                        lambda x: int(round(x * (1 - yahoo_point_rate))) if pd.notna(x) else None
                    )
                
                # 楽天の価格処理
                rakuten_price_col = f'楽天_価格_{i}'
                if rakuten_price_col in result_df.columns:
                    # ポイント加味価格 = 価格 * (1 - ポイント率)
                    result_df[f'楽天_価格_ポイント加味_{i}'] = result_df[rakuten_price_col].apply(
                        lambda x: int(round(x * (1 - rakuten_point_rate))) if pd.notna(x) else None
                    )
            
            # 2. 送料条件を加味した表示形式の列を追加
            for i in range(1, 4):  # 1～3の3つを処理
                # ヤフーの送料条件付き表示
                yahoo_price_col = f'ヤフー_価格_ポイント加味_{i}'
                yahoo_shipping_col = f'ヤフー_送料条件_{i}'
                
                if yahoo_price_col in result_df.columns and yahoo_shipping_col in result_df.columns:
                    result_df[f'ヤフー_価格_ポイント加味_送料条件_{i}'] = result_df.apply(
                        lambda row: f"【{int(row[yahoo_price_col])}】" if pd.notna(row[yahoo_price_col]) and pd.notna(row[yahoo_shipping_col]) and 
                                    (row[yahoo_shipping_col] == '送料無料') else
                                    (f"〈{int(row[yahoo_price_col])}〉" if pd.notna(row[yahoo_price_col]) and pd.notna(row[yahoo_shipping_col]) and 
                                    (row[yahoo_shipping_col] == '条件付き送料無料') else
                                    (str(int(row[yahoo_price_col])) if pd.notna(row[yahoo_price_col]) else None)),
                        axis=1
                    )
                
                # 楽天の送料条件付き表示
                rakuten_price_col = f'楽天_価格_ポイント加味_{i}'
                rakuten_shipping_col = f'楽天_送料条件_{i}'
                
                if rakuten_price_col in result_df.columns and rakuten_shipping_col in result_df.columns:
                    result_df[f'楽天_価格_ポイント加味_送料条件_{i}'] = result_df.apply(
                        lambda row: f"【{int(row[rakuten_price_col])}】" if pd.notna(row[rakuten_price_col]) and pd.notna(row[rakuten_shipping_col]) and 
                                    (row[rakuten_shipping_col] == '送料込み') else
                                    (str(int(row[rakuten_price_col])) if pd.notna(row[rakuten_price_col]) else None),
                        axis=1
                    )

                # 列の順序を調整
                if any(col.startswith(('ヤフー_価格_ポイント加味_', '楽天_価格_ポイント加味_')) for col in result_df.columns):
                    # 現在の列リスト
                    current_cols = list(result_df.columns)
                    
                    # 新しい列リストを構築
                    ordered_cols = []
                    
                    # 最初に列を追加する（ポイント加味や送料条件以外）
                    for col in current_cols:
                        if not col.startswith(('ヤフー_価格_ポイント加味_', '楽天_価格_ポイント加味_')) and col not in ['楽天_価格ナビURL', 'ヨリヤス_比較URL']:
                            ordered_cols.append(col)
                    
                    # ヤフーのポイント加味列を順に追加
                    for i in range(1, 4):
                        col = f'ヤフー_価格_ポイント加味_{i}'
                        if col in current_cols:
                            ordered_cols.append(col)
                        
                        col = f'ヤフー_価格_ポイント加味_送料条件_{i}'
                        if col in current_cols:
                            ordered_cols.append(col)
                    
                    # 楽天のポイント加味列を順に追加
                    for i in range(1, 4):
                        col = f'楽天_価格_ポイント加味_{i}'
                        if col in current_cols:
                            ordered_cols.append(col)
                        
                        col = f'楽天_価格_ポイント加味_送料条件_{i}'
                        if col in current_cols:
                            ordered_cols.append(col)
                    
                    # URL列を最後に追加
                    if '楽天_価格ナビURL' in current_cols:
                        ordered_cols.append('楽天_価格ナビURL')
                    if 'ヨリヤス_比較URL' in current_cols:
                        ordered_cols.append('ヨリヤス_比較URL')
                    
                    # 列の順序を適用
                    result_df = result_df[ordered_cols]
            
            # 3. 楽天価格ナビURLの作成
            if 'JAN' in result_df.columns:
                result_df['楽天_価格ナビURL'] = result_df['JAN'].apply(
                    lambda x: f"https://search.rakuten.co.jp/search/mall/{x}/?s=2" if pd.notna(x) else None
                )
            
            # 4. ヨリヤス比較URLの作成（出力されるように修正）
            if 'JAN' in result_df.columns:
                # notna()チェックを明示的に行い、値が欠損値でない場合のみURLを生成
                result_df['ヨリヤス_比較URL'] = result_df['JAN'].apply(
                    lambda x: f"https://yoriyasu.jp/products?keyword={x}&sort=priceLow&page=1" if pd.notna(x) and str(x).strip() != '' else None
                )
                # 作成されたURLの件数を表示（デバッグ用）
                url_count = result_df['ヨリヤス_比較URL'].notna().sum()
                print(f"  - ヨリヤス比較URL: {url_count}件生成")
            
            print(f"✅ ヤフー・楽天情報処理完了: 価格・ポイント情報を追加しました")

            # ヤフーと楽天の最安値を比較して「ネット価格_実質最安値」列を作成
            yahoo_min_col = 'ヤフー_価格_ポイント加味_1'
            rakuten_min_col = '楽天_価格_ポイント加味_1'

            if yahoo_min_col in result_df.columns and rakuten_min_col in result_df.columns:
                # 両方の列が存在する場合、小さい方を選択
                result_df['ネット価格_実質最安値'] = result_df.apply(
                    lambda row: min(row[yahoo_min_col], row[rakuten_min_col]) 
                                if pd.notna(row[yahoo_min_col]) and pd.notna(row[rakuten_min_col]) 
                                else (row[yahoo_min_col] if pd.notna(row[yahoo_min_col]) 
                                    else (row[rakuten_min_col] if pd.notna(row[rakuten_min_col]) 
                                            else None)),
                    axis=1
                )
                
                # 値が入っている行数をカウント
                non_null_count = result_df['ネット価格_実質最安値'].notna().sum()
                print(f"  - ネット価格_実質最安値: {non_null_count}件の値を計算")               
            return result_df
            
        except Exception as e:
            logger.error(f"ヤフー・楽天情報処理エラー: {str(e)}")
            traceback.print_exc()
            return df
        
    def add_profit_calculations(self, df):
        """
        手数料合計と利益に関する計算を行うメソッド
        各種手数料の合計や利益額、利益率を計算します
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # 手数料関連の列が存在するか確認
            fee_columns = [
                '手数料・利益_販売手数料(税込)',
                '手数料・利益_発送代行手数料',
                '手数料・利益_メディア手数料',
                '手数料・利益_月額保管料'
            ]
            
            # 存在する列のみを対象とする
            existing_fee_columns = [col for col in fee_columns if col in result_df.columns]
            
            if not existing_fee_columns:
                logger.warning("手数料関連の列が見つかりませんでした。手数料合計は計算できません。")
            else:
                # 手数料合計を計算する関数
                def calculate_total_fee(row):
                    total = 0
                    
                    # 各手数料を合計（Noneの場合は0として扱う）
                    for col in existing_fee_columns:
                        value = row[col]
                        if pd.notna(value):
                            total += value
                    
                    return total
                
                # 手数料合計を計算して列に追加
                result_df['手数料・利益_手数料合計'] = result_df.apply(calculate_total_fee, axis=1)
                logger.info("手数料合計の列を追加しました")
            
            # 仕入れ価格の最安値を計算する関数を修正
            def calculate_real_cost(row):
                # JAN価格_JAN価格下代(税込)を確認
                jan_price = None
                if 'JAN価格_JAN価格下代(税込)' in row and pd.notna(row['JAN価格_JAN価格下代(税込)']):
                    jan_price = float(row['JAN価格_JAN価格下代(税込)'])
                
                # ネット価格_実質最安値を確認
                net_price = None
                if 'ネット価格_実質最安値' in row and pd.notna(row['ネット価格_実質最安値']):
                    net_price = float(row['ネット価格_実質最安値'])
                
                # JAN価格とネット価格を比較して小さい方（安い方）を採用
                if jan_price is not None and net_price is not None:
                    return min(jan_price, net_price)
                elif jan_price is not None:
                    return jan_price
                elif net_price is not None:
                    return net_price
                else:
                    # 修正: 0ではなくNoneを返す
                    return None
            
            # 利益額の計算
            if '販売価格_設定販売額' in result_df.columns and '手数料・利益_手数料合計' in result_df.columns:
                # 実質最安値を計算して列に追加
                result_df['仕入価格_実質最安値'] = result_df.apply(calculate_real_cost, axis=1)
                
                # 修正: 実質最安値が0（値がない場合）は計算しない
                result_df['手数料・利益_利益額'] = result_df.apply(
                    lambda row: round(
                        row['販売価格_設定販売額'] - row['仕入価格_実質最安値'] + row['手数料・利益_手数料合計']
                    ) if pd.notna(row['販売価格_設定販売額']) and pd.notna(row['仕入価格_実質最安値']) and row['仕入価格_実質最安値'] > 0 else None, 
                    axis=1
                )
                
                logger.info("利益額の列を追加しました")

                # 修正: 利益額がNoneの場合は利益率も計算しない
                result_df['手数料・利益_利益率'] = result_df.apply(
                    lambda row: f"{round((row['手数料・利益_利益額'] / row['販売価格_設定販売額']) , 3)}" 
                    if pd.notna(row['手数料・利益_利益額']) and pd.notna(row['販売価格_設定販売額']) and row['販売価格_設定販売額'] > 0 else None,
                    axis=1
                )
                
                logger.info("利益率の列を追加しました")
            
            return result_df
            
        except Exception as e:
            logger.error(f"利益計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df

    def add_expected_sales_calculations(self, df):
        """
        期待販売数と期待利益に関する計算を行うメソッド
        期待販売数や期待利益を計算します
        
        Parameters:
        -----------
        df : pandas.DataFrame
            処理対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が追加されたデータフレーム
        """
        try:
            # 元のデータフレームのコピーを作成
            result_df = df.copy()
            
            # 期待販売数(1ヶ月)の計算
            if '30日間_新品販売数' in result_df.columns and 'FBA数' in result_df.columns:
                # 期待販売数を計算（月間販売数 ÷ (FBA出品者数 + 1)）
                result_df['期待販売数・利益_販売期待数(1ヶ月)'] = result_df.apply(
                    lambda row: round(
                        row['30日間_新品販売数'] / (row['FBA数'] + 1)
                    ) if pd.notna(row['30日間_新品販売数']) and pd.notna(row['FBA数']) and (row['FBA数'] + 1) > 0 else 0,
                    axis=1
                )
                
                logger.info("期待販売数(1ヶ月)の列を追加しました")
            
            # 期待利益(1ヶ月)の計算
            if '期待販売数・利益_販売期待数(1ヶ月)' in result_df.columns and '手数料・利益_利益額' in result_df.columns:
                result_df['期待販売数・利益_期待利益(1ヶ月)'] = result_df.apply(
                    lambda row: round(
                        row['期待販売数・利益_販売期待数(1ヶ月)'] * row['手数料・利益_利益額']
                    ) if pd.notna(row['期待販売数・利益_販売期待数(1ヶ月)']) and pd.notna(row['手数料・利益_利益額']) else None,
                    axis=1
                )
                
                logger.info("期待利益(1ヶ月)の列を追加しました")
            
            # 期待利益(3ヶ月)の計算
            if '90日間_新品販売数' in result_df.columns and '手数料・利益_利益額' in result_df.columns:
                # 3ヶ月先は出品者数の増減が予測困難なため、平均的な出品者数として4を使用
                result_df['期待販売数・利益_期待利益(3ヶ月)'] = result_df.apply(
                    lambda row: round(
                        (row['90日間_新品販売数'] / 4) * row['手数料・利益_利益額']
                    ) if pd.notna(row['90日間_新品販売数']) and pd.notna(row['手数料・利益_利益額']) else None,
                    axis=1
                )
                
                logger.info("期待利益(3ヶ月)の列を追加しました")
            
            return result_df
            
        except Exception as e:
            logger.error(f"期待販売数・利益計算処理エラー: {str(e)}")
            traceback.print_exc()
            return df

    def process(self):
        """
        メイン処理を実行
        
        CSVファイルを読み込み、計算処理を行い、結果を保存します。
        
        Returns:
        --------
        pandas.DataFrame
            計算後のデータフレーム（成功した場合）
        None
            エラーが発生した場合
        """
        try:
            logger.info("計算処理を開始します")
            
            # 各種パスの確認と表示
            print(f"📂 プロジェクトルートディレクトリ: {self.root_dir}")
            print(f"📂 データディレクトリ: {self.data_dir}")
            print(f"📂 ログディレクトリ: {self.log_dir}")
            
            # 設定ファイルから読み込んだファイル名を表示
            config = self.config['calculator']['output']
            print(f"\n📄 設定ファイルの情報:")
            print(f"  - 入力ファイル: {config.get('input_file', 'integrated_data.csv')}")
            print(f"  - 出力ファイル: {config.get('output_file', 'calculated_data.csv')}")
            
            # データの読み込み
            df = self.load_data()
            
            # 列名の確認
            logger.info(f"入力データの列: {', '.join(df.columns)}")
            
            # 工程1: 基本的な計算処理
            print("\n📊 工程1: 基本的な計算処理を実行中...")
            result_df = self.add_calculation_columns(df)
            
            # 工程2-1: サイズ計算処理
            print("📊 工程2-1: サイズ計算処理を実行中...")
            result_df = self.add_size_calculations(result_df)
    
            # 工程2-2: カテゴリ計算処理
            print("📊 工程2-2: カテゴリ計算処理を実行中...")
            result_df = self.add_category_calculations(result_df)
            
            # 工程2-3-1: 仕入れ価格計算処理
            print("📊 工程2-3-1: 仕入れ価格計算処理を実行中...")
            result_df = self.add_sourcing_price_calculations(result_df)
            
            # 工程2-3-2: ヤフー・楽天情報処理
            print("📊 工程2-3-2: ヤフー・楽天情報処理を実行中...")
            result_df = self.add_yahoo_rakuten_calculations(result_df)
            
            # 工程3-1: 手数料合計・利益計算処理
            print("📊 工程3-1: 手数料合計・利益計算処理を実行中...")
            result_df = self.add_profit_calculations(result_df)
            
            # 工程3-2: 期待販売数・期待利益計算処理
            print("📊 工程3-2: 期待販売数・期待利益計算処理を実行中...")
            result_df = self.add_expected_sales_calculations(result_df)
            
            # データの保存
            self.save_data(result_df)
            
            # 処理結果の概要を表示
            self.print_summary(df, result_df)
            
            logger.info("計算処理が正常に完了しました")
            return result_df
            
        except Exception as e:
            logger.error(f"処理全体でエラーが発生: {str(e)}")
            print(f"❌ エラーが発生しました: {str(e)}")
            traceback.print_exc()
            return None
    
    def print_summary(self, original_df, result_df):
        """
        処理結果の概要を表示
        
        Parameters:
        -----------
        original_df : pandas.DataFrame
            元のデータフレーム
        result_df : pandas.DataFrame
            処理後のデータフレーム
        
        Returns:
        --------
        pandas.DataFrame
            表示用のデータフレーム
        """
        # 仕入れソース列を除外した表示用データフレームを作成
        display_df = result_df.copy()
        columns_to_drop = [col for col in display_df.columns if 
                         col.startswith('ネッシー_') or 
                         col.startswith('スーデリ_') or 
                         col.startswith('ヤフー_') or 
                         col.startswith('ヨリヤス_')]
        
        if columns_to_drop:
            display_df = display_df.drop(columns=columns_to_drop)
            print(f"ℹ️ 表示から{len(columns_to_drop)}列の仕入れソースデータを除外しました")
        
        # 追加された列（仕入れソース列を除く）
        new_columns = [col for col in display_df.columns if col not in original_df.columns]
        
        print("\n=== 処理結果のサマリー ===")
        print(f"・入力データ: {len(original_df)}行, {len(original_df.columns)}列")
        print(f"・出力データ: {len(display_df)}行, {len(display_df.columns)}列")
        print(f"・追加された列: {len(new_columns)}列")
        
        if new_columns:
            print("\n追加された列の一覧:")
            for col in new_columns:
                print(f"・{col}")
        
        print(f"\n✨ 処理が完了しました！")
        
        # 処理結果のサンプルとしてdisplay_dfを返す
        return display_df


# テスト用の実行コード
if __name__ == "__main__":
    # 計算処理クラスのインスタンスを作成
    calculator = ProductCalculator()
    
    # 処理を実行
    result_df = calculator.process()
    
    # 成功したかどうかの確認
    if result_df is not None:
        print("✅ データ計算処理が成功しました！")
    else:
        print("❌ データ計算処理に失敗しました。")