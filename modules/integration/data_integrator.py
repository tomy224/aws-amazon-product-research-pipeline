#!/usr/bin/env python
# coding: utf-8

"""
データ統合モジュール

このモジュールは、SP-APIとKeepa APIのデータを統合するための機能を提供します。
統合されたデータはCSVファイルとして保存されます。
"""

import sys
import os
from pathlib import Path

# プロジェクトルートをモジュール検索パスに追加
project_root = str(Path(__file__).resolve().parents[2])  # modules/integration から2階層上
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import logging
from datetime import datetime
import re

# 自作モジュールのインポート
from modules.utils.file_utils import find_project_root, load_yaml_config
from modules.utils.logger_utils import setup_logging, get_logger

# ロガーの取得
logger = get_logger(__name__)

class DataIntegrator:
    """SP-APIとKeepa APIのデータを統合するクラス"""
    
    def __init__(self, config_path=None):
        """
        初期化と設定読み込み
        
        Parameters:
        -----------
        config_path : str, optional
            設定ファイルのパス。指定がない場合はデフォルトパスを使用
        """
        # プロジェクトルートディレクトリの検出
        self.root_dir = find_project_root()
        
        # データディレクトリとログディレクトリのパスを設定
        self.data_dir = os.path.join(self.root_dir, 'data')
        self.log_dir = os.path.join(self.root_dir, 'logs')
        
        # ディレクトリの存在確認
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
            
        # 設定ファイルの読み込み
        self.config = self._load_config(config_path)
        
        # ログ機能の設定
        self._setup_logging()
    
    def _load_config(self, config_path=None):
        """設定ファイルを読み込む"""
        # load_yaml_configを使用して設定を読み込む
        config = load_yaml_config(config_path)
        
        # データ統合設定の存在確認
        if 'data_integration' not in config:
            config['data_integration'] = {}
            
        # output設定の初期化（なければデフォルト値を設定）
        if 'output' not in config['data_integration']:
            config['data_integration']['output'] = {
                'sp_api_input': 'sp_api_output_filtered.csv',
                'keepa_input': 'keepa_output.csv',
                'output_file': 'integrated_data.csv'
            }
            
        # sources設定の初期化（なければ空リストを設定）
        if 'sources' not in config['data_integration']:
            config['data_integration']['sources'] = []
            
        logger.info(f"設定ファイルを読み込みました")
        return config
    
    def _setup_logging(self):
        """ログ機能のセットアップ"""
        log_file = setup_logging(
            self.log_dir, 
            "integration", 
            console_level=logging.INFO, 
            file_level=logging.DEBUG
        )
        logger.info("データ統合処理を開始します")
        print(f"📄 ログファイル: {log_file}")

    def load_data(self, sp_api_file=None, keepa_file=None):
        """
        CSVファイルの読み込み
        
        Parameters:
        -----------
        sp_api_file : str, optional
            SP-APIデータのファイルパス（デフォルトは設定ファイルから）
        keepa_file : str, optional
            Keepaデータのファイルパス（デフォルトは設定ファイルから）
            
        Returns:
        --------
        tuple
            (sp_df, keepa_df) - 読み込んだデータフレーム
        """
        try:
            # 入力ファイル名が指定されていない場合は設定ファイルから取得
            if sp_api_file is None:
                sp_api_file = self.config['data_integration']['output']['sp_api_input']
                
            if keepa_file is None:
                keepa_file = self.config['data_integration']['output']['keepa_input']
            
            # 相対パスの場合はdataディレクトリを基準にする
            if not os.path.isabs(sp_api_file):
                sp_api_file = os.path.join(self.data_dir, sp_api_file)
                
            if not os.path.isabs(keepa_file):
                keepa_file = os.path.join(self.data_dir, keepa_file)
            
            # ファイルの存在確認とエラーメッセージの改善
            if not os.path.exists(sp_api_file):
                error_msg = f"SP-APIファイルが見つかりません: {sp_api_file}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
                
            if not os.path.exists(keepa_file):
                error_msg = f"Keepaファイルが見つかりません: {keepa_file}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            # SP-APIデータの読み込み - JANを文字列として読み込む
            sp_df = pd.read_csv(sp_api_file, encoding='utf-8-sig', dtype={'JAN': str, '元コード': str})
            logger.info(f"SP-APIデータを読み込みました: {len(sp_df)}件")
            print(f"📊 SP-APIデータ: {len(sp_df)}件")
            
            # Keepaデータの読み込み - JANを文字列として読み込む
            keepa_df = pd.read_csv(keepa_file, encoding='utf-8-sig', dtype={'JAN': str})
            logger.info(f"Keepaデータを読み込みました: {len(keepa_df)}件")
            print(f"📊 Keepaデータ: {len(keepa_df)}件")
            
            # JANが浮動小数点になっている場合の修正処理
            if 'JAN' in sp_df.columns:
                sp_df['JAN'] = sp_df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            if 'JAN' in keepa_df.columns:
                keepa_df['JAN'] = keepa_df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            
            return sp_df, keepa_df
            
        except Exception as e:
            logger.error(f"データ読み込みエラー: {str(e)}")
            raise

    def load_source_data(self, source_config):
        """
        ソース設定に基づいてCSVファイルを読み込む
        
        Parameters:
        -----------
        source_config : dict
            ソース設定情報
            
        Returns:
        --------
        dict
            ファイル名をキー、データフレームを値とする辞書
        """
        result = {}
        
        try:
            files = source_config.get('files', [])
            key_column = source_config.get('key_column', 'JAN')
            
            # JAN列の代替名リスト（よくある命名パターン）
            jan_column_alternatives = ['JAN', 'JANコード', 'jan', 'jancode', 'jan_code', 'ean', 'EAN']
            
            for file in files:
                try:
                    # 相対パスの場合はプロジェクトのdataディレクトリを基準にする
                    file_path = file
                    if not os.path.isabs(file_path):
                        file_path = os.path.join(self.data_dir, file_path)
                    
                    # ファイルの存在確認
                    if not os.path.exists(file_path):
                        logger.warning(f"ファイルが見つかりません: {file_path}。スキップします。")
                        print(f"⚠️ ファイルが見つかりません: {file_path}。スキップします。")
                        continue
                    
                    # CSVファイルを読み込む
                    df = pd.read_csv(file_path, encoding='utf-8-sig')
                    
                    # 実際のキー列を特定
                    actual_key_column = key_column  # デフォルト値
                    
                    # 設定されたキー列がない場合は代替名から探す
                    if key_column not in df.columns:
                        # 代替名リストから列名を探す
                        for alt_column in jan_column_alternatives:
                            if alt_column in df.columns:
                                actual_key_column = alt_column
                                print(f"ℹ️ {file}では「{key_column}」の代わりに「{actual_key_column}」を使用します")
                                break
                    
                    # キー列の存在確認
                    if actual_key_column not in df.columns:
                        logger.warning(f"キー列 '{key_column}' またはその代替名が {file} に見つかりません。スキップします。")
                        print(f"⚠️ キー列 '{key_column}' またはその代替名が {file} に見つかりません。スキップします。")
                        print(f"  利用可能な列: {', '.join(df.columns)}")
                        continue
                    
                    # 結果に追加
                    result[file] = {
                        'df': df,
                        'key_column': actual_key_column
                    }
                    
                    logger.info(f"ソースデータを読み込みました: {file} ({len(df)}件), キー列: {actual_key_column}")
                    print(f"📊 {file}を読み込みました: {len(df)}件, キー列: {actual_key_column}")
                    
                except Exception as e:
                    logger.error(f"ファイル {file} の読み込みエラー: {str(e)}")
                    print(f"⚠️ {file}の読み込みエラー: {str(e)}")
                    continue
                    
            return result
            
        except Exception as e:
            logger.error(f"ソースデータ読み込み全体エラー: {str(e)}")
            return {}


    
    def _merge_yahoo_rakuten_data(self, base_df, source_data):
        """
        Yahoo/Rakutenデータを横展開して結合する特別メソッド（デバッグ強化版）
        
        Parameters:
        -----------
        base_df : pandas.DataFrame
            ベースのデータフレーム
        source_data : dict
            ヤフー/楽天の価格データ
                
        Returns:
        --------
        pandas.DataFrame
            結合後のデータフレーム
        """
        result_df = base_df.copy()
        
        # 🔍 ベースデータのASIN情報をデバッグ出力
        print(f"\n🔍 Yahoo/Rakuten結合デバッグ情報:")
        
        if 'ASIN' not in result_df.columns:
            logger.warning("ベースデータにASIN列がありません。Yahoo/Rakutenデータは結合できません。")
            print("⚠️ ベースデータにASIN列がありません。Yahoo/Rakutenデータは結合できません。")
            return result_df
        
        # ベースデータのASIN情報を詳細チェック
        base_asins = result_df['ASIN'].dropna().unique()
        print(f"  📊 ベースデータ（Keepa）のASIN:")
        print(f"     - 総ASIN数: {len(base_asins)}件")
        print(f"     - ASIN例: {list(base_asins)[:5]}...")  # 最初の5つを表示
        
        # ASIN列を文字列に変換
        result_df['ASIN'] = result_df['ASIN'].astype(str).str.replace('.0$', '', regex=True)
        
        total_added_columns = 0
        
        for file, data in source_data.items():
            source_df = data['df']
            key_column = data['key_column']
            
            print(f"\n  📄 処理中ファイル: {file}")
            print(f"     - キー列: {key_column}")
            print(f"     - 総行数: {len(source_df)}件")
            
            # 🔍 ソースデータのASIN情報をデバッグ出力
            if key_column in source_df.columns:
                # キー列を文字列に変換
                source_df[key_column] = source_df[key_column].astype(str).str.replace('.0$', '', regex=True)
                
                source_asins = source_df[key_column].dropna().unique()
                print(f"     - ソースASIN数: {len(source_asins)}件")
                print(f"     - ソースASIN例: {list(source_asins)[:5]}...")
                
                # 🔑 ASIN照合状況を詳細チェック
                common_asins = set(base_asins).intersection(set(source_asins))
                print(f"     - 共通ASIN: {len(common_asins)}件")
                
                if len(common_asins) > 0:
                    print(f"     - 共通ASIN例: {list(common_asins)[:5]}...")
                else:
                    print(f"     ⚠️ 共通ASINが0件です！")
                    print(f"     🔍 ベースASIN型: {type(base_asins[0]) if len(base_asins) > 0 else 'なし'}")
                    print(f"     🔍 ソースASIN型: {type(source_asins[0]) if len(source_asins) > 0 else 'なし'}")
                    
                    # さらに詳細な比較
                    if len(base_asins) > 0 and len(source_asins) > 0:
                        print(f"     🔍 ベースASIN[0]: '{base_asins[0]}' (長さ: {len(str(base_asins[0]))})")
                        print(f"     🔍 ソースASIN[0]: '{source_asins[0]}' (長さ: {len(str(source_asins[0]))})")
            
            # 🔍 API列の存在確認
            if 'API' not in source_df.columns:
                print(f"     ⚠️ API列がありません。Yahoo/Rakuten分類ができません。")
                continue
            
            # API列の値を確認
            api_values = source_df['API'].value_counts()
            print(f"     - API分布: {dict(api_values)}")
            
            # API列で分類
            yahoo_df = source_df[source_df['API'] == 'Yahoo'].copy()
            rakuten_df = source_df[source_df['API'] == 'Rakuten'].copy()
            
            print(f"     - Yahoo行数: {len(yahoo_df)}件")
            print(f"     - Rakuten行数: {len(rakuten_df)}件")
            
            # 列追加前の列数を記録
            before_columns = len(result_df.columns)
            
            # 各ASINごとに上位3件を取得してマージする処理
            base_asin_list = result_df['ASIN'].dropna().unique()
            
            added_yahoo_data = 0
            added_rakuten_data = 0
            
            for asin in base_asin_list:
                # Yahoo情報の取得（上位3件）
                yahoo_rows = yahoo_df[yahoo_df[key_column] == asin].head(3)
                for i, (_, row) in enumerate(yahoo_rows.iterrows(), 1):
                    # プレフィックスを付けた列名でデータを追加
                    for col in ['価格', '送料条件', '商品URL']:
                        if col in row and pd.notna(row[col]):
                            col_name = f'ヤフー_{col}_{i}'
                            result_df.loc[result_df['ASIN'] == asin, col_name] = row[col]
                            added_yahoo_data += 1
                
                # Rakuten情報の取得（上位3件）
                rakuten_rows = rakuten_df[rakuten_df[key_column] == asin].head(3)
                for i, (_, row) in enumerate(rakuten_rows.iterrows(), 1):
                    # プレフィックスを付けた列名でデータを追加
                    for col in ['価格', '送料条件', '商品URL']:
                        if col in row and pd.notna(row[col]):
                            col_name = f'楽天_{col}_{i}'
                            result_df.loc[result_df['ASIN'] == asin, col_name] = row[col]
                            added_rakuten_data += 1
            
            # 列追加後の列数を記録
            after_columns = len(result_df.columns)
            added_columns = after_columns - before_columns
            total_added_columns += added_columns
            
            print(f"     ✅ データ追加結果:")
            print(f"        - 追加列数: {added_columns}列")
            print(f"        - Yahooデータ追加: {added_yahoo_data}個")
            print(f"        - Rakutenデータ追加: {added_rakuten_data}個")
        
        print(f"\n  🎯 Yahoo/Rakuten結合サマリー:")
        print(f"     - 総追加列数: {total_added_columns}列")
        print(f"     - 最終列数: {len(result_df.columns)}列")
        
        if total_added_columns == 0:
            print(f"  ⚠️ Yahoo/Rakutenデータが追加されませんでした！")
            print(f"  💡 考えられる原因:")
            print(f"     1. ASINの形式が一致していない")
            print(f"     2. データが空またはNaN値")
            print(f"     3. API列の値が期待値と異なる")
        else:
            print(f"  ✅ Yahoo/Rakutenデータの結合に成功しました！")
        
        logger.info(f"Yahoo/Rakutenデータを横展開して結合しました: {len(result_df.columns)}列（{total_added_columns}列追加）")
        
        return result_df

    def merge_source_data(self, base_df, source_data, source_config):
        """
        ベースのデータフレームにソースデータを結合する
        
        Parameters:
        -----------
        base_df : pandas.DataFrame
            ベースのデータフレーム
        source_data : dict
            ファイル名をキー、データフレーム情報を値とする辞書
        source_config : dict
            ソース設定情報
                
        Returns:
        --------
        pandas.DataFrame
            結合後のデータフレーム
        """
        result_df = base_df.copy()
        source_type = source_config.get('type', '不明')
        
        # Yahoo/Rakutenのデータ向け特別処理
        if source_type.lower() == 'yahoo_rakuten':
            # Yahoo/Rakutenデータの横展開処理
            return self._merge_yahoo_rakuten_data(result_df, source_data)
        
        # プレフィックスをサイト名に基づいて設定
        if source_type.lower() == 'netsea':
            prefix = 'ネッシー_'
        elif source_type.lower() == 'sudeli':
            prefix = 'スーデリ_'
        else:
            # その他のソースタイプの場合はそのまま使用
            prefix = source_config.get('prefix', '')
        
        try:
            # ベースDFにJAN列が存在するか確認
            if 'JAN' not in result_df.columns:
                logger.warning("ベースデータにJAN列がありません。結合できない可能性があります。")
                print("⚠️ ベースデータにJAN列がありません。結合できない可能性があります。")
                
                # JAN列がない場合、元コード列からJANを取得してみる
                if '元コード' in result_df.columns:
                    # コードタイプがEANであれば元コードをJANとして使用
                    if 'コードタイプ' in result_df.columns:
                        result_df['JAN'] = result_df.apply(
                            lambda row: row['元コード'] if row['コードタイプ'] == 'EAN' else None, 
                            axis=1
                        )
                        print("📊 元コードとコードタイプからJAN列を作成しました")
            
            # JAN列を文字列に変換し、浮動小数点の末尾を削除
            if 'JAN' in result_df.columns:
                result_df['JAN'] = result_df['JAN'].astype(str).str.replace('.0$', '', regex=True)
            
            # 統合するソースファイルごとに個別に処理
            for file, data in source_data.items():
                source_df = data['df']
                key_column = data['key_column']
                
                # キー列の値を文字列に変換し、浮動小数点の末尾を削除
                source_df[key_column] = source_df[key_column].astype(str).str.replace('.0$', '', regex=True)
                
                # JANコードをキーに結合
                logger.info(f"'{key_column}'列を'JAN'として結合: {file}")
                print(f"📊 {source_type}データから'{key_column}'列を'JAN'として結合: {file}")
                
                # 結合前にマッチするJANコードの数を確認
                if 'JAN' in result_df.columns:
                    # ベースデータのJANリスト
                    base_jans = set(result_df['JAN'].dropna().unique())
                    # ソースデータのJANリスト
                    source_jans = set(source_df[key_column].dropna().unique())
                    # 共通するJANの数
                    common_jans = base_jans.intersection(source_jans)
                    
                    # 重複するJANをチェック
                    duplicate_jans = source_df[source_df[key_column].duplicated(keep=False)][key_column].unique()
                    if len(duplicate_jans) > 0:
                        duplicate_count = len(duplicate_jans)
                        example_duplicates = list(duplicate_jans)[:3]  # 最大3つまで表示
                        print(f"ℹ️ {file}内に{duplicate_count}件の重複JANを検出: {', '.join(example_duplicates)}など")
                        print(f"ℹ️ 重複JANは各JANの最初のデータのみを使用します")
                    
                    # 重要な修正：各JANの最初のエントリのみを保持する
                    # drop_duplicates()メソッドのkeep='first'引数で最初の行のみを残す
                    source_df_unique = source_df.drop_duplicates(subset=[key_column], keep='first')
                    
                    # 重複削除後の結果を表示
                    removed_count = len(source_df) - len(source_df_unique)
                    if removed_count > 0:
                        print(f"ℹ️ 重複を除去: {len(source_df)}行 → {len(source_df_unique)}行 ({removed_count}行削除)")
                    
                    # マッチするJANの例を表示（最大5つ）
                    if common_jans:
                        example_jans = list(common_jans)[:5]
                        print(f"ℹ️ マッチするJANの例: {', '.join(example_jans)}")
                        
                        # マッチするJANがある場合のみ処理を続行
                        # 列名にプレフィックスを追加
                        source_df_renamed = source_df_unique.copy()  # 重複除去済みのデータフレームを使用
                        rename_dict = {}
                        
                        # キー列以外の列名にプレフィックスを追加
                        for col in source_df_unique.columns:
                            if col != key_column:
                                rename_dict[col] = f"{prefix}{col}"
                        
                        source_df_renamed = source_df_renamed.rename(columns=rename_dict)
                        
                        # 結合前の列数とデータ数を記録
                        pre_merge_columns = len(result_df.columns)
                        
                        # マッチしたJANコードを持つ行のみにフィルタリング
                        filtered_source_df = source_df_renamed[source_df_renamed[key_column].isin(common_jans)]
                        
                        if not filtered_source_df.empty:
                            # 結合実行
                            result_df = pd.merge(
                                result_df,
                                filtered_source_df,
                                left_on='JAN',
                                right_on=key_column,
                                how='left',
                                suffixes=('', f'_{file}')  # 重複列の処理
                            )
                            
                            # 結合結果のチェック
                            post_merge_columns = len(result_df.columns)
                            added_columns = post_merge_columns - pre_merge_columns
                            
                            # 重複キー列を削除
                            if key_column != 'JAN' and key_column in result_df.columns:
                                result_df = result_df.drop(columns=[key_column])
                            
                            # 実際にマッチしたデータの確認
                            match_count = 0
                            if added_columns > 0:
                                # 追加された最初の列を見つける
                                for col in result_df.columns[-added_columns:]:
                                    if col in result_df.columns:
                                        match_count = result_df[col].notna().sum()
                                        break
                            
                            print(f"✅ 結合完了: {len(common_jans)}件のJANがマッチ、{match_count}行のデータに情報追加、{added_columns}列追加")
                            logger.info(f"{file}のデータを結合しました: マッチJAN {len(common_jans)}件、マッチ行 {match_count}件、列数 {added_columns}列追加")
                        else:
                            print(f"⚠️ マッチするJANコードがフィルタリング後に残りませんでした。結合をスキップします。")
                    else:
                        print(f"⚠️ マッチするJANコードがありません。結合をスキップします。")
                else:
                    print(f"⚠️ 結合をスキップ: ベースデータにJAN列がありません")
                    logger.warning(f"結合をスキップ: ベースデータにJAN列がありません")
                
            print(f"✅ {source_type}データの結合完了: 現在 {len(result_df.columns)}列")
            return result_df
            
        except Exception as e:
            logger.error(f"ソースデータ結合エラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return base_df

    def merge_data(self, sp_df, keepa_df):
        """
        データの結合
        
        Keepaデータを基準にして、SP-APIデータをASINキーで結合します。
        """
        try:
            # 結合前の情報を表示
            logger.info(f"Keepaデータ: {len(keepa_df)}件, SP-APIデータ: {len(sp_df)}件")
            print(f"📊 結合前 - Keepaデータ: {len(keepa_df)}件, SP-APIデータ: {len(sp_df)}件")
            
            # 修正: SP-APIデータの型を事前に確認して修正
            # 特に「自己発送最安値のポイント」列の処理
            if '自己発送最安値のポイント' in sp_df.columns:
                # 文字列の'False'と'True'を数値に変換してから処理
                sp_df['自己発送最安値のポイント'] = sp_df['自己発送最安値のポイント'].replace(['False', 'True'], [0, 1])
                # NaN値やNone値を一時的な数値に置き換え
                sp_df['自己発送最安値のポイント'] = sp_df['自己発送最安値のポイント'].fillna(-999)
                # 数値型に明示的に変換
                sp_df['自己発送最安値のポイント'] = pd.to_numeric(sp_df['自己発送最安値のポイント'], errors='coerce')
            
            # Keepaデータをベースにして、ASINをキーに結合（左結合）
            merged_df = pd.merge(
                keepa_df,     # Keepaデータを基準にする
                sp_df,
                on='ASIN',
                how='left',  # Keepaデータを基準に左結合
                suffixes=('', '_sp')  # Keepaの列名を優先
            )
            
            # 重複するJAN列の処理（Keepaデータを優先）
            if 'JAN_sp' in merged_df.columns:
                # JAN列が存在する場合は、Keepaデータの値を優先、ない場合はSP-APIデータを使用
                merged_df['JAN'] = merged_df['JAN'].combine_first(merged_df['JAN_sp'])
                # 重複列を削除
                merged_df = merged_df.drop(columns=['JAN_sp'])
                logger.info("JAN列を統合しました (Keepa優先)")
            
            # 修正: 結合後に「自己発送最安値のポイント」列のデータ型とNaN処理を修正
            if '自己発送最安値のポイント' in merged_df.columns:
                # 文字列の'False'と'True'をまず数値に変換
                merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace(['False', 'True'], [0, 1])
                # ブール値を数値に変換
                merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace([False, True], [0, 1])
                # -999（元々のNaN）を再びNoneに戻す
                merged_df['自己発送最安値のポイント'] = merged_df['自己発送最安値のポイント'].replace(-999, None)
                # 明示的に数値型に変換して型の一貫性を確保
                merged_df['自己発送最安値のポイント'] = pd.to_numeric(merged_df['自己発送最安値のポイント'], errors='coerce')
            
            # JAN列の統計を表示
            if 'JAN' in merged_df.columns:
                jan_count = merged_df['JAN'].notna().sum()
                total_rows = len(merged_df)
                logger.info(f"JAN列あり: {jan_count}/{total_rows}件 ({jan_count/total_rows*100:.1f}%)")
                print(f"ℹ️ JAN列あり: {jan_count}/{total_rows}件 ({jan_count/total_rows*100:.1f}%)")
            
            logger.info(f"データを結合しました: {len(merged_df)}件")
            return merged_df
            
        except Exception as e:
            logger.error(f"データ結合エラー: {str(e)}")
            raise

    def rearrange_columns(self, df):
        """
        カラムの並び替え
        
        指定された列順序のカラムを先頭に配置し、それ以外のカラムは末尾に保持します
        
        Parameters:
        -----------
        df : pandas.DataFrame
            並び替え対象のデータフレーム
            
        Returns:
        --------
        pandas.DataFrame
            列が並び替えられたデータフレーム
        """
        try:
            # 望ましい列順を定義
            column_order = [
                # 基本情報1
                'ASIN', 'JAN', '商品名', 'カテゴリーID', 'メーカー型番', 'レビュー有無', 
                'メーカー名', 'ブランド名', '総出品者数', 'セット数', '商品追跡日', 
                '商品発売日', '追跡開始からの経過日数', 'アダルト商品対象',
    
                # 基本情報2
                '参考価格', 'パッケージ最長辺', 'パッケージ中辺', 'パッケージ最短辺', 
                'パッケージ重量', '現在ランキング', '30日間平均ランキング', 
                '90日間平均ランキング', '180日間平均ランキング', 'amazonURL', 
                'KeepaURL', 'バリエーションASIN',
    
                # 価格情報
                'Amazon価格', 'カート価格', 'カート価格送料', 'カート価格のポイント', 
                'リードタイム（時間）', 'FBA最安値', 'FBA最安値のポイント', 
                '自己発送最安値', '自己発送最安値の送料', '自己発送最安値のポイント', 
                'FBA_販売手数料', 'FBA_配送代行手数料',
    
                # 出品者情報
                'amazon本体有無1', 'amazon本体有無2', 'FBA数', '自己発送数', 
                'FBA最安値出品者数', '自己発送最安値出品者数', 
                'amazon_30日間在庫切れ率', 'amazon_90日間在庫切れ率',
    
                # 販売数情報
                '30日間_総販売数', '30日間_新品販売数', '30日間_中古販売数', 
                '30日間_コレクター販売数', 'Keepa30日間販売数', 
                '90日間_総販売数', '90日間_新品販売数', '90日間_中古販売数', 
                '90日間_コレクター販売数', 'Keepa90日間販売数',
                '180日間_総販売数', '180日間_新品販売数', '180日間_中古販売数', 
                '180日間_コレクター販売数', 'Keepa180日間販売数',
    
                # 価格履歴
                'amazon価格_現在価格', 'amazon価格_最高価格', 'amazon価格_最低価格',
                'amazon価格_30日平均価格', 'amazon価格_90日平均価格', 
                'amazon価格_180日平均価格', '新品価格_現在価格', '新品価格_最高価格',
                '新品価格_最低価格', '新品価格_30日平均価格', '新品価格_90日平均価格',
                '新品価格_180日平均価格',
    
                # その他 ※「その他」のあとに列が追加され、定義されていない列があり、その後にヤフー楽天の列が追加される
                '画像URL', '元コード', 'コードタイプ'
            ]
            
            # 存在する列のみを抽出（エラー防止のため）
            specified_columns = [col for col in column_order if col in df.columns]
            
            # 指定されていない残りの列（追加された列など）を取得
            remaining_columns = [col for col in df.columns if col not in column_order]
            
            # 指定列 + 残りの列の順で新しい列順を作成
            new_column_order = specified_columns + remaining_columns
            
            # 並び替えを実行
            df = df[new_column_order]
            
            # 結果をログに記録
            logger.info(f"カラムを並び替えました: 指定列 {len(specified_columns)}列 + 追加列 {len(remaining_columns)}列")
            print(f"📊 カラム並び替え: 指定列 {len(specified_columns)}列 + 追加列 {len(remaining_columns)}列")
            
            return df
            
        except Exception as e:
            logger.error(f"カラム並び替えエラー: {str(e)}")
            raise

    def save_data(self, df, output_file=None):
        """
        統合データの保存
        
        Parameters:
        -----------
        df : pandas.DataFrame
            保存するデータフレーム
        output_file : str, optional
            出力ファイル名（デフォルトは設定ファイルから）
        """
        try:
            # JANコードを文字列として処理する
            if 'JAN' in df.columns:
                df['JAN'] = df['JAN'].astype(str)
                # 浮動小数点形式の場合は元の形式に修正
                df['JAN'] = df['JAN'].replace(r'\.0$', '', regex=True)
                # 指数表記を修正
                df['JAN'] = df['JAN'].apply(lambda x: f"{float(x):.0f}" if re.match(r'\d+\.\d+e\+\d+', str(x).lower()) else x)
                
            # 出力ファイル名が指定されていない場合は設定ファイルから取得
            if output_file is None:
                output_file = self.config['data_integration']['output']['output_file']
                
            # 相対パスの場合はデータディレクトリを基準にする
            if not os.path.isabs(output_file):
                output_file = os.path.join(self.data_dir, output_file)
                
            # 出力ディレクトリの存在確認
            output_dir = os.path.dirname(output_file)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"統合データを保存しました: {output_file}")
            print(f"✅ {len(df)}件のデータを {output_file} に保存しました")
            
        except Exception as e:
            logger.error(f"データ保存エラー: {str(e)}")
            raise
    
    def process(self):
        """
        統合処理の実行
        
        Keepaデータを基準にして、以下のようにデータを統合します：
        1. Keepaデータを読み込む
        2. SP-APIデータをASINキーでKeepaデータに結合
        3. Yahoo/Rakutenデータを含む追加ソースデータをJANキーで結合
        
        Returns:
        --------
        pandas.DataFrame
            統合後のデータフレーム
        """
        try:
            # 各種パスの確認と表示
            print(f"📂 プロジェクトルートディレクトリ: {self.root_dir}")
            print(f"📂 データディレクトリ: {self.data_dir}")
            print(f"📂 ログディレクトリ: {self.log_dir}")
            
            # 設定ファイルから読み込んだファイル名を表示
            config = self.config['data_integration']['output']
            print(f"\n📄 設定ファイルの情報:")
            print(f"  - SP-API入力ファイル: {config['sp_api_input']}")
            print(f"  - Keepa入力ファイル: {config['keepa_input']}")
            print(f"  - 出力ファイル: {config['output_file']}")
            
            # 追加ソース情報を表示
            sources = self.config['data_integration'].get('sources', [])
            if sources:
                print("\n📄 追加ソース情報:")
                for i, source in enumerate(sources, 1):
                    source_type = source.get('type', '不明')
                    files = source.get('files', [])
                    print(f"  ソース{i} ({source_type}): {', '.join(files)}")
            
            # SP-APIとKeepaデータの読み込み
            sp_df, keepa_df = self.load_data()
            
            # SP-APIとKeepaデータの結合
            merged_df = self.merge_data(sp_df, keepa_df)
            
            # 追加ソースデータの結合
            for source_config in sources:
                source_type = source_config.get('type', '不明')
                print(f"\n📊 ソースデータ読み込み ({source_type})")
                
                # ソースデータの読み込み
                source_data = self.load_source_data(source_config)
                
                if source_data:
                    # ソースデータの結合
                    merged_df = self.merge_source_data(merged_df, source_data, source_config)
                    print(f"✅ {source_type}データの結合完了: 現在 {len(merged_df.columns)}列")
            
            # カラムの並び替え
            merged_df = self.rearrange_columns(merged_df)
            
            # 統合データの保存
            self.save_data(merged_df)
            
            # 修正: 結果の統計情報（JAN列の有無をチェックしてから表示）
            print("\n=== 統合結果の概要 ===")
            print(f"・総データ件数: {len(merged_df)}件")
            if 'JAN' in merged_df.columns:
                jan_count = merged_df['JAN'].notna().sum()
                print(f"・JAN列あり件数: {jan_count}件")
            print(f"・カラム数: {len(merged_df.columns)}列")
            
            output_file = config['output_file']  # 変数を明示的に取得
            print(f"\n✨ 処理完了！ データを {output_file} に保存しました")
            print(f"（Keepaデータを基準に結合しました）")
            
            return merged_df
            
        except Exception as e:
            logger.error(f"実行時エラー: {str(e)}")
            print(f"❌ エラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

# テスト用の実行コード
if __name__ == "__main__":
    # インテグレーターのインスタンス作成
    integrator = DataIntegrator()
    
    # 統合処理の実行
    result_df = integrator.process()
    
    # 成功したかどうかの確認
    if result_df is not None:
        print("✅ データ統合が成功しました！")
    else:
        print("❌ データ統合に失敗しました。")