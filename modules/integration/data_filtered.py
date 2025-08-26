#!/usr/bin/env python
# coding: utf-8

"""
データフィルタリングスクリプト

calculated_data.csvファイルを読み込み、設定に基づいてフィルタリングし、
指定された条件で並べ替えた結果をfiltered_data.csvとして保存します。

フィルタリング条件：
- 列「手数料・利益_利益額」が指定金額以上
- 列「手数料・利益_利益率」が指定範囲内
- 値がない場合は条件に一致したものとして扱う

並び替え条件：
- 列「現在ランキング」の若い順
"""

import os
import sys
import argparse
import pandas as pd
import traceback
from datetime import datetime

# プロジェクトルートをPythonパスに追加
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.insert(0, project_root)

# 自作モジュールのインポート
from modules.utils.file_utils import find_project_root, load_yaml_config, load_csv
from modules.utils.logger_utils import setup_logging, get_logger

# ロガーの取得
logger = get_logger(__name__)

def filter_and_sort_data(config, input_file, output_file):
    """
    データのフィルタリングと並び替えを行う

    Args:
        config (dict): 設定情報
        input_file (str): 入力ファイルパス
        output_file (str): 出力ファイルパス
    
    Returns:
        pandas.DataFrame: 処理後のデータフレーム
    """
    try:
        # CSVファイルの読み込み
        print(f"📊 データを読み込み中: {input_file}")
        df = load_csv(input_file)
        
        # 元のデータサイズを記録
        original_size = len(df)
        print(f"📈 元のデータサイズ: {original_size}件")
        
        # フィルタリング条件を取得
        filter_conditions = config.get('filter_conditions', {})
        profit_min = filter_conditions.get('profit_min', 100)
        profit_rate_min = filter_conditions.get('profit_rate_min', 0)
        profit_rate_max = filter_conditions.get('profit_rate_max', 40)
        
        print(f"🔍 フィルタリング条件:")
        print(f"  - 利益額 {profit_min}円以上")
        print(f"  - 利益率 {profit_rate_min}%以上{profit_rate_max}%以下")
        
        # 利益額でフィルタリング（NULL値は条件を満たさないとみなす）
        profit_mask = (~df['手数料・利益_利益額'].isna()) & (df['手数料・利益_利益額'] >= profit_min)
        
        # 利益率でフィルタリング（NULL値は条件を満たすとみなす）
        # 利益率カラムの値に%が含まれる場合は数値に変換
        if '手数料・利益_利益率' in df.columns:
            # %記号があれば削除して数値に変換
            rate_column = df['手数料・利益_利益率'].copy()
            if rate_column.dtype == 'object':  # 文字列型の場合
                # 非NaN値のみ処理
                mask = ~rate_column.isna()
                rate_column.loc[mask] = rate_column.loc[mask].str.replace('%', '').astype(float)
                
            # フィルタリング条件を構築（NULL値は条件を満たさないとみなす）
            rate_mask = (~rate_column.isna()) & ((rate_column >= profit_rate_min) & (rate_column <= profit_rate_max))
        else:
            # カラムが存在しない場合は、すべての行がフィルター条件を満たすとみなす
            rate_mask = pd.Series(True, index=df.index)
        
        # 両方の条件を組み合わせてフィルタリング
        filtered_df = df[profit_mask & rate_mask].copy()
        
        # フィルタリング後のサイズ
        filtered_size = len(filtered_df)
        print(f"📊 フィルタリング後のデータサイズ: {filtered_size}件 (除外: {original_size - filtered_size}件)")
        
        # ランキングで並び替え
        # NULL値は最後に来るように設定（大きな値として扱う）
        print("🔢 「現在ランキング」で昇順に並び替え中...")
        if '現在ランキング' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('現在ランキング', 
                                               ascending=True, 
                                               na_position='last').reset_index(drop=True)
            
        # ✅ JAN列を ="" 形式に変換し、小数点.0を除く
        if 'JAN' in filtered_df.columns:
            filtered_df['JAN'] = filtered_df['JAN'].apply(lambda x: f'="{str(x).split(".")[0]}"')

        # ✅ カテゴリーID列を ="" 形式に変換し、小数点.0を除く
        if 'カテゴリーID' in filtered_df.columns:
            filtered_df['カテゴリーID'] = filtered_df['カテゴリーID'].apply(lambda x: f'="{str(x).split(".")[0]}"')

        # ✅ メーカー型番列を ="" 形式に変換（NaN対策つき）
        if 'メーカー型番' in filtered_df.columns:
            filtered_df['メーカー型番'] = filtered_df['メーカー型番'].apply(
                lambda x: f'="{str(x).split(".")[0]}"' if not pd.isna(x) else ''
            )


        
        # CSVファイルとして保存
        print(f"💾 データを保存中: {output_file}")
        # pandas のto_csvを直接使用
        filtered_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ 処理が完了しました。{filtered_size}件のデータを保存しました。")
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"データ処理中にエラーが発生: {str(e)}")
        print(f"❌ エラーが発生しました: {str(e)}")
        traceback.print_exc()
        return None

def main():
    """メイン実行関数"""
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description='データフィルタリングスクリプト')
    parser.add_argument('--input', '-i', help='入力CSVファイルパス')
    parser.add_argument('--output', '-o', help='出力CSVファイルパス')
    parser.add_argument('--config', '-c', help='設定ファイルパス')
    args = parser.parse_args()
    
    try:
        # プロジェクトルートディレクトリを取得
        root_dir = find_project_root()
        
        # ログディレクトリの設定
        log_dir = os.path.join(root_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # ログ設定
        log_file = setup_logging(log_dir, 'data_filter')
        logger.info("データフィルタリング処理を開始します")
        
        # 設定ファイルの読み込み
        config_path = args.config if args.config else os.path.join(root_dir, 'config', 'settings.yaml')
        config = load_yaml_config(config_path)
        
        # 入出力ファイルパスの設定
        input_file = args.input if args.input else os.path.join(root_dir, 'data', 'calculated_data.csv')
        output_file = args.output if args.output else os.path.join(root_dir, 'data', 'filtered_data.csv')
        
        # フィルタリングと並び替えの実行
        print("🚀 データフィルタリング処理を開始します...")
        filter_and_sort_data(config, input_file, output_file)
        
        return 0
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}", exc_info=True)
        print(f"❌ エラーが発生しました: {str(e)}")
        traceback.print_exc()
        return 1

# スクリプトとして実行された場合のエントリーポイント
if __name__ == "__main__":
    sys.exit(main())