# modules/utils/file_utils.py
import os
import csv
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Union, Optional

from .logger_utils import get_logger, log_function_call

# ロガーの取得
logger = get_logger(__name__)

@log_function_call
def find_project_root():
    """
    プロジェクトのルートディレクトリを検出する
    
    Returns:
        str: プロジェクトルートディレクトリの絶対パス
    """
    # 現在のファイルの絶対パスを取得
    current_dir = os.path.abspath(os.getcwd())
    
    # 親ディレクトリを探索
    path = Path(current_dir)
    while True:
        # .gitディレクトリがあればそれをルートとみなす
        if (path / '.git').exists():
            return str(path)
        
        # プロジェクトのルートを示す他のファイル/ディレクトリの存在チェック
        if (path / 'setup.py').exists() or (path / 'README.md').exists():
            return str(path)
        
        # これ以上上の階層がない場合は現在のディレクトリを返す
        if path.parent == path:
            return str(path)
        
        # 親ディレクトリへ
        path = path.parent

@log_function_call
def load_yaml_config(config_path: Optional[str] = None) -> Dict:
    """
    YAMLファイルから設定を読み込む
    
    Args:
        config_path (str, optional): 設定ファイルのパス
            指定がない場合はプロジェクトルートのconfig/settings.yamlを使用
    
    Returns:
        dict: 設定データ
    
    Raises:
        FileNotFoundError: 設定ファイルが見つからない場合
        yaml.YAMLError: YAMLの解析に失敗した場合
    """
    if config_path is None:
        root_dir = find_project_root()
        config_path = os.path.join(root_dir, 'config', 'settings.yaml')
    
    logger.info(f"設定ファイルを読み込み: {config_path}")
    
    if not os.path.exists(config_path):
        error_msg = f"設定ファイルが見つかりません: {config_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            logger.debug("設定ファイルの読み込みに成功しました")
            return config
    except yaml.YAMLError as e:
        error_msg = f"YAML解析エラー: {str(e)}"
        logger.error(error_msg)
        raise

@log_function_call
def save_to_csv(data: List[Dict], output_path: str, append: bool = False) -> None:
    """
    データをCSVファイルに保存する
    
    Args:
        data (List[Dict]): 保存するデータ（辞書のリスト）
        output_path (str): 出力ファイルパス
        append (bool): 追記モードで保存するかどうか（デフォルト: False）
    
    Raises:
        ValueError: データが空の場合
    """
    if not data:
        logger.warning("保存するデータが空です")
        return
    
    # 出力ディレクトリの確認
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"出力ディレクトリを作成しました: {output_dir}")
    
    # 保存モード
    mode = 'a' if append else 'w'
    header = not append or not os.path.exists(output_path)
    
    # データフレームに変換してCSVに保存
    df = pd.DataFrame(data)
    df.to_csv(output_path, mode=mode, index=False, encoding='utf-8-sig', header=header)
    
    action = "追記" if append else "保存"
    logger.info(f"{len(data)}件のデータを{action}しました: {output_path}")

@log_function_call
def load_csv(file_path: str, encoding: str = 'utf-8-sig') -> pd.DataFrame:
    """
    CSVファイルを読み込む
    
    Args:
        file_path (str): CSVファイルのパス
        encoding (str): ファイルエンコーディング（デフォルト: utf-8-sig）
    
    Returns:
        pd.DataFrame: 読み込んだデータフレーム
    
    Raises:
        FileNotFoundError: ファイルが見つからない場合
    """
    if not os.path.exists(file_path):
        error_msg = f"ファイルが見つかりません: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        logger.info(f"{file_path} から {len(df)}件のデータを読み込みました")
        return df
    except Exception as e:
        error_msg = f"CSV読み込みエラー: {str(e)}"
        logger.error(error_msg)
        raise

@log_function_call
def load_json(file_path: str) -> Dict:
    """
    JSONファイルを読み込む
    
    Args:
        file_path (str): JSONファイルのパス
    
    Returns:
        dict: 読み込んだJSONデータ
    
    Raises:
        FileNotFoundError: ファイルが見つからない場合
        json.JSONDecodeError: JSONの解析に失敗した場合
    """
    if not os.path.exists(file_path):
        error_msg = f"ファイルが見つかりません: {file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"{file_path} からJSONデータを読み込みました")
        return data
    except json.JSONDecodeError as e:
        error_msg = f"JSON解析エラー: {str(e)}"
        logger.error(error_msg)
        raise

@log_function_call
def save_json(data: Dict, file_path: str, indent: int = 4) -> None:
    """
    データをJSONファイルに保存する
    
    Args:
        data (dict): 保存するデータ
        file_path (str): 出力ファイルパス
        indent (int): インデントのスペース数（デフォルト: 4）
    """
    # 出力ディレクトリの確認
    output_dir = os.path.dirname(file_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    
    logger.info(f"JSONデータを保存しました: {file_path}")


# modules/utils/file_utils.py に追加

@log_function_call
def load_jan_codes(input_file: str) -> list:
    """
    CSVファイルからJANコードを読み込む
    
    Args:
        input_file (str): 入力CSVファイルのパス
            
    Returns:
        list: JANコードのリスト
    
    Raises:
        FileNotFoundError: ファイルが見つからない場合
    """
    jan_codes = []
    
    if not os.path.exists(input_file):
        error_msg = f"入力ファイルが見つかりません: {input_file}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        # CSVファイルの読み込み
        with open(input_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader, None)  # ヘッダー行を読み飛ばす
            
            # JANコード列を探す
            jan_col = 0
            if header:
                for i, col in enumerate(header):
                    if 'JAN' in col.upper() or 'コード' in col or 'CODE' in col.upper():
                        jan_col = i
                        break
            
            # 各行からJANコードを取得
            for row in reader:
                if row and len(row) > jan_col and row[jan_col].strip():
                    jan_code = row[jan_col].strip()
                    # 数字のみで構成されているか確認
                    if jan_code.isdigit():
                        jan_codes.append(jan_code)
        
        logger.info(f"{len(jan_codes)}件のJANコードを読み込みました: {input_file}")
        return jan_codes
        
    except Exception as e:
        error_msg = f"JANコード読み込みエラー: {str(e)}"
        logger.error(error_msg)
        raise