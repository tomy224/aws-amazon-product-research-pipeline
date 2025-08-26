# modules/utils/logger_utils.py
import os
import logging
from datetime import datetime

def setup_logging(log_dir, name_prefix, console_level=logging.INFO, file_level=logging.DEBUG):
    """
    ログ環境をセットアップする
    
    Args:
        log_dir (str): ログファイル保存ディレクトリ
        name_prefix (str): ログファイル名の接頭辞
        console_level (int): コンソール出力のログレベル
        file_level (int): ファイル出力のログレベル
        
    Returns:
        str: ログファイルのパス
    """
    # ディレクトリが存在しない場合は作成
    os.makedirs(log_dir, exist_ok=True)
    
    # すでに存在するハンドラを削除（重複を防ぐため）
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # ログファイルパスの設定
    log_filename = f'{name_prefix}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_file = os.path.join(log_dir, log_filename)
    
    # ルートロガーの設定
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # ルートロガーは最低レベルに設定
    
    # ファイルハンドラの設定
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(file_level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # コンソールハンドラの設定
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # ログファイルの場所を明示的に表示
    print(f"ログファイル出力先: {log_file}")
    logging.info(f"ログ機能の初期化が完了しました: {log_file}")
    
    return log_file

def get_logger(name=None):
    """
    名前付きロガーを取得する
    
    Args:
        name (str, optional): ロガー名
        
    Returns:
        Logger: 設定済みのロガーインスタンス
    """
    return logging.getLogger(name)

def log_function_call(func):
    """
    関数呼び出しをログに記録するデコレータ
    
    Args:
        func: デコレート対象の関数
        
    Returns:
        function: ラップされた関数
    """
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.debug(f"関数 {func.__name__} が呼び出されました")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"関数 {func.__name__} が正常終了しました")
            return result
        except Exception as e:
            logger.error(f"関数 {func.__name__} でエラーが発生: {str(e)}", exc_info=True)
            raise
    return wrapper
    