import pandas as pd

def has_unprocessed_rows(csv_path: str, status_column: str = "処理状況", unprocessed_value: str = "未処理") -> bool:
    """
    CSVファイル内に未処理データが存在するかを判定する。

    Args:
        csv_path (str): チェック対象のCSVファイルパス
        status_column (str): 処理状態を示す列名（デフォルト: "処理状況"）
        unprocessed_value (str): 未処理を表す値（デフォルト: "未処理"）

    Returns:
        bool: 未処理の行が存在する場合はTrue、なければFalse
    """
    try:
        df = pd.read_csv(csv_path)
        return (df[status_column] == unprocessed_value).any()
    except Exception as e:
        print(f"CSVチェック中にエラーが発生しました: {e}")
        return False
