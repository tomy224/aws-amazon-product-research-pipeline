import time
import logging
from modules.utils.logger_utils import get_logger

# ロガーの取得
logger = get_logger(__name__)

class KeepaTimer:
    """Keepa API実行のタイミングを管理するクラス"""
    
    def __init__(self, interval=3600):  # デフォルト1時間（3600秒）
        """
        初期化
        
        Args:
            interval (int): Keepa API実行間隔（秒）
        """
        self.interval = interval
        self.last_keepa_run = time.time()
        logger.info(f"KeepaTimerを初期化しました: 間隔={interval}秒")
    
    def should_run_keepa(self):
        """
        Keepa APIを実行すべき時間になったかどうかを判断
        
        Returns:
            bool: 実行すべき場合はTrue
        """
        current_time = time.time()
        elapsed = current_time - self.last_keepa_run
        
        if elapsed >= self.interval:
            # タイマーをリセット
            self.last_keepa_run = current_time
            logger.info(f"Keepa API実行条件を満たしました (経過時間: {elapsed:.1f}秒)")
            return True
        
        return False
    
    def time_until_next_run(self):
        """
        次のKeepa API実行までの残り時間（秒）
        
        Returns:
            float: 残り時間（秒）
        """
        current_time = time.time()
        elapsed = current_time - self.last_keepa_run
        return max(0, self.interval - elapsed)

    def reset(self):
        """タイマーをリセット"""
        self.last_keepa_run = time.time()
        logger.info("KeepaTimerをリセットしました")