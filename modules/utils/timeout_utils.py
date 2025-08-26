# modules/utils/timeout_utils.py
import time
import logging

logger = logging.getLogger(__name__)

class TimeoutMonitor:
    """Lambda実行のタイムアウト監視クラス"""
    
    def __init__(self, start_time=None, max_execution_seconds=840):
        """
        タイムアウト監視の初期化
        
        Args:
            start_time (float, optional): 開始時刻（Noneの場合は現在時刻）
            max_execution_seconds (int): 最大実行時間（秒）
                デフォルトは14分（840秒）- Lambda制限15分より余裕を持たせる
        """
        self.start_time = start_time or time.time()
        self.max_execution_seconds = max_execution_seconds
    
    def check_timeout(self, threshold_percentage=0.9):
        """
        タイムアウト判定を行う
        
        Args:
            threshold_percentage (float): タイムアウトと判断する閾値（0.0〜1.0）
                デフォルトは0.9（最大実行時間の90%に達したらタイムアウトと判断）
                
        Returns:
            bool: タイムアウトならTrue、それ以外はFalse
        """
        elapsed_time = time.time() - self.start_time
        threshold_time = self.max_execution_seconds * threshold_percentage
        
        if elapsed_time >= threshold_time:
            logger.warning(f"タイムアウト間近: {elapsed_time:.1f}秒 / {threshold_time:.1f}秒 ({threshold_percentage*100:.0f}%)")
            return True
        
        return False
    
    def get_remaining_time(self):
        """
        残り実行時間を取得
        
        Returns:
            float: 残り実行時間（秒）
        """
        elapsed_time = time.time() - self.start_time
        remaining_time = max(0, self.max_execution_seconds - elapsed_time)
        return remaining_time