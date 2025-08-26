# modules/utils/s3_utils.py
import boto3
import logging
import os

logger = logging.getLogger(__name__)

class S3Utils:
    """S3操作のためのユーティリティクラス"""
    
    def __init__(self, bucket_name=None):
        """
        S3ユーティリティの初期化
        
        Args:
            bucket_name (str, optional): デフォルトのS3バケット名
        """
        self.s3_client = boto3.client('s3')
        self.default_bucket = bucket_name
    
    def download_file(self, key, local_path, bucket=None):
        """
        S3からファイルをダウンロード
        
        Args:
            key (str): S3オブジェクトキー
            local_path (str): ローカルの保存先パス
            bucket (str, optional): S3バケット名（指定がなければデフォルト値を使用）
            
        Returns:
            bool: 成功したらTrue、失敗したらFalse
        """
        bucket = bucket or self.default_bucket
        if not bucket:
            logger.error("バケット名が指定されていません")
            return False
            
        try:
            # 保存先ディレクトリが存在しない場合は作成
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # ファイルをダウンロード
            self.s3_client.download_file(bucket, key, local_path)
            logger.info(f"S3からファイルをダウンロードしました: s3://{bucket}/{key} -> {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"S3ダウンロードエラー: {str(e)}")
            return False
    
    def upload_file(self, local_path, key, bucket=None):
        """
        S3にファイルをアップロード
        
        Args:
            local_path (str): アップロードするローカルファイルのパス
            key (str): S3での保存先キー
            bucket (str, optional): S3バケット名（指定がなければデフォルト値を使用）
            
        Returns:
            bool: 成功したらTrue、失敗したらFalse
        """
        bucket = bucket or self.default_bucket
        if not bucket:
            logger.error("バケット名が指定されていません")
            return False
            
        try:
            # ファイルが存在するか確認
            if not os.path.exists(local_path):
                logger.error(f"アップロード対象ファイルが見つかりません: {local_path}")
                return False
                
            # ファイルをアップロード
            self.s3_client.upload_file(local_path, bucket, key)
            logger.info(f"ファイルをS3にアップロードしました: {local_path} -> s3://{bucket}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"S3アップロードエラー: {str(e)}")
            return False
    
    def list_objects(self, prefix, bucket=None):
        """
        S3バケット内のオブジェクト一覧を取得
        
        Args:
            prefix (str): 検索プレフィックス
            bucket (str, optional): S3バケット名
            
        Returns:
            list: オブジェクトキーのリスト
        """
        bucket = bucket or self.default_bucket
        if not bucket:
            logger.error("バケット名が指定されていません")
            return []
            
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                return []
                
            return [item['Key'] for item in response['Contents']]
            
        except Exception as e:
            logger.error(f"S3オブジェクト一覧取得エラー: {str(e)}")
            return []