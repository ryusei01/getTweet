"""データ保存モジュール"""
import json
import csv
from pathlib import Path
from typing import List, Dict
import logging
from datetime import datetime

from config import Config

logger = logging.getLogger(__name__)


class DataSaver:
    """データ保存クラス"""
    
    def __init__(self):
        self.config = Config
    
    def save_tweets_json(self, tweets: List[Dict], filename: str = "tweets.json"):
        """TweetデータをJSON形式で保存"""
        output_path = self.config.RUN_DIR / filename
        
        # メタデータを追加
        data = {
            'metadata': {
                'total_tweets': len(tweets),
                'exported_at': datetime.now().isoformat(),
                'version': '1.0'
            },
            'tweets': tweets
        }
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"JSONファイルを保存しました: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"JSON保存エラー: {e}")
            raise
    
    def save_tweets_csv(self, tweets: List[Dict], filename: str = "tweets_summary.csv"):
        """TweetデータをCSV形式で保存（サマリー）"""
        output_path = self.config.RUN_DIR / filename
        
        if not tweets:
            logger.warning("保存するTweetがありません")
            return None
        
        try:
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'tweet_id', 'created_at', 'text', 'author_username',
                    'like_count', 'retweet_count', 'reply_count', 'quote_count',
                    'media_count', 'url'
                ])
                
                writer.writeheader()
                
                for tweet in tweets:
                    metrics = tweet.get('public_metrics', {})
                    media_count = len(tweet.get('media', []))
                    
                    row = {
                        'tweet_id': tweet.get('tweet_id', ''),
                        'created_at': tweet.get('created_at', ''),
                        'text': tweet.get('text', '')[:200],  # 長いテキストは切り詰め
                        'author_username': tweet.get('author_username', ''),
                        'like_count': metrics.get('like_count', 0),
                        'retweet_count': metrics.get('retweet_count', 0),
                        'reply_count': metrics.get('reply_count', 0),
                        'quote_count': metrics.get('quote_count', 0),
                        'media_count': media_count,
                        'url': tweet.get('url', '')
                    }
                    writer.writerow(row)
            
            logger.info(f"CSVファイルを保存しました: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"CSV保存エラー: {e}")
            raise

