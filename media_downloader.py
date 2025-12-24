"""メディアダウンロードモジュール"""
import os
import requests
from pathlib import Path
from typing import Dict, List, Optional
import logging
from tqdm import tqdm
import time

from config import Config

logger = logging.getLogger(__name__)


class MediaDownloader:
    """メディアファイルダウンローダー"""
    
    def __init__(self):
        self.config = Config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Cookieが設定されている場合はリクエストにも付与（非公開アカウントのメディア取得に必要）
        if self.config.TWITTER_COOKIES:
            self.session.headers.update({'Cookie': self.config.TWITTER_COOKIES})
    
    def download_media(self, tweets: List[Dict]) -> List[Dict]:
        """Tweetに含まれるメディアをダウンロード"""
        total_media = sum(len(tweet.get('media', [])) for tweet in tweets)
        
        if total_media == 0:
            logger.info("ダウンロードするメディアがありません")
            return tweets
        
        logger.info(f"{total_media}件のメディアをダウンロードします")
        
        downloaded_count = 0
        with tqdm(total=total_media, desc="メディアダウンロード", unit="件") as pbar:
            for tweet in tweets:
                tweet_id = tweet.get('tweet_id')
                media_list = tweet.get('media', [])
                
                for media in media_list:
                    try:
                        local_path = self._download_single_media(media, tweet_id)
                        if local_path:
                            media['local_path'] = str(local_path)
                            downloaded_count += 1
                        pbar.update(1)
                        time.sleep(0.5)  # レート制限回避
                    except Exception as e:
                        logger.error(f"メディアダウンロードエラー: {e}")
                        media['local_path'] = None
                        pbar.update(1)
        
        logger.info(f"{downloaded_count}件のメディアをダウンロードしました")
        return tweets
    
    def _download_single_media(self, media: Dict, tweet_id: str) -> Optional[Path]:
        """単一のメディアファイルをダウンロード（429時にリトライ）"""
        media_type = media.get('type')
        url = media.get('url')
        media_index = media.get('media_index', 0)
        
        if not url:
            return None
        
        # URLを高解像度版に変換（画像の場合）
        if media_type == 'photo' and '?format=' not in url:
            # 高解像度版を取得
            url = url.replace(':small', ':large').replace(':thumb', ':large')
        
        max_retry = 3
        backoff = 60  # 秒
        
        for attempt in range(1, max_retry + 1):
            try:
                response = self.session.get(url, timeout=30, stream=True)
                
                if response.status_code == 429:
                    # 429は一般的に15分ウィンドウのことが多いので、最低900秒待機に引き上げ
                    wait_for = max(backoff, 900)
                    logger.warning(f"429 Too Many Requests (media): {url} - {wait_for}秒待機してリトライ ({attempt}/{max_retry})")
                    time.sleep(wait_for)
                    backoff = min(wait_for * 2, 3600)  # 最大1時間
                    continue
                
                response.raise_for_status()
                
                # ファイル拡張子を決定
                ext = self._get_extension(url, media_type, response.headers.get('content-type'))
                
                # 保存先パスを決定
                if media_type == 'photo':
                    save_dir = self.config.IMAGES_DIR
                elif media_type in ['video', 'animated_gif']:
                    save_dir = self.config.VIDEOS_DIR
                else:
                    save_dir = self.config.OUTPUT_DIR
                
                filename = f"{tweet_id}_{media_index}{ext}"
                save_path = save_dir / filename
                
                # ファイルを保存
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                # ファイルサイズを記録
                file_size = save_path.stat().st_size
                media['file_size'] = file_size
                
                return save_path
                
            except Exception as e:
                logger.error(f"メディアダウンロード失敗 ({url}): {e}")
                if attempt == max_retry:
                    return None
                # 429以外でも一時的エラーの可能性があるためリトライ
                logger.info(f"再試行します ({attempt}/{max_retry})")
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
    
    def _get_extension(self, url: str, media_type: str, content_type: str = None) -> str:
        """ファイル拡張子を取得"""
        # URLから拡張子を取得
        if '.' in url:
            ext = '.' + url.split('.')[-1].split('?')[0].split(':')[0]
            if ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.mov']:
                return ext
        
        # Content-Typeから拡張子を決定
        if content_type:
            if 'image/jpeg' in content_type:
                return '.jpg'
            elif 'image/png' in content_type:
                return '.png'
            elif 'image/gif' in content_type:
                return '.gif'
            elif 'image/webp' in content_type:
                return '.webp'
            elif 'video/mp4' in content_type:
                return '.mp4'
        
        # デフォルト
        if media_type == 'photo':
            return '.jpg'
        elif media_type in ['video', 'animated_gif']:
            return '.mp4'
        else:
            return '.bin'

