"""メディアダウンロードモジュール"""
import os
import requests
from pathlib import Path
from typing import Dict, List, Optional, Callable
import logging
from tqdm import tqdm
import time
from queue import Queue
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import Config

logger = logging.getLogger(__name__)


class MediaDownloader:
    """メディアファイルダウンローダー"""
    
    def __init__(self, max_workers: int = 3):
        """
        Args:
            max_workers: 並行ダウンロードの最大スレッド数
        """
        self.config = Config
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Cookieが設定されている場合はリクエストにも付与（非公開アカウントのメディア取得に必要）
        if self.config.TWITTER_COOKIES:
            self.session.headers.update({'Cookie': self.config.TWITTER_COOKIES})
        
        # 並行ダウンロード用のキューとスレッド
        self.download_queue: Queue = Queue()
        self.download_thread: Optional[Thread] = None
        self.is_downloading = False
        self.downloaded_count = 0
        self.total_media = 0
        self.pbar: Optional[tqdm] = None
    
    def download_media(self, tweets: List[Dict]) -> List[Dict]:
        """Tweetに含まれるメディアをダウンロード（同期的）"""
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
    
    def start_parallel_download(self, progress_callback: Optional[Callable] = None):
        """並行ダウンロードを開始（バックグラウンドで実行）"""
        if self.is_downloading:
            logger.warning("並行ダウンロードは既に開始されています")
            return
        
        self.is_downloading = True
        self.downloaded_count = 0
        self.total_media = 0
        self.pbar = tqdm(desc="メディアダウンロード（並行）", unit="件", position=1, leave=True)
        
        def download_worker():
            """ダウンロードワーカースレッド"""
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                
                while self.is_downloading or not self.download_queue.empty():
                    # キューからタスクを取得（タイムアウト付き）
                    try:
                        task = self.download_queue.get(timeout=1.0)
                    except:
                        # 完了したタスクを処理
                        completed_futures = []
                        for future in list(futures.keys()):
                            if future.done():
                                completed_futures.append(future)
                        
                        for future in completed_futures:
                            tweet_id, media = futures.pop(future)
                            try:
                                local_path = future.result()
                                if local_path:
                                    media['local_path'] = str(local_path)
                                    self.downloaded_count += 1
                                else:
                                    media['local_path'] = None
                            except Exception as e:
                                logger.error(f"メディアダウンロードエラー: {e}")
                                media['local_path'] = None
                            
                            self.pbar.update(1)
                            if progress_callback:
                                progress_callback(self.downloaded_count, self.total_media)
                            
                            self.download_queue.task_done()
                            time.sleep(0.3)  # レート制限回避
                        continue
                    
                    tweet_id, media = task
                    
                    # ダウンロードを実行
                    future = executor.submit(self._download_single_media, media, tweet_id)
                    futures[future] = (tweet_id, media)
                    
                    # 完了したタスクを処理
                    completed_futures = []
                    for future in list(futures.keys()):
                        if future.done():
                            completed_futures.append(future)
                    
                    for future in completed_futures:
                        tweet_id, media = futures.pop(future)
                        try:
                            local_path = future.result()
                            if local_path:
                                media['local_path'] = str(local_path)
                                self.downloaded_count += 1
                            else:
                                media['local_path'] = None
                        except Exception as e:
                            logger.error(f"メディアダウンロードエラー: {e}")
                            media['local_path'] = None
                        
                        self.pbar.update(1)
                        if progress_callback:
                            progress_callback(self.downloaded_count, self.total_media)
                        
                        self.download_queue.task_done()
                        time.sleep(0.3)  # レート制限回避
                
                # 残りのタスクを処理
                for future in as_completed(list(futures.keys())):
                    tweet_id, media = futures.pop(future)
                    try:
                        local_path = future.result()
                        if local_path:
                            media['local_path'] = str(local_path)
                            self.downloaded_count += 1
                        else:
                            media['local_path'] = None
                    except Exception as e:
                        logger.error(f"メディアダウンロードエラー: {e}")
                        media['local_path'] = None
                    
                    self.pbar.update(1)
                    if progress_callback:
                        progress_callback(self.downloaded_count, self.total_media)
            
            self.pbar.close()
            logger.info(f"{self.downloaded_count}件のメディアをダウンロードしました（並行）")
        
        self.download_thread = Thread(target=download_worker, daemon=True)
        self.download_thread.start()
        logger.info("並行メディアダウンロードを開始しました")
    
    def add_tweet_for_download(self, tweet: Dict):
        """ダウンロードキューにツイートを追加"""
        if not self.is_downloading:
            logger.warning("並行ダウンロードが開始されていません")
            return
        
        tweet_id = tweet.get('tweet_id')
        media_list = tweet.get('media', [])
        
        for media in media_list:
            self.download_queue.put((tweet_id, media))
            self.total_media += 1
        
        if self.pbar is not None:
            self.pbar.total = self.total_media
            self.pbar.refresh()
    
    def stop_parallel_download(self, wait_for_completion: bool = True):
        """並行ダウンロードを停止
        
        Args:
            wait_for_completion: Trueの場合、進行中のダウンロードが完了するまで待機
        """
        self.is_downloading = False
        
        if self.download_thread:
            if wait_for_completion:
                # キューが空になるまで待機（最大60秒）
                logger.info("進行中のメディアダウンロードの完了を待機しています...")
                timeout = 60
                elapsed = 0
                while not self.download_queue.empty() and elapsed < timeout:
                    time.sleep(1)
                    elapsed += 1
                
                if not self.download_queue.empty():
                    logger.warning(f"一部のメディアダウンロードが完了しませんでした（{len(self.download_queue.queue)}件残り）")
            
            # スレッドの終了を待つ
            self.download_thread.join(timeout=30)  # 最大30秒待機
        
        if self.pbar is not None:
            self.pbar.close()
        
        logger.info(f"並行メディアダウンロードを停止しました（{self.downloaded_count}/{self.total_media}件完了）")
    
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

