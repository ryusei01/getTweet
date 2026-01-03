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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import subprocess
import tempfile

from config import Config

logger = logging.getLogger(__name__)

# region agent log
_DEBUG_LOG_PATH = r"h:\document\program\project\getTweet\.cursor\debug.log"
from pathlib import Path as _AgentPath
_LOCAL_DEBUG_NDJSON = str((_AgentPath(__file__).resolve().parent / "debug.ndjson"))
_LOCAL_CURSOR_DEBUG = str((_AgentPath(__file__).resolve().parent / ".cursor" / "debug.log"))


def _agent_log(hypothesisId: str, location: str, message: str, data: dict = None, runId: str = "pre") -> None:
    try:
        import json as _json
        import time as _time

        payload = {
            "sessionId": "debug-session",
            "runId": runId,
            "hypothesisId": hypothesisId,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(_time.time() * 1000),
        }
        line = _json.dumps(payload, ensure_ascii=False) + "\n"
        try:
            _AgentPath(_DEBUG_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
            with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            with open(_LOCAL_DEBUG_NDJSON, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
        try:
            _AgentPath(_LOCAL_CURSOR_DEBUG).parent.mkdir(parents=True, exist_ok=True)
            with open(_LOCAL_CURSOR_DEBUG, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
    except Exception:
        pass


def _safe_url_tag(url: str) -> str:
    try:
        return (url or "")[:160]
    except Exception:
        return ""

# endregion


class MediaDownloader:
    """メディアファイルダウンローダー"""
    
    def __init__(self, max_workers: int = 3):
        """
        Args:
            max_workers: 並行ダウンロードの最大スレッド数
        """
        self.config = Config
        self.max_workers = max_workers
        # requests.Sessionはスレッドセーフではないため、スレッドローカルで管理
        self._thread_local = threading.local()

        self._base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': '*/*',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
        # Cookieが設定されている場合はリクエストにも付与（非公開アカウントのメディア取得に必要）
        if self.config.TWITTER_COOKIES:
            self._base_headers['Cookie'] = self.config.TWITTER_COOKIES
        
        # 並行ダウンロード用のキューとスレッド
        self.download_queue: Queue = Queue()
        self.download_thread: Optional[Thread] = None
        self.is_downloading = False
        self.downloaded_count = 0
        self.total_media = 0
        self.pbar: Optional[tqdm] = None

    def _get_session(self) -> requests.Session:
        """スレッドごとのSessionを返す"""
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update(self._base_headers)
            self._thread_local.session = sess
        return sess
    
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
                tweet_url = tweet.get('url')
                media_list = tweet.get('media', [])
                
                for media in media_list:
                    try:
                        # Refererとして使えるように保持（ダウンロードの403回避に効くことがある）
                        if tweet_url and isinstance(media, dict) and 'tweet_url' not in media:
                            media['tweet_url'] = tweet_url
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
        tweet_url = tweet.get('url')  # Referer用
        media_list = tweet.get('media', [])
        
        for media in media_list:
            # Refererとして使えるように保持（ダウンロードの403回避に効くことがある）
            if tweet_url and isinstance(media, dict) and 'tweet_url' not in media:
                media['tweet_url'] = tweet_url
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

        # blob: はブラウザ内部URLなのでrequestsでは取得できない
        if isinstance(url, str) and url.startswith("blob:"):
            logger.warning(f"blob URLのためスキップします: {tweet_id} idx={media_index}")
            return None

        # Referer（あると403回避に効くことがある）
        referer = None
        if isinstance(media, dict):
            referer = media.get("tweet_url") or None
        if not referer:
            referer = "https://twitter.com/"
        
        # URLを高解像度版に変換（画像の場合）
        if media_type == 'photo' and '?format=' not in url:
            # 高解像度版を取得
            url = url.replace(':small', ':large').replace(':thumb', ':large')

        # HLS(m3u8)はrequestsで素直に落としても動画にならないので、ffmpegがあれば変換する
        if isinstance(url, str) and ".m3u8" in url:
            _agent_log("H1", "media_downloader.py:_download_single_media", "m3u8 detected", {"tweet_id": tweet_id, "url": _safe_url_tag(url)})
            return self._download_hls_by_segments(url, tweet_id, media_index, referer)
        
        max_retry = 3
        backoff = 60  # 秒
        
        for attempt in range(1, max_retry + 1):
            try:
                sess = self._get_session()
                headers = {'Referer': referer}
                _agent_log("H2", "media_downloader.py:_download_single_media", "request", {"tweet_id": tweet_id, "type": media_type, "attempt": attempt, "url": _safe_url_tag(url), "referer": referer[:60]})
                response = sess.get(url, timeout=30, stream=True, headers=headers)
                _agent_log("H2", "media_downloader.py:_download_single_media", "response", {"tweet_id": tweet_id, "status": response.status_code, "ctype": response.headers.get("content-type", "")[:80]})
                
                if response.status_code == 429:
                    # 429は一般的に15分ウィンドウのことが多いので、最低900秒待機に引き上げ
                    wait_for = max(backoff, 900)
                    logger.warning(f"429 Too Many Requests (media): {url} - {wait_for}秒待機してリトライ ({attempt}/{max_retry})")
                    time.sleep(wait_for)
                    backoff = min(wait_for * 2, 3600)  # 最大1時間
                    continue

                if response.status_code in (401, 403):
                    # RefererやCookie不足、保護ツイート等
                    logger.warning(f"{response.status_code} (media): {url} - Referer/Cookieが必要な可能性があります")
                
                response.raise_for_status()
                
                # ファイル拡張子を決定
                ext = self._get_extension(url, media_type, response.headers.get('content-type'))
                _agent_log("H4", "media_downloader.py:_download_single_media", "extension", {"tweet_id": tweet_id, "ext": ext, "type": media_type})
                
                # 保存先パスを決定
                if media_type in ['photo', 'video_thumbnail']:
                    save_dir = self.config.IMAGES_DIR
                elif media_type in ['video', 'animated_gif']:
                    save_dir = self.config.VIDEOS_DIR
                else:
                    save_dir = self.config.OUTPUT_DIR
                
                filename = f"{tweet_id}_{media_index}{ext}"
                save_path = save_dir / filename

                # 既に存在する場合はスキップ
                if save_path.exists() and save_path.stat().st_size > 0:
                    return save_path
                
                # 原子的に保存（途中で落ちても壊れファイルを残しにくい）
                tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"{tweet_id}_{media_index}_", suffix=ext, dir=str(save_dir))
                os.close(tmp_fd)
                try:
                    with open(tmp_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    Path(tmp_path).replace(save_path)
                finally:
                    try:
                        if Path(tmp_path).exists():
                            Path(tmp_path).unlink()
                    except Exception:
                        pass
                
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


    def _parse_m3u8_playlist(self, m3u8_url: str, referer: str) -> List[str]:
        """m3u8ファイルをダウンロードしてパースし、セグメントURLのリストを返す"""
        # region agent log
        _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "enter", {"m3u8_url": _safe_url_tag(m3u8_url)})
        # endregion
        
        sess = self._get_session()
        headers = {'Referer': referer}
        
        try:
            response = sess.get(m3u8_url, timeout=20, headers=headers)
            response.raise_for_status()
            text = response.text or ""
            
            # region agent log
            _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "downloaded", {"m3u8_url": _safe_url_tag(m3u8_url), "content_length": len(text)})
            # endregion
            
            # master m3u8かどうかをチェック（#EXT-X-STREAM-INFがある場合）
            if "#EXT-X-STREAM-INF" in text:
                # region agent log
                _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "master_playlist", {"m3u8_url": _safe_url_tag(m3u8_url)})
                # endregion
                
                # 最高帯域のvariantを選択
                best_bw = -1
                best_uri = None
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                for i, ln in enumerate(lines):
                    if ln.startswith("#EXT-X-STREAM-INF"):
                        m = None
                        try:
                            m = next((p for p in ln.split(",") if "BANDWIDTH=" in p), None)
                        except Exception:
                            m = None
                        bw = -1
                        if m and "BANDWIDTH=" in m:
                            try:
                                bw = int(m.split("BANDWIDTH=")[-1])
                            except Exception:
                                bw = -1
                        # 次行がURI
                        if i + 1 < len(lines):
                            uri = lines[i + 1]
                            if not uri.startswith("#"):
                                if bw > best_bw:
                                    best_bw = bw
                                    best_uri = uri
                
                if best_uri:
                    # 相対URL対応（/で始まる場合はオリジンからの絶対パス）
                    if best_uri.startswith("http"):
                        variant_url = best_uri
                    elif best_uri.startswith("/"):
                        from urllib.parse import urlparse
                        parsed = urlparse(m3u8_url)
                        origin = f"{parsed.scheme}://{parsed.netloc}"
                        variant_url = f"{origin}{best_uri}"
                    else:
                        base = m3u8_url.rsplit("/", 1)[0]
                        variant_url = f"{base}/{best_uri}"
                    
                    # region agent log
                    _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "selected_variant", {"variant_url": _safe_url_tag(variant_url), "bandwidth": best_bw})
                    # endregion
                    
                    # 再帰的にvariantのm3u8を取得
                    return self._parse_m3u8_playlist(variant_url, referer)
            
            # セグメントURLを抽出（#EXTINFの後の行）
            segments: List[str] = []
            lines = text.splitlines()
            base_url = m3u8_url.rsplit("/", 1)[0]
            
            # オリジン抽出（/で始まる絶対パス用）
            from urllib.parse import urlparse
            parsed = urlparse(m3u8_url)
            origin = f"{parsed.scheme}://{parsed.netloc}"
            
            for i, line in enumerate(lines):
                line = line.strip()
                if line.startswith("#EXTINF"):
                    # 次の行がセグメントURL
                    if i + 1 < len(lines):
                        segment_url = lines[i + 1].strip()
                        if segment_url and not segment_url.startswith("#"):
                            # 相対URL対応（/で始まる場合はオリジンからの絶対パス）
                            if segment_url.startswith("http"):
                                segments.append(segment_url)
                            elif segment_url.startswith("/"):
                                segments.append(f"{origin}{segment_url}")
                            else:
                                segments.append(f"{base_url}/{segment_url}")
            
            # region agent log
            _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "parsed", {"segment_count": len(segments), "sample": [_safe_url_tag(s) for s in segments[:3]]})
            # endregion
            
            return segments
            
        except Exception as e:
            # region agent log
            _agent_log("HLS1", "media_downloader.py:_parse_m3u8_playlist", "error", {"error": str(e)[:200]})
            # endregion
            logger.error(f"m3u8パースエラー: {e}")
            return []
    
    def _download_segments(self, segment_urls: List[str], referer: str, temp_dir: Path) -> List[Path]:
        """セグメントURLのリストから各セグメントをダウンロード"""
        # region agent log
        _agent_log("HLS2", "media_downloader.py:_download_segments", "enter", {"segment_count": len(segment_urls)})
        # endregion
        
        sess = self._get_session()
        headers = {'Referer': referer}
        downloaded_segments: List[Path] = []
        
        for idx, segment_url in enumerate(segment_urls):
            try:
                # region agent log
                _agent_log("HLS2", "media_downloader.py:_download_segments", "downloading", {"index": idx, "total": len(segment_urls), "url": _safe_url_tag(segment_url)})
                # endregion
                
                response = sess.get(segment_url, timeout=30, headers=headers, stream=True)
                response.raise_for_status()
                
                segment_path = temp_dir / f"segment_{idx:05d}.ts"
                with open(segment_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                downloaded_segments.append(segment_path)
                
                # region agent log
                _agent_log("HLS2", "media_downloader.py:_download_segments", "downloaded", {"index": idx, "size": segment_path.stat().st_size})
                # endregion
                
            except Exception as e:
                # region agent log
                _agent_log("HLS2", "media_downloader.py:_download_segments", "error", {"index": idx, "error": str(e)[:200]})
                # endregion
                logger.error(f"セグメントダウンロードエラー ({segment_url}): {e}")
                # エラーがあっても続行（一部セグメントが失敗しても結合は試みる）
        
        # region agent log
        _agent_log("HLS2", "media_downloader.py:_download_segments", "complete", {"downloaded_count": len(downloaded_segments), "total": len(segment_urls)})
        # endregion
        
        return downloaded_segments
    
    def _concatenate_segments(self, segment_paths: List[Path], output_path: Path) -> bool:
        """セグメントファイルを結合"""
        # region agent log
        _agent_log("HLS3", "media_downloader.py:_concatenate_segments", "enter", {"segment_count": len(segment_paths), "output": str(output_path)})
        # endregion
        
        if not segment_paths:
            # region agent log
            _agent_log("HLS3", "media_downloader.py:_concatenate_segments", "no_segments", {})
            # endregion
            return False
        
        try:
            # .tsファイルを単純にバイナリ連結
            with open(output_path, 'wb') as outfile:
                for seg_path in segment_paths:
                    if seg_path.exists():
                        with open(seg_path, 'rb') as infile:
                            outfile.write(infile.read())
            
            # region agent log
            _agent_log("HLS3", "media_downloader.py:_concatenate_segments", "concatenated", {"output_size": output_path.stat().st_size if output_path.exists() else 0})
            # endregion
            
            return output_path.exists() and output_path.stat().st_size > 0
            
        except Exception as e:
            # region agent log
            _agent_log("HLS3", "media_downloader.py:_concatenate_segments", "error", {"error": str(e)[:200]})
            # endregion
            logger.error(f"セグメント結合エラー: {e}")
            return False
    
    def _download_hls_by_segments(self, m3u8_url: str, tweet_id: str, media_index: int, referer: str) -> Optional[Path]:
        """m3u8(HLS)をセグメントごとにダウンロードして結合"""
        # region agent log
        _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "enter", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url), "referer": _safe_url_tag(referer)})
        # endregion
        
        save_dir = self.config.VIDEOS_DIR
        try:
            save_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # region agent log
            _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "mkdir_failed", {"error": str(e)})
            # endregion
            logger.error(f"保存ディレクトリの作成に失敗: {e}")
            return None
        
        save_path = save_dir / f"{tweet_id}_{media_index}.mp4"
        if save_path.exists() and save_path.stat().st_size > 0:
            # region agent log
            _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "already_exists", {"size": save_path.stat().st_size})
            # endregion
            return save_path
        
        # ステップ1: m3u8ファイルをパースしてセグメントURLを取得
        segment_urls = self._parse_m3u8_playlist(m3u8_url, referer)
        if not segment_urls:
            # region agent log
            _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "no_segments", {})
            # endregion
            logger.error(f"m3u8からセグメントURLを取得できませんでした: {m3u8_url}")
            return None
        
        # ステップ2: セグメントをダウンロード（一時ディレクトリに保存）
        temp_dir = tempfile.mkdtemp(prefix=f"hls_{tweet_id}_{media_index}_")
        temp_path = Path(temp_dir)
        
        try:
            downloaded_segments = self._download_segments(segment_urls, referer, temp_path)
            
            if not downloaded_segments:
                # region agent log
                _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "no_downloaded_segments", {})
                # endregion
                logger.error(f"セグメントのダウンロードに失敗しました")
                return None
            
            # ステップ3: セグメントを結合（一時ファイルとして.tsで保存）
            temp_ts = temp_path / "combined.ts"
            success = self._concatenate_segments(downloaded_segments, temp_ts)
            
            if not success or not temp_ts.exists():
                # region agent log
                _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "concatenate_failed", {})
                # endregion
                logger.error(f"セグメントの結合に失敗しました")
                return None
            
            # ステップ4: .tsファイルをMP4に変換（ffmpegがあれば）
            ffmpeg = shutil.which("ffmpeg")
            if ffmpeg:
                # region agent log
                _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "converting_to_mp4", {})
                # endregion
                try:
                    cmd = [
                        ffmpeg,
                        "-y",
                        "-loglevel", "error",
                        "-i", str(temp_ts),
                        "-c", "copy",
                        str(save_path),
                    ]
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
                    if save_path.exists() and save_path.stat().st_size > 0:
                        # region agent log
                        _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "success", {"save_path": str(save_path), "size": save_path.stat().st_size})
                        # endregion
                        return save_path
                except Exception as e:
                    # region agent log
                    _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "ffmpeg_conversion_failed", {"error": str(e)[:200]})
                    # endregion
                    logger.warning(f"ffmpegでのMP4変換に失敗: {e}")
            
            # ffmpegがない場合、または変換に失敗した場合は.tsファイルのまま保存
            ts_save_path = save_dir / f"{tweet_id}_{media_index}.ts"
            try:
                import shutil as _shutil
                _shutil.move(str(temp_ts), str(ts_save_path))
            except Exception as e:
                # region agent log
                _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "move_failed", {"error": str(e)[:200]})
                # endregion
                logger.error(f".tsファイルの移動に失敗: {e}")
                return None
            
            # region agent log
            _agent_log("HLS0", "media_downloader.py:_download_hls_by_segments", "saved_as_ts", {"save_path": str(ts_save_path), "size": ts_save_path.stat().st_size})
            # endregion
            logger.info(f".tsファイルとして保存しました（多くのプレーヤーで再生可能）: {ts_save_path}")
            return ts_save_path
                
        finally:
            # 一時ファイルをクリーンアップ
            try:
                import shutil as _shutil
                _shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    def _download_hls_with_ffmpeg(self, m3u8_url: str, tweet_id: str, media_index: int, referer: str) -> Optional[Path]:
        """m3u8(HLS)をffmpegでmp4として保存（ffmpegが無ければスキップ）"""
        # region agent log
        _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "enter", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url), "referer": _safe_url_tag(referer)})
        # endregion
        
        # ffmpegを探す（まずPATHから、見つからなければシステム環境変数から直接取得）
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            # システム環境変数から直接取得を試みる（Windows環境変数が現在のプロセスに反映されていない場合）
            import os
            path_entries = []
            
            # 現在のプロセスのPATH
            path_entries.extend(os.environ.get('PATH', '').split(os.pathsep))
            
            # Windowsレジストリから環境変数を直接読み取る
            try:
                import winreg
                # ユーザー環境変数
                try:
                    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
                        user_path = winreg.QueryValueEx(key, "PATH")[0]
                        path_entries.extend(user_path.split(os.pathsep))
                except (FileNotFoundError, OSError):
                    pass
                
                # システム環境変数
                try:
                    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment") as key:
                        sys_path = winreg.QueryValueEx(key, "PATH")[0]
                        path_entries.extend(sys_path.split(os.pathsep))
                except (FileNotFoundError, OSError):
                    pass
            except ImportError:
                pass  # winregが利用できない場合（Linuxなど）はスキップ
            
            # PATH文字列からffmpegを探す
            for path_entry in path_entries:
                if path_entry and 'ffmpeg' in path_entry.lower():
                    ffmpeg_exe = Path(path_entry) / "ffmpeg.exe"
                    if ffmpeg_exe.exists():
                        ffmpeg = str(ffmpeg_exe)
                        break
        
        # region agent log
        _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "ffmpeg_check", {"ffmpeg_path": ffmpeg or "NOT_FOUND", "tweet_id": tweet_id})
        # endregion
        
        if not ffmpeg:
            logger.warning(f"m3u8のためffmpegが必要です。スキップします: {m3u8_url}")
            # region agent log
            _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "ffmpeg_not_found", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url)})
            # endregion
            return None

        save_dir = self.config.VIDEOS_DIR
        # region agent log
        _agent_log("H2", "media_downloader.py:_download_hls_with_ffmpeg", "before_mkdir", {"save_dir": str(save_dir), "tweet_id": tweet_id})
        # endregion
        
        try:
            save_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # region agent log
            _agent_log("H2", "media_downloader.py:_download_hls_with_ffmpeg", "mkdir_failed", {"save_dir": str(save_dir), "error": str(e), "tweet_id": tweet_id})
            # endregion
            logger.error(f"保存ディレクトリの作成に失敗: {e}")
            return None
        
        save_path = save_dir / f"{tweet_id}_{media_index}.mp4"
        # region agent log
        _agent_log("H3", "media_downloader.py:_download_hls_with_ffmpeg", "check_existing", {"save_path": str(save_path), "exists": save_path.exists() if save_path.exists() else False, "tweet_id": tweet_id})
        # endregion
        
        if save_path.exists() and save_path.stat().st_size > 0:
            # region agent log
            _agent_log("H3", "media_downloader.py:_download_hls_with_ffmpeg", "already_exists", {"save_path": str(save_path), "size": save_path.stat().st_size, "tweet_id": tweet_id})
            # endregion
            return save_path

        # Refererが必要なケースに備えて、ffmpegにヘッダを渡す
        # -headers は "Key: Value\r\n" 形式を連結
        headers = f"Referer: {referer}\r\n"
        headers += "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
        if self.config.TWITTER_COOKIES:
            headers += f"Cookie: {self.config.TWITTER_COOKIES}\r\n"
        
        # region agent log
        _agent_log("H4", "media_downloader.py:_download_hls_with_ffmpeg", "before_cmd", {"has_cookie": bool(self.config.TWITTER_COOKIES), "headers_len": len(headers), "has_user_agent": "User-Agent" in headers, "tweet_id": tweet_id})
        # endregion

        cmd = [
            ffmpeg,
            "-y",
            "-loglevel", "error",
            "-headers", headers,
            "-i", m3u8_url,
            "-c", "copy",
            str(save_path),
        ]
        # region agent log
        _agent_log("H4", "media_downloader.py:_download_hls_with_ffmpeg", "cmd_constructed", {"cmd_len": len(cmd), "tweet_id": tweet_id})
        # endregion
        
        try:
            # region agent log
            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_start", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url)})
            # endregion
            
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
            
            # region agent log
            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_success", {"tweet_id": tweet_id, "returncode": result.returncode, "save_path": str(save_path), "file_exists": save_path.exists() if save_path.exists() else False})
            # endregion
            
            if save_path.exists() and save_path.stat().st_size > 0:
                # region agent log
                _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "file_saved", {"tweet_id": tweet_id, "save_path": str(save_path), "size": save_path.stat().st_size})
                # endregion
                return save_path
            else:
                # region agent log
                _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "file_not_created", {"tweet_id": tweet_id, "save_path": str(save_path)})
                # endregion
                logger.error(f"ffmpeg実行後、ファイルが作成されませんでした: {save_path}")
                return None
                
        except subprocess.TimeoutExpired as e:
            # region agent log
            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_timeout", {"tweet_id": tweet_id, "error": str(e)})
            # endregion
            logger.error(f"ffmpegでのm3u8保存がタイムアウトしました: {e}")
            try:
                if save_path.exists() and save_path.stat().st_size == 0:
                    save_path.unlink()
            except Exception:
                pass
            return None
        except subprocess.CalledProcessError as e:
            # region agent log
            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "subprocess_error", {"tweet_id": tweet_id, "returncode": e.returncode, "stderr": (e.stderr or "")[:500], "stdout": (e.stdout or "")[:500]})
            # endregion
            logger.error(f"ffmpegでのm3u8保存に失敗 (returncode={e.returncode}): {e.stderr or e.stdout or str(e)}")
            try:
                if save_path.exists() and save_path.stat().st_size == 0:
                    save_path.unlink()
            except Exception:
                pass
            return None
        except Exception as e:
            # region agent log
            _agent_log("H5", "media_downloader.py:_download_hls_with_ffmpeg", "exception", {"tweet_id": tweet_id, "error_type": type(e).__name__, "error": str(e)[:500]})
            # endregion
            logger.error(f"ffmpegでのm3u8保存に失敗: {e}")
            try:
                if save_path.exists() and save_path.stat().st_size == 0:
                    save_path.unlink()
            except Exception:
                pass
            return None
    
    def _get_extension(self, url: str, media_type: str, content_type: str = None) -> str:
        """ファイル拡張子を取得"""
        # URLから拡張子を取得
        if '.' in url:
            ext = '.' + url.split('.')[-1].split('?')[0].split(':')[0]
            if ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.mov', '.webm']:
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
            elif 'video/webm' in content_type:
                return '.webm'
        
        # デフォルト
        if media_type == 'photo':
            return '.jpg'
        elif media_type in ['video', 'animated_gif']:
            return '.mp4'
        else:
            return '.bin'

