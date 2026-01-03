#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Update _download_hls_with_ffmpeg with logging"""

import re
from pathlib import Path

file_path = Path(__file__).parent / "media_downloader.py"
content = file_path.read_text(encoding='utf-8')

# Find and replace the method
old_pattern = r'(    def _download_hls_with_ffmpeg\(self, m3u8_url: str, tweet_id: str, media_index: int, referer: str\) -> Optional\[Path\]:.*?)(        return None\n    )'
new_method = '''    def _download_hls_with_ffmpeg(self, m3u8_url: str, tweet_id: str, media_index: int, referer: str) -> Optional[Path]:
        """m3u8(HLS)をffmpegでmp4として保存（ffmpegが無ければスキップ）"""
        # region agent log
        _agent_log("H1", "media_downloader.py:_download_hls_with_ffmpeg", "enter", {"tweet_id": tweet_id, "m3u8_url": _safe_url_tag(m3u8_url), "referer": _safe_url_tag(referer)})
        # endregion
        
        ffmpeg = shutil.which("ffmpeg")
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
        save_dir.mkdir(exist_ok=True)
        save_path = save_dir / f"{tweet_id}_{media_index}.mp4"
        if save_path.exists() and save_path.stat().st_size > 0:
            return save_path

        # Refererが必要なケースに備えて、ffmpegにヘッダを渡す
        # -headers は "Key: Value\\r\\n" 形式を連結
        headers = f"Referer: {referer}\\r\\n"
        if self.config.TWITTER_COOKIES:
            headers += f"Cookie: {self.config.TWITTER_COOKIES}\\r\\n"

        cmd = [
            ffmpeg,
            "-y",
            "-loglevel", "error",
            "-headers", headers,
            "-i", m3u8_url,
            "-c", "copy",
            str(save_path),
        ]
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
    '''

# Use a simpler pattern - find method start and replace until the next method
match = re.search(r'    def _download_hls_with_ffmpeg\(self[^:]+:.*?\n)(.*?)(    def _get_extension)', content, re.DOTALL)
if match:
    # Replace everything between method signature and next method
    new_content = content[:match.start()] + new_method + '\n    ' + content[match.end(2):]
    file_path.write_text(new_content, encoding='utf-8')
    print("Updated _download_hls_with_ffmpeg method with logging")
else:
    print("Pattern not found, trying alternative approach...")
    # Alternative: find by line numbers
    lines = content.split('\n')
    start_idx = None
    for i, line in enumerate(lines):
        if 'def _download_hls_with_ffmpeg' in line:
            start_idx = i
            break
    if start_idx:
        # Find end (next method at same indent level)
        indent = len(lines[start_idx]) - len(lines[start_idx].lstrip())
        end_idx = start_idx + 1
        while end_idx < len(lines) and (not lines[end_idx].strip() or (lines[end_idx].strip() and (lines[end_idx].strip().startswith('#') or (not lines[end_idx].strip().startswith('def ') and len(lines[end_idx]) - len(lines[end_idx].lstrip()) > indent)))):
            end_idx += 1
        # Replace
        new_lines = lines[:start_idx] + new_method.split('\n') + lines[end_idx:]
        file_path.write_text('\n'.join(new_lines), encoding='utf-8')
        print(f"Updated _download_hls_with_ffmpeg method (lines {start_idx+1}-{end_idx})")
    else:
        print("ERROR: Method not found")





