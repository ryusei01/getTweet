"""メディアのみ保存（ユーティリティ）"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

from config import Config

logger = logging.getLogger(__name__)

# region agent log
_DEBUG_LOG_PATH = r"h:\document\program\project\getTweet\.cursor\debug.log"
_LOCAL_DEBUG_NDJSON = str((Path(__file__).resolve().parent / "debug.ndjson"))
_LOCAL_CURSOR_DEBUG = str((Path(__file__).resolve().parent / ".cursor" / "debug.log"))


def _agent_log(hypothesisId: str, location: str, message: str, data: Optional[Dict] = None, runId: str = "pre") -> None:
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

        # 1) 指定ログパス（あれば）
        try:
            Path(_DEBUG_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
            with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass

        # 2) 実行中プロジェクト直下（確実に見つけられる）
        try:
            with open(_LOCAL_DEBUG_NDJSON, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass

        # 3) 実行中プロジェクトの .cursor 配下（作れれば）
        try:
            Path(_LOCAL_CURSOR_DEBUG).parent.mkdir(parents=True, exist_ok=True)
            with open(_LOCAL_CURSOR_DEBUG, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            pass
    except Exception:
        pass


def _safe_url_tag(url: Optional[str]) -> str:
    try:
        if not url:
            return ""
        return str(url)[:160]
    except Exception:
        return ""

# endregion


def load_tweets_from_result_json(path: Path) -> List[Dict]:
    """DataSaverの出力JSON（{metadata, tweets}）またはtweets配列JSONを読み込む"""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "tweets" in data and isinstance(data["tweets"], list):
        return data["tweets"]
    if isinstance(data, list):
        return data
    raise ValueError(f"対応していないJSON形式です: {path}")


def ensure_media_has_tweet_url(tweets: Iterable[Dict]) -> None:
    """メディアdictにtweet_url(Referer用)が無ければ付与する（403回避に効く場合あり）"""
    for tweet in tweets:
        tweet_url = tweet.get("url")
        if not tweet_url:
            continue
        for media in tweet.get("media", []) or []:
            if isinstance(media, dict) and "tweet_url" not in media:
                media["tweet_url"] = tweet_url


def _pick_video_url_from_html(html: str) -> Optional[str]:
    """HTML文字列から video.twimg.com の動画URLを拾う（MP4優先、次にWebM、最後にm3u8）"""
    if not html:
        return None
    mp4s = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.mp4[^\"'\\s>]*", html)
    if mp4s:
        _agent_log("H3", "media_only.py:_pick_video_url_from_html", "picked mp4 from html", {"url": _safe_url_tag(mp4s[0])})
        return mp4s[0]
    webms = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.webm[^\"'\\s>]*", html)
    if webms:
        _agent_log("H3", "media_only.py:_pick_video_url_from_html", "picked webm from html", {"url": _safe_url_tag(webms[0])})
        return webms[0]
    m3u8s = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.m3u8[^\"'\\s>]*", html)
    if m3u8s:
        _agent_log("H3", "media_only.py:_pick_video_url_from_html", "picked m3u8 from html", {"url": _safe_url_tag(m3u8s[0])})
        return m3u8s[0]
    return None


def _extract_tweet_id(url: str) -> Optional[str]:
    """URLからツイートIDを抽出"""
    if not url:
        return None
    match = re.search(r"status/(\d+)", str(url))
    return match.group(1) if match else None


def _resolve_video_from_syndication(tweet_id: str, sess: requests.Session) -> Optional[str]:
    """cdn.syndication.twimg.com/tweet-result を使って動画URLを取得（最も高画質なMP4/WebMを選択）"""
    api = f"https://cdn.syndication.twimg.com/tweet-result?id={tweet_id}&lang=en"
    try:
        resp = sess.get(api, timeout=15)
        _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "syndication response", {"tweet_id": tweet_id, "status": resp.status_code})
        if resp.status_code != 200:
            return None
        data = resp.json()

        video_info = None
        media_items = data.get("entities", {}).get("media", []) or []
        if not media_items:
            media_items = data.get("mediaDetails", []) or []
        for m in media_items:
            v_info = m.get("video_info")
            if v_info:
                video_info = v_info
                break
        if not video_info:
            _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "no video_info", {"tweet_id": tweet_id})
            return None

        variants = video_info.get("variants", []) or []
        mp4s = [v for v in variants if v.get("content_type") == "video/mp4" and v.get("url")]
        webms = [v for v in variants if v.get("content_type") == "video/webm" and v.get("url")]
        m3u8s = [v for v in variants if v.get("url") and ".m3u8" in v.get("url")]
        _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "variants summary", {"tweet_id": tweet_id, "mp4": len(mp4s), "webm": len(webms), "m3u8": len(m3u8s)})

        if mp4s:
            mp4s.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
            picked = mp4s[0].get("url")
            _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "picked mp4", {"tweet_id": tweet_id, "bitrate": mp4s[0].get("bitrate", 0), "url": _safe_url_tag(picked)})
            return picked
        if webms:
            webms.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
            picked = webms[0].get("url")
            _agent_log("H4", "media_only.py:_resolve_video_from_syndication", "picked webm", {"tweet_id": tweet_id, "bitrate": webms[0].get("bitrate", 0), "url": _safe_url_tag(picked)})
            return picked
        if m3u8s:
            picked = m3u8s[0].get("url")
            _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "picked m3u8", {"tweet_id": tweet_id, "url": _safe_url_tag(picked)})
            return picked
        return None
    except Exception as e:
        _agent_log("H1", "media_only.py:_resolve_video_from_syndication", "exception", {"tweet_id": tweet_id, "err": str(e)[:160]})
        return None


def _looks_like_video_thumbnail(url: str) -> bool:
    if not url:
        return False
    return any(k in url for k in ["ext_tw_video_thumb", "amplify_video_thumb"])


def enrich_tweets_with_resolved_videos_from_thumbnails(
    tweets: List[Dict],
    *,
    max_resolve: int = 0,
    sleep_seconds: float = 0.5,
) -> int:
    """
    JSON内のmediaが photo でも、URLが動画サムネっぽい場合にツイートHTMLから実動画URLを探して追加する。

    Returns:
        追加できた video 件数
    """
    if not tweets:
        return 0

    _agent_log("H2", "media_only.py:enrich_tweets_with_resolved_videos_from_thumbnails", "enter", {"tweets": len(tweets)})

    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
    )
    if Config.TWITTER_COOKIES:
        sess.headers["Cookie"] = Config.TWITTER_COOKIES

    resolved = 0
    checked = 0
    for tweet in tweets:
        if max_resolve > 0 and checked >= max_resolve:
            break

        tweet_url = tweet.get("url")
        if not tweet_url:
            continue

        media_list = tweet.get("media", []) or []
        if not isinstance(media_list, list) or not media_list:
            continue

        # 既にvideoがあるならスキップ（重複回避）
        if any(isinstance(m, dict) and m.get("type") == "video" for m in media_list):
            continue

        thumb_candidates = [
            m
            for m in media_list
            if isinstance(m, dict)
            and m.get("url")
            and _looks_like_video_thumbnail(str(m.get("url")))
        ]
        if not thumb_candidates:
            continue

        checked += 1
        try:
            tweet_id = _extract_tweet_id(tweet_url)
            _agent_log("H2", "media_only.py:enrich", "candidate", {"tweet_id": tweet_id or "", "url": _safe_url_tag(tweet_url), "thumbs": len(thumb_candidates)})

            video_url = None
            if tweet_id:
                video_url = _resolve_video_from_syndication(tweet_id, sess)

            # 手順1.5: Syndicationが空/動画情報無しの場合はPlaywrightでネットワークから捕捉
            if not video_url:
                try:
                    from twitter_video_api import resolve_best_video_url

                    video_url = resolve_best_video_url(tweet_url)
                    if video_url:
                        _agent_log("H5", "media_only.py:enrich", "picked from playwright", {"tweet_id": tweet_id or "", "video_url": _safe_url_tag(video_url)})
                except Exception as e:
                    _agent_log("H5", "media_only.py:enrich", "playwright exception", {"tweet_id": tweet_id or "", "err": str(e)[:160]})

            if not video_url:
                # Refererを付けると弾かれにくいケースがある
                resp = sess.get(tweet_url, timeout=30, headers={"Referer": "https://twitter.com/"})
                _agent_log("H3", "media_only.py:enrich", "tweet html response", {"tweet_id": tweet_id or "", "status": resp.status_code})
                if resp.status_code in (401, 403):
                    logger.warning(f"{resp.status_code} でツイートHTML取得に失敗（Cookie不足の可能性）: {tweet_url}")
                    continue
                resp.raise_for_status()
                video_url = _pick_video_url_from_html(resp.text)

            if not video_url:
                _agent_log("H2", "media_only.py:enrich", "no video url", {"tweet_id": tweet_id or ""})
                continue

            # 先頭のサムネを紐付け
            thumb = thumb_candidates[0]
            media_index = thumb.get("media_index", 0)
            media_list.append(
                {
                    "type": "video",
                    "url": video_url,
                    "media_index": media_index,
                    "thumbnail_url": thumb.get("url"),
                    "tweet_url": tweet_url,
                    "resolved_from": "resolved",
                }
            )
            tweet["media"] = media_list
            resolved += 1
            _agent_log("H2", "media_only.py:enrich", "added video", {"tweet_id": tweet_id or "", "video_url": _safe_url_tag(video_url)})
        except Exception as e:
            _agent_log("H2", "media_only.py:enrich", "exception", {"err": str(e)[:160], "url": _safe_url_tag(tweet_url)})
        finally:
            if sleep_seconds:
                time.sleep(sleep_seconds)

    _agent_log("H2", "media_only.py:enrich_tweets_with_resolved_videos_from_thumbnails", "exit", {"checked": checked, "resolved": resolved})
    return resolved


def normalize_username(username: Optional[str]) -> str:
    """@の有無や大小文字の差を吸収してユーザー名を正規化"""
    if not username:
        return ""
    u = username.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower()


def is_target_author(tweet: Dict, target_username: str) -> bool:
    """Tweetが指定ユーザー本人の投稿かどうか（RT等で別作者になるケースを除外）"""
    author = normalize_username(tweet.get("author_username"))
    target = normalize_username(target_username)
    return bool(author) and author == target


def filter_tweets_by_author(tweets: Iterable[Dict], target_username: str) -> List[Dict]:
    """指定ユーザー本人の投稿のみ抽出"""
    return [t for t in tweets if is_target_author(t, target_username)]


def build_media_manifest_from_tweets(tweets: Iterable[Dict]) -> Dict:
    """Tweet配列からメディア一覧のマニフェストを生成"""
    media_items: List[Dict] = []
    tweet_count = 0
    for tweet in tweets:
        tweet_count += 1
        tweet_id = tweet.get("tweet_id")
        created_at = tweet.get("created_at")
        author_username = tweet.get("author_username")
        tweet_url = tweet.get("url")
        for media in tweet.get("media", []) or []:
            media_items.append(
                {
                    "tweet_id": tweet_id,
                    "created_at": created_at,
                    "author_username": author_username,
                    "tweet_url": tweet_url,
                    "media_index": media.get("media_index", 0),
                    "type": media.get("type"),
                    "url": media.get("url"),
                    "local_path": media.get("local_path"),
                    "file_size": media.get("file_size"),
                }
            )

    downloaded = sum(1 for m in media_items if m.get("local_path"))
    return {
        "metadata": {
            "exported_at": datetime.now().isoformat(),
            "total_tweets_included": tweet_count,
            "total_media": len(media_items),
            "downloaded_media": downloaded,
            "run_dir": str(Config.RUN_DIR),
        },
        "media": media_items,
    }


def save_media_manifest_from_tweets(
    tweets: Iterable[Dict],
    filename: str = "media_manifest.json",
    output_dir: Optional[Path] = None,
) -> Path:
    """メディアマニフェストをJSONで保存"""
    out_dir = output_dir or Config.RUN_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / filename
    data = build_media_manifest_from_tweets(tweets)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return output_path


