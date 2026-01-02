"""Twitter動画URL解決（Playwrightでネットワークからm3u8/mp4/webmを捕捉）"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright

from config import Config

# region agent log
_DEBUG_LOG_PATH = r"h:\document\program\project\getTweet\.cursor\debug.log"
_LOCAL_DEBUG_NDJSON = str((Path(__file__).resolve().parent / "debug.ndjson"))
_LOCAL_CURSOR_DEBUG = str((Path(__file__).resolve().parent / ".cursor" / "debug.log"))


def _agent_log(hypothesisId: str, location: str, message: str, data: Optional[Dict] = None, runId: str = "pre") -> None:
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": runId,
            "hypothesisId": hypothesisId,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        for p in (_DEBUG_LOG_PATH, _LOCAL_DEBUG_NDJSON, _LOCAL_CURSOR_DEBUG):
            try:
                Path(p).parent.mkdir(parents=True, exist_ok=True)
                with open(p, "a", encoding="utf-8") as f:
                    f.write(line)
            except Exception:
                pass
    except Exception:
        pass


def _safe_url_tag(url: Optional[str]) -> str:
    try:
        return (url or "")[:200]
    except Exception:
        return ""

# endregion


def _best_m3u8_from_master(m3u8_url: str, sess: requests.Session, referer: str) -> str:
    """master m3u8なら最高帯域のvariantを選び、そうでなければそのまま返す"""
    try:
        r = sess.get(m3u8_url, timeout=20, headers={"Referer": referer})
        if r.status_code != 200:
            return m3u8_url
        text = r.text or ""
        if "#EXT-X-STREAM-INF" not in text:
            return m3u8_url

        best_bw = -1
        best_uri = None
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for i, ln in enumerate(lines):
            if ln.startswith("#EXT-X-STREAM-INF"):
                bw = -1
                try:
                    for part in ln.split(","):
                        if "BANDWIDTH=" in part:
                            bw = int(part.split("BANDWIDTH=")[-1])
                            break
                except Exception:
                    bw = -1
                if i + 1 < len(lines):
                    uri = lines[i + 1]
                    if not uri.startswith("#") and bw > best_bw:
                        best_bw = bw
                        best_uri = uri

        if not best_uri:
            return m3u8_url

        if best_uri.startswith("http"):
            return best_uri
        base = m3u8_url.rsplit("/", 1)[0]
        return f"{base}/{best_uri.lstrip('/')}"
    except Exception:
        return m3u8_url


def resolve_video_urls_with_playwright(tweet_url: str, max_wait_ms: int = 20000) -> List[str]:
    """ツイートURLを開き、再生に使われるm3u8/mp4/webm URLをネットワークから捕捉する"""
    found: List[str] = []
    seen = set()

    def on_request(req):
        try:
            u = req.url
            if "video.twimg.com" not in u:
                return
            if any(ext in u for ext in (".m3u8", ".mp4", ".webm")):
                if u not in seen:
                    seen.add(u)
                    found.append(u)
        except Exception:
            return

    _agent_log("H5", "twitter_video_api.py:resolve_video_urls_with_playwright", "enter", {"tweet_url": _safe_url_tag(tweet_url)})

    with sync_playwright() as p:
        launch_opts = dict(headless=True, args=["--disable-blink-features=AutomationControlled"])
        if Config.USE_SYSTEM_CHROME:
            launch_opts["channel"] = "chrome"
        browser = p.chromium.launch(**launch_opts)
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        if Config.TWITTER_COOKIES:
            cookies = []
            for item in Config.TWITTER_COOKIES.split(";"):
                item = item.strip()
                if "=" in item:
                    k, v = item.split("=", 1)
                    cookies.append({"name": k.strip(), "value": v.strip(), "domain": "twitter.com", "path": "/"})
            if cookies:
                try:
                    context.add_cookies(cookies)
                except Exception:
                    pass

        page = context.new_page()
        page.on("request", on_request)
        page.goto(tweet_url, wait_until="domcontentloaded")

        # 動画再生を誘発
        try:
            btn = page.query_selector('button[data-testid="playButton"]')
            if btn:
                btn.click(timeout=2000)
            else:
                v = page.query_selector("video")
                if v:
                    v.click(timeout=2000)
        except Exception:
            pass

        deadline = time.time() + (max_wait_ms / 1000.0)
        while time.time() < deadline:
            if found:
                break
            time.sleep(0.2)

        context.close()
        browser.close()

    _agent_log("H5", "twitter_video_api.py:resolve_video_urls_with_playwright", "exit", {"found": len(found), "sample": [_safe_url_tag(u) for u in found[:3]]})
    return found


def resolve_best_video_url(tweet_url: str) -> Optional[str]:
    """ツイートURLから最適な動画URL（MP4優先、次にWebM、最後にm3u8）を返す"""
    urls = resolve_video_urls_with_playwright(tweet_url)
    if not urls:
        return None

    mp4s = [u for u in urls if ".mp4" in u]
    if mp4s:
        return mp4s[0]
    webms = [u for u in urls if ".webm" in u]
    if webms:
        return webms[0]
    m3u8s = [u for u in urls if ".m3u8" in u]
    if not m3u8s:
        return None

    sess = requests.Session()
    if Config.TWITTER_COOKIES:
        sess.headers["Cookie"] = Config.TWITTER_COOKIES
    return _best_m3u8_from_master(m3u8s[0], sess, referer=tweet_url)





