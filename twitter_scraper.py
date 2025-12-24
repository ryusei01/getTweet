"""Twitter Tweet取得モジュール"""
import time
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote_plus
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
import logging
from tqdm import tqdm

from config import Config

logger = logging.getLogger(__name__)


class TwitterScraper:
    """Twitter Tweetスクレイパー"""
    
    def __init__(self):
        self.config = Config
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.tweets: List[Dict] = []
        # 429対策: 初回は15分待機をデフォルトに（Twitterの一般的な制限ウィンドウに合わせる）
        self.rate_limit_wait = 900  # 15分（秒）
        
    def _setup_browser(self):
        """ブラウザをセットアップ"""
        self.playwright = sync_playwright().start()
        
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--autoplay-policy=no-user-gesture-required',
            '--use-gl=angle',  # ハードウェアアクセラレーション有効化のため
        ]

        # ユーザーデータディレクトリが指定されている場合
        if self.config.USER_DATA_DIR:
            user_data_dir = Path(self.config.USER_DATA_DIR)
            user_data_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"ユーザーデータディレクトリを使用: {user_data_dir}")
            
            # 永続コンテキストを作成（user_data_dirを使用）
            launch_opts = dict(
                user_data_dir=str(user_data_dir),
                headless=self.config.HEADLESS,
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                args=launch_args,
            )
            if self.config.USE_SYSTEM_CHROME:
                launch_opts["channel"] = "chrome"  # システムChromeを使用（H.264/Widevine対応）
            self.context = self.playwright.chromium.launch_persistent_context(**launch_opts)
            
            # 既存のページを使用するか、新しいページを作成
            if len(self.context.pages) > 0:
                self.page = self.context.pages[0]
            else:
                self.page = self.context.new_page()
        else:
            # 従来の方法（user_data_dirを使用しない）
            launch_opts = dict(
                headless=self.config.HEADLESS,
                args=launch_args,
            )
            if self.config.USE_SYSTEM_CHROME:
                launch_opts["channel"] = "chrome"  # システムChromeを使用（H.264/Widevine対応）
            self.browser = self.playwright.chromium.launch(**launch_opts)
            
            # 新しいコンテキストを作成
            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            self.page = self.context.new_page()
            
            # Cookieを設定（user_data_dirを使用しない場合のみ）
            if self.config.TWITTER_COOKIES:
                cookies = self._parse_cookies(self.config.TWITTER_COOKIES)
                self.context.add_cookies(cookies)
                logger.info("Cookieを設定しました")
    
    def _parse_cookies(self, cookie_string: str) -> List[Dict]:
        """Cookie文字列をパース"""
        cookies = []
        domain = "twitter.com"
        
        # セミコロンで分割
        for item in cookie_string.split(';'):
            item = item.strip()
            if '=' in item:
                key, value = item.split('=', 1)
                cookies.append({
                    'name': key.strip(),
                    'value': value.strip(),
                    'domain': domain,
                    'path': '/'
                })
        
        return cookies
    
    def _wait_for_page_load(self, timeout: int = 30000):
        """ページの読み込みを待つ"""
        try:
            self.page.wait_for_load_state("networkidle", timeout=timeout)
        except:
            pass  # タイムアウトしても続行
    
    def get_user_tweets(
        self,
        username: str,
        use_search: bool = False,
        since: Optional[str] = None,
        until: Optional[str] = None,
        days_per_chunk: int = 7,
    ) -> List[Dict]:
        """指定ユーザーのTweetを取得（他人のアカウントも可）
        
        Args:
            username: Twitterユーザー名（@なし）
            use_search: Trueの場合、検索クエリで期間を分割して取得
            since: 取得開始日 (YYYY-MM-DD)。未指定なら1年前をデフォルトに設定
            until: 取得終了日 (YYYY-MM-DD)。未指定なら今日
            days_per_chunk: 検索モード時のチャンク日数
            
        Returns:
            Tweetデータのリスト
        """
        if not self.browser:
            self._setup_browser()
        
        if use_search:
            return self._get_tweets_by_search(username, since, until, days_per_chunk)
        else:
            return self._get_tweets_by_scroll(username)

    def _get_tweets_by_scroll(self, username: str) -> List[Dict]:
        """プロフィール画面をスクロールして取得"""
        url = f"https://twitter.com/{username}"
        logger.info(f"ユーザーページにアクセス: {url}")
        
        try:
            self.page.goto(url, wait_until="domcontentloaded")
            time.sleep(self.config.ACTION_DELAY)
            self._wait_for_page_load()
            
            # 非公開アカウントまたは存在しないアカウントのチェック
            page_text = self.page.inner_text('body')
            if 'このアカウントは存在しません' in page_text or 'This account doesn\'t exist' in page_text:
                raise ValueError(f"アカウント '{username}' は存在しません")
            if 'このアカウントは非公開です' in page_text or 'This account is private' in page_text:
                raise ValueError(f"アカウント '{username}' は非公開です。フォローしていないとTweetを取得できません")
            
            # スクロールしながらTweetを取得
            seen_tweet_ids = set()
            scroll_count = 0
            no_new_tweets_count = 0
            
            with tqdm(desc="Tweet取得中", unit="件") as pbar:
                while True:
                    # 現在のページからTweetを抽出
                    new_tweets = self._extract_tweets()
                    
                    # 新しいTweetのみを追加
                    added_count = 0
                    for tweet in new_tweets:
                        tweet_id = tweet.get('tweet_id')
                        if tweet_id and tweet_id not in seen_tweet_ids:
                            self.tweets.append(tweet)
                            seen_tweet_ids.add(tweet_id)
                            added_count += 1
                    
                    pbar.update(added_count)
                    pbar.set_postfix({"取得済み": len(self.tweets)})
                    
                    # 新しいTweetがなければカウント
                    if added_count == 0:
                        no_new_tweets_count += 1
                        if no_new_tweets_count >= 3:
                            logger.info("新しいTweetが見つかりません。取得を終了します。")
                            break
                    else:
                        no_new_tweets_count = 0
                    
                    # 最大Tweet数チェック
                    if self.config.MAX_TWEETS > 0 and len(self.tweets) >= self.config.MAX_TWEETS:
                        logger.info(f"最大Tweet数({self.config.MAX_TWEETS})に達しました。")
                        break

                    # レートリミット/エラーページの検知
                    if self._is_rate_limited():
                        self._handle_rate_limit()
                        # ページを再読み込みして続行
                        try:
                            self.page.reload(wait_until="domcontentloaded")
                            self._wait_for_page_load()
                        except Exception as e:
                            logger.warning(f"再読み込み中にエラー: {e}")
                        continue
                    
                    # スクロール
                    self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(self.config.SCROLL_DELAY)
                    
                    scroll_count += 1
                    # 100回スクロールごとに待機時間を長くする
                    if scroll_count % 100 == 0:
                        logger.info(f"{scroll_count}回スクロールしました。少し待機します...")
                        time.sleep(5)
            
            logger.info(f"合計 {len(self.tweets)} 件のTweetを取得しました")
            return self.tweets
            
        except Exception as e:
            logger.error(f"Tweet取得中にエラーが発生: {e}", exc_info=True)
            raise

    def _get_tweets_by_search(
        self,
        username: str,
        since: Optional[str],
        until: Optional[str],
        days_per_chunk: int,
    ) -> List[Dict]:
        """検索クエリで期間分割しながら取得"""
        # デフォルト期間: 1年前から今日まで
        today = datetime.utcnow().date()
        default_since = today - timedelta(days=365)
        start_date = datetime.strptime(since, "%Y-%m-%d").date() if since else default_since
        end_date = datetime.strptime(until, "%Y-%m-%d").date() if until else today
        if start_date >= end_date:
            raise ValueError("since は until より過去の日付にしてください")

        ranges = self._generate_date_ranges(start_date, end_date, days_per_chunk)

        seen_tweet_ids = set()
        with tqdm(desc="検索で取得中", unit="件") as pbar:
            for since_d, until_d in ranges:
                query = f"from:{username} since:{since_d} until:{until_d}"
                search_url = f"https://twitter.com/search?q={quote_plus(query)}&src=typed_query&f=live"
                logger.info(f"検索で取得: {since_d} - {until_d}")
                try:
                    self.page.goto(search_url, wait_until="domcontentloaded")
                    time.sleep(self.config.ACTION_DELAY)
                    self._wait_for_page_load()

                    no_new_tweets_count = 0
                    # 各チャンクでスクロール上限（例: 50回）を設定
                    scroll_limit = 50
                    for _ in range(scroll_limit):
                        new_tweets = self._extract_tweets()
                        added = 0
                        for tweet in new_tweets:
                            tweet_id = tweet.get("tweet_id")
                            if tweet_id and tweet_id not in seen_tweet_ids:
                                self.tweets.append(tweet)
                                seen_tweet_ids.add(tweet_id)
                                added += 1
                        pbar.update(added)
                        pbar.set_postfix({"取得済み": len(self.tweets)})

                        if added == 0:
                            no_new_tweets_count += 1
                            if no_new_tweets_count >= 3:
                                break
                        else:
                            no_new_tweets_count = 0

                        if self.config.MAX_TWEETS > 0 and len(self.tweets) >= self.config.MAX_TWEETS:
                            logger.info(f"最大Tweet数({self.config.MAX_TWEETS})に達しました。")
                            return self.tweets

                        if self._is_rate_limited():
                            self._handle_rate_limit()
                            try:
                                self.page.reload(wait_until="domcontentloaded")
                                self._wait_for_page_load()
                            except Exception as e:
                                logger.warning(f"再読み込み中にエラー: {e}")
                            continue

                        # スクロール
                        self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(self.config.SCROLL_DELAY)
                except Exception as e:
                    logger.error(f"検索チャンク取得中にエラー: {e}", exc_info=True)
                    continue

        logger.info(f"合計 {len(self.tweets)} 件のTweetを取得しました（検索モード）")
        return self.tweets

    def _generate_date_ranges(self, start: datetime.date, end: datetime.date, days: int):
        """[start, end) を days 日ごとに区切って返す"""
        ranges = []
        current = start
        delta = timedelta(days=days)
        while current < end:
            nxt = min(current + delta, end)
            ranges.append((current.isoformat(), nxt.isoformat()))
            current = nxt
        return ranges
    
    def _extract_tweets(self) -> List[Dict]:
        """現在のページからTweetを抽出"""
        tweets = []
        
        try:
            # Tweet要素を取得
            # TwitterのHTML構造に基づくセレクター
            tweet_elements = self.page.query_selector_all('article[data-testid="tweet"]')
            
            for element in tweet_elements:
                try:
                    tweet = self._parse_tweet_element(element)
                    if tweet:
                        tweets.append(tweet)
                except Exception as e:
                    logger.debug(f"Tweet解析エラー: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Tweet抽出エラー: {e}")
        
        return tweets
    
    def _parse_tweet_element(self, element) -> Optional[Dict]:
        """Tweet要素をパース"""
        try:
            # Tweet IDを取得（URLから）
            tweet_link = element.query_selector('a[href*="/status/"]')
            if not tweet_link:
                return None
            
            href = tweet_link.get_attribute('href')
            tweet_id_match = re.search(r'/status/(\d+)', href)
            if not tweet_id_match:
                return None
            tweet_id = tweet_id_match.group(1)
            
            # テキストを取得
            text_element = element.query_selector('div[data-testid="tweetText"]')
            text = text_element.inner_text() if text_element else ""
            
            # 投稿日時を取得
            time_element = element.query_selector('time')
            created_at = time_element.get_attribute('datetime') if time_element else ""
            
            # メトリクスを取得
            metrics = self._extract_metrics(element)
            
            # メディアを取得
            media = self._extract_media(element, tweet_id)
            
            tweet = {
                'tweet_id': tweet_id,
                'created_at': created_at,
                'text': text,
                'author_username': self._extract_username(element),
                'public_metrics': metrics,
                'media': media,
                'url': f"https://twitter.com{href}"
            }
            
            return tweet
            
        except Exception as e:
            logger.debug(f"Tweet要素のパースエラー: {e}")
            return None
    
    def _extract_username(self, element) -> str:
        """ユーザー名を抽出"""
        try:
            user_link = element.query_selector('div[data-testid="User-Name"] a')
            if user_link:
                href = user_link.get_attribute('href')
                if href:
                    return href.lstrip('/')
        except:
            pass
        return ""
    
    def _extract_metrics(self, element) -> Dict:
        """メトリクス（いいね数、リツイート数など）を抽出"""
        metrics = {
            'like_count': 0,
            'retweet_count': 0,
            'reply_count': 0,
            'quote_count': 0
        }
        
        try:
            # いいね数
            like_element = element.query_selector('button[data-testid="like"]')
            if like_element:
                like_text = like_element.inner_text()
                metrics['like_count'] = self._parse_number(like_text)
            
            # リツイート数
            retweet_element = element.query_selector('button[data-testid="retweet"]')
            if retweet_element:
                retweet_text = retweet_element.inner_text()
                metrics['retweet_count'] = self._parse_number(retweet_text)
            
            # リプライ数
            reply_element = element.query_selector('button[data-testid="reply"]')
            if reply_element:
                reply_text = reply_element.inner_text()
                metrics['reply_count'] = self._parse_number(reply_text)
                
        except Exception as e:
            logger.debug(f"メトリクス抽出エラー: {e}")
        
        return metrics
    
    def _parse_number(self, text: str) -> int:
        """テキストから数値を抽出（K, M表記に対応）"""
        if not text:
            return 0
        
        text = text.strip().upper()
        if not text or text == '0':
            return 0
        
        # K, M表記を処理
        multiplier = 1
        if 'K' in text:
            multiplier = 1000
            text = text.replace('K', '')
        elif 'M' in text:
            multiplier = 1000000
            text = text.replace('M', '')
        
        try:
            number = float(text.replace(',', ''))
            return int(number * multiplier)
        except:
            return 0
    
    def _extract_media(self, element, tweet_id: str) -> List[Dict]:
        """メディア（画像、動画）を抽出"""
        media_list = []
        
        try:
            # 画像を取得
            img_elements = element.query_selector_all('img[src*="pbs.twimg.com"]')
            for idx, img in enumerate(img_elements):
                src = img.get_attribute('src')
                if src and 'profile_images' not in src:  # プロフィール画像を除外
                    media_list.append({
                        'type': 'photo',
                        'url': src,
                        'media_index': idx
                    })
            
            # 動画を取得
            video_elements = element.query_selector_all('video')
            for idx, video in enumerate(video_elements):
                src = video.get_attribute('src')
                if src:
                    media_list.append({
                        'type': 'video',
                        'url': src,
                        'media_index': idx
                    })
                    
        except Exception as e:
            logger.debug(f"メディア抽出エラー: {e}")
        
        return media_list

    def _is_rate_limited(self) -> bool:
        """429/問題発生ページを検知"""
        try:
            body_text = self.page.inner_text("body")
            keywords = [
                "Too Many Requests",
                "429",
                "問題が発生しました",
                "再読み込みしてください",
                "やりなおす",
            ]
            return any(k in body_text for k in keywords)
        except Exception:
            return False

    def _handle_rate_limit(self):
        """レートリミット検知時の待機"""
        logger.warning(f"429/レートリミットを検知。{self.rate_limit_wait}秒（約{self.rate_limit_wait//60}分）待機します。")
        time.sleep(self.rate_limit_wait)
        # 連続で当たる場合は待機時間を伸ばす（上限1時間）
        self.rate_limit_wait = min(self.rate_limit_wait * 2, 3600)
    
    def close(self):
        """ブラウザを閉じる"""
        if self.context:
            self.context.close()
            logger.info("ブラウザコンテキストを閉じました")
        elif self.browser:
            self.browser.close()
            logger.info("ブラウザを閉じました")
        if self.playwright:
            self.playwright.stop()

