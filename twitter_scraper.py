"""Twitter Tweet取得モジュール"""
import time
import json
import re
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable
from urllib.parse import quote_plus
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
import logging
from tqdm import tqdm

from config import Config
from media_only import is_target_author

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
        # メディアダウンロード対象の作者フィルタ（RT等で別作者になるケース対策）
        self.media_author_filter: Optional[str] = None
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
    
    def _is_login_page(self) -> bool:
        """ログイン画面かどうかを判定"""
        try:
            url = self.page.url
            if '/i/flow/login' in url or '/login' in url:
                return True
            
            # ページタイトルやテキストで判定
            page_text = self.page.inner_text('body').lower()
            title = self.page.title().lower()
            
            login_indicators = [
                'log in',
                'ログイン',
                'sign in',
                'sign in to twitter',
                'twitterにログイン'
            ]
            
            if any(indicator in title or indicator in page_text for indicator in login_indicators):
                return True
            
            # ログインボタンの存在確認
            login_button = self.page.query_selector('button[data-testid="loginButton"]')
            if login_button:
                return True
                
        except:
            pass
        
        return False
    
    def _handle_login_redirect(self) -> bool:
        """ログイン画面に遷移された場合の処理
        
        Returns:
            True: ログイン成功またはログイン画面ではない
            False: ログイン失敗（手動ログインが必要）
        """
        if not self._is_login_page():
            return True
        
        logger.warning("ログイン画面に遷移されました。自動ログインを試行します...")
        
        # まずCookieを試す
        if self.config.TWITTER_COOKIES:
            try:
                cookies = self._parse_cookies(self.config.TWITTER_COOKIES)
                self.context.add_cookies(cookies)
                logger.info("Cookieを再設定しました")
                
                # リロード
                self.page.reload(wait_until="domcontentloaded")
                time.sleep(self.config.ACTION_DELAY)
                self._wait_for_page_load()
                
                # 再度チェック
                if not self._is_login_page():
                    logger.info("Cookieによる自動ログインに成功しました")
                    return True
            except Exception as e:
                logger.warning(f"Cookie再設定中にエラー: {e}")
        
        # Cookieが効かない場合、ユーザー名+パスワードでログインを試行
        if self.config.TWITTER_USERNAME and self.config.TWITTER_PASSWORD:
            return self._login_with_credentials()
        else:
            logger.warning("TWITTER_COOKIESもユーザー名+パスワードも設定されていません。")
            logger.warning("USER_DATA_DIRを使用している場合は、launch_browser.pyでログインしてください。")
            return False
    
    def _login_with_credentials(self) -> bool:
        """ユーザー名とパスワードでログイン
        
        Returns:
            True: ログイン成功
            False: ログイン失敗
        """
        try:
            logger.info("ユーザー名+パスワードでログインを試行します...")
            
            # ログインページに移動
            self.page.goto("https://twitter.com/i/flow/login", wait_until="domcontentloaded")
            time.sleep(self.config.ACTION_DELAY)
            self._wait_for_page_load()
            
            # ユーザー名/メールアドレス入力欄を探す
            # Twitterのログインフォームは複数のステップがある場合がある
            username_input = None
            
            # 複数のセレクターを試す
            selectors = [
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[type="text"]',
                'input[data-testid="ocfEnterTextTextInput"]'
            ]
            
            for selector in selectors:
                try:
                    username_input = self.page.query_selector(selector)
                    if username_input and username_input.is_visible():
                        break
                except:
                    continue
            
            if not username_input:
                logger.error("ユーザー名入力欄が見つかりません")
                return False
            
            # ユーザー名を入力
            username_input.fill(self.config.TWITTER_USERNAME)
            time.sleep(1)
            
            # 次へボタンをクリック
            next_button = self.page.query_selector('button[type="button"]:has-text("次へ")') or \
                         self.page.query_selector('button[type="button"]:has-text("Next")') or \
                         self.page.query_selector('div[role="button"]:has-text("次へ")') or \
                         self.page.query_selector('div[role="button"]:has-text("Next")')
            
            if next_button:
                next_button.click()
                time.sleep(2)
                self._wait_for_page_load()
            
            # パスワード入力欄を探す
            password_input = None
            password_selectors = [
                'input[name="password"]',
                'input[type="password"]',
                'input[autocomplete="current-password"]'
            ]
            
            for selector in password_selectors:
                try:
                    password_input = self.page.query_selector(selector)
                    if password_input and password_input.is_visible():
                        break
                except:
                    continue
            
            if not password_input:
                logger.warning("パスワード入力欄が見つかりません。CAPTCHAや2FAが必要な可能性があります。")
                return False
            
            # パスワードを入力
            password_input.fill(self.config.TWITTER_PASSWORD)
            time.sleep(1)
            
            # ログインボタンをクリック
            login_button = self.page.query_selector('button[data-testid="LoginForm_Login_Button"]') or \
                          self.page.query_selector('button[type="submit"]') or \
                          self.page.query_selector('div[role="button"]:has-text("ログイン")') or \
                          self.page.query_selector('div[role="button"]:has-text("Log in")')
            
            if login_button:
                login_button.click()
                time.sleep(3)
                self._wait_for_page_load()
            else:
                # Enterキーで送信を試す
                password_input.press("Enter")
                time.sleep(3)
                self._wait_for_page_load()
            
            # ログイン成功を確認
            if not self._is_login_page():
                logger.info("ユーザー名+パスワードによるログインに成功しました")
                return True
            else:
                # CAPTCHAや2FAが必要な可能性
                page_text = self.page.inner_text('body').lower()
                if 'captcha' in page_text or 'verify' in page_text or '認証' in page_text:
                    logger.warning("CAPTCHAまたは2FA認証が必要です。手動でログインしてください。")
                else:
                    logger.warning("ログインに失敗しました。ユーザー名またはパスワードが間違っている可能性があります。")
                return False
                
        except Exception as e:
            logger.error(f"ログイン処理中にエラー: {e}", exc_info=True)
            return False
    
    def get_user_tweets(
        self,
        username: str,
        use_search: bool = False,
        since: Optional[str] = None,
        until: Optional[str] = None,
        days_per_chunk: int = 7,
        on_tweet_fetched: Optional[Callable[[Dict], None]] = None,
        parallel_chunks: Optional[bool] = None,
    ) -> List[Dict]:
        """指定ユーザーのTweetを取得（他人のアカウントも可）
        
        Args:
            username: Twitterユーザー名（@なし）
            use_search: Trueの場合、検索クエリで期間を分割して取得
            since: 取得開始日 (YYYY-MM-DD)。未指定なら1年前をデフォルトに設定
            until: 取得終了日 (YYYY-MM-DD)。未指定なら今日
            days_per_chunk: 検索モード時のチャンク日数
            on_tweet_fetched: ツイート取得時に呼ばれるコールバック関数
            parallel_chunks: 検索モード時にチャンクを並行処理するか（NoneならConfigを使用）
            
        Returns:
            Tweetデータのリスト
        """
        if not self.browser:
            self._setup_browser()
        
        if use_search:
            return self._get_tweets_by_search(username, since, until, days_per_chunk, on_tweet_fetched, parallel_chunks=parallel_chunks)
        else:
            return self._get_tweets_by_scroll(username, on_tweet_fetched)

    def _get_tweets_by_scroll(self, username: str, on_tweet_fetched: Optional[Callable[[Dict], None]] = None) -> List[Dict]:
        """プロフィール画面をスクロールして取得"""
        url = f"https://twitter.com/{username}"
        logger.info(f"ユーザーページにアクセス: {url}")
        
        try:
            # ユーザーページへのアクセス（リトライ対応）
            max_retries = 3
            retry_count = 0
            access_success = False
            
            while retry_count < max_retries and not access_success:
                self.page.goto(url, wait_until="domcontentloaded")
                time.sleep(self.config.ACTION_DELAY)
                self._wait_for_page_load()
                
                # 429エラーチェック
                if self._is_rate_limited():
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"ユーザーページアクセス時に429エラーを検知。リトライ {retry_count}/{max_retries}")
                        # まず短い待機で再試行（最初の1-2回は短い待機）
                        if retry_count <= 2:
                            wait_time = 10  # 10秒待機
                            logger.info(f"{wait_time}秒待機して再試行します...")
                            time.sleep(wait_time)
                        else:
                            # 3回目以降は長い待機
                            self._handle_rate_limit()
                        continue
                    else:
                        logger.error("ユーザーページアクセスのリトライ上限に達しました。処理を中断します。")
                        raise ValueError("429エラーが継続しています。しばらく待ってから再試行してください。")
                
                # ログイン画面チェックと自動ログイン
                if not self._handle_login_redirect():
                    logger.error("ログインに失敗しました。処理を中断します。")
                    raise ValueError("ログインが必要です。launch_browser.pyでログインするか、TWITTER_COOKIESを更新してください。")
                
                # 非公開アカウントまたは存在しないアカウントのチェック
                page_text = self.page.inner_text('body')
                if 'このアカウントは存在しません' in page_text or 'This account doesn\'t exist' in page_text:
                    raise ValueError(f"アカウント '{username}' は存在しません")
                if 'このアカウントは非公開です' in page_text or 'This account is private' in page_text:
                    raise ValueError(f"アカウント '{username}' は非公開です。フォローしていないとTweetを取得できません")
                
                access_success = True
            
            if not access_success:
                raise ValueError("ユーザーページへのアクセスに失敗しました。")
            
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
                            
                            # コールバックを呼び出し（メディアダウンロード用）
                            if on_tweet_fetched:
                                on_tweet_fetched(tweet)
                    
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
                        logger.warning("スクロール中に429エラーを検知。ユーザーページを再試行します。")
                        self._handle_rate_limit()
                        
                        # ユーザーページに再度アクセス（最大3回リトライ）
                        retry_count = 0
                        max_retries = 3
                        retry_success = False
                        
                        while retry_count < max_retries and not retry_success:
                            retry_count += 1
                            try:
                                self.page.goto(url, wait_until="domcontentloaded")
                                time.sleep(self.config.ACTION_DELAY)
                                self._wait_for_page_load()
                                
                                # 429エラーがまだ続いているかチェック
                                if self._is_rate_limited():
                                    if retry_count < max_retries:
                                        logger.warning(f"再試行 {retry_count}/{max_retries} でも429エラーが続いています。待機後に再試行します。")
                                        self._handle_rate_limit()
                                        continue
                                    else:
                                        logger.error("再試行上限に達しました。処理を中断します。")
                                        raise ValueError("429エラーが継続しています。しばらく待ってから再試行してください。")
                                
                                # ログイン画面チェック
                                if not self._handle_login_redirect():
                                    logger.error("再試行時にログインに失敗しました。処理を中断します。")
                                    raise ValueError("ログインが必要です。launch_browser.pyでログインするか、TWITTER_COOKIESを更新してください。")
                                
                                retry_success = True
                                logger.info(f"再試行 {retry_count} でユーザーページへのアクセスに成功しました。")
                                
                            except ValueError:
                                raise  # ValueErrorはそのまま再スロー
                            except Exception as e:
                                if retry_count < max_retries:
                                    logger.warning(f"再試行 {retry_count}/{max_retries} 中にエラー: {e}。再試行します。")
                                    time.sleep(5)  # 短い待機時間
                                    continue
                                else:
                                    logger.error(f"再試行上限に達しました。エラー: {e}")
                                    raise
                        
                        if not retry_success:
                            raise ValueError("ユーザーページへの再アクセスに失敗しました。")
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
        on_tweet_fetched: Optional[Callable[[Dict], None]] = None,
        parallel_chunks: Optional[bool] = None,
    ) -> List[Dict]:
        """検索クエリで期間分割しながら取得
        
        Args:
            parallel_chunks: Trueの場合、複数のチャンクを並行処理
                            Noneの場合は設定ファイルの値を使用（デフォルト: False）
        """
        # デフォルトは順次処理（ログイン状態を保持するため）
        if parallel_chunks is None:
            parallel_chunks = getattr(self.config, 'SEARCH_PARALLEL', False)
        # デフォルト期間: 1年前から今日まで
        today = datetime.utcnow().date()
        default_since = today - timedelta(days=365)
        start_date = datetime.strptime(since, "%Y-%m-%d").date() if since else default_since
        end_date = datetime.strptime(until, "%Y-%m-%d").date() if until else today
        if start_date >= end_date:
            raise ValueError("since は until より過去の日付にしてください")

        ranges = self._generate_date_ranges(start_date, end_date, days_per_chunk)
        
        if parallel_chunks and len(ranges) > 1:
            # 並行処理でチャンクを取得
            return self._get_tweets_by_search_parallel(
                username, ranges, on_tweet_fetched
            )
        else:
            # 順次処理（downloaderはself._current_downloaderから取得）
            return self._get_tweets_by_search_sequential(
                username, ranges, on_tweet_fetched, downloader=self._current_downloader
            )
    
    def _get_tweets_by_search_sequential(
        self,
        username: str,
        ranges: List[tuple],
        on_tweet_fetched: Optional[Callable[[Dict], None]] = None,
        downloader: Optional[object] = None,
    ) -> List[Dict]:
        """検索チャンクを順次処理
        
        Args:
            downloader: MediaDownloaderインスタンス（チャンクごとにメディアダウンロードする場合）
        """
        seen_tweet_ids = set()
        with tqdm(desc="検索で取得中", unit="件") as pbar:
            for since_d, until_d in ranges:
                chunk_tweets = []  # このチャンクで取得したTweet
                query = f"from:{username} since:{since_d} until:{until_d}"
                search_url = f"https://twitter.com/search?q={quote_plus(query)}&src=typed_query&f=live"
                logger.info(f"検索で取得: {since_d} - {until_d}")
                try:
                    # 検索URLへのアクセス（リトライ対応）
                    max_retries = 3
                    retry_count = 0
                    search_success = False
                    
                    while retry_count < max_retries and not search_success:
                        self.page.goto(search_url, wait_until="domcontentloaded")
                        time.sleep(self.config.ACTION_DELAY)
                        self._wait_for_page_load()
                        
                        # 429エラーチェック
                        if self._is_rate_limited():
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"検索アクセス時に429エラーを検知。リトライ {retry_count}/{max_retries}")
                                # まず短い待機で再試行（最初の1-2回は短い待機）
                                if retry_count <= 2:
                                    wait_time = 10  # 10秒待機
                                    logger.info(f"{wait_time}秒待機して再試行します...")
                                    time.sleep(wait_time)
                                else:
                                    # 3回目以降は長い待機
                                    self._handle_rate_limit()
                                continue
                            else:
                                logger.error(f"検索アクセスのリトライ上限に達しました。チャンク {since_d} - {until_d} をスキップします。")
                                break
                        
                        # ログイン画面チェックと自動ログイン
                        if not self._handle_login_redirect():
                            logger.warning(f"ログインに失敗しました。チャンク {since_d} - {until_d} をスキップします。")
                            break
                        
                        search_success = True
                    
                    if not search_success:
                        continue

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
                                chunk_tweets.append(tweet)  # チャンク用にも保存
                                seen_tweet_ids.add(tweet_id)
                                added += 1
                                
                                # コールバックを呼び出し（メディアダウンロード用）
                                if on_tweet_fetched:
                                    on_tweet_fetched(tweet)
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
                            # チャンクごとのメディアダウンロード
                            if downloader and chunk_tweets:
                                logger.info(f"チャンク {since_d} - {until_d} のメディアをダウンロード中...")
                                tweets_for_media = chunk_tweets
                                if self.media_author_filter:
                                    tweets_for_media = [t for t in chunk_tweets if is_target_author(t, self.media_author_filter)]
                                if tweets_for_media:
                                    downloaded_chunk = downloader.download_media(tweets_for_media)
                                    downloaded_map = {t.get("tweet_id"): t for t in downloaded_chunk}
                                    for tweet in self.tweets:
                                        tid = tweet.get("tweet_id")
                                        if tid in downloaded_map:
                                            tweet["media"] = downloaded_map[tid].get("media", [])
                            return self.tweets

                        if self._is_rate_limited():
                            logger.warning("スクロール中に429エラーを検知。検索を再試行します。")
                            self._handle_rate_limit()
                            
                            # 検索URLに再度アクセス（最大3回リトライ）
                            retry_count = 0
                            max_retries = 3
                            retry_success = False
                            
                            while retry_count < max_retries and not retry_success:
                                retry_count += 1
                                try:
                                    # 検索URLに再度アクセス
                                    self.page.goto(search_url, wait_until="domcontentloaded")
                                    time.sleep(self.config.ACTION_DELAY)
                                    self._wait_for_page_load()
                                    
                                    # 429エラーがまだ続いているかチェック
                                    if self._is_rate_limited():
                                        if retry_count < max_retries:
                                            logger.warning(f"再試行 {retry_count}/{max_retries} でも429エラーが続いています。待機後に再試行します。")
                                            self._handle_rate_limit()
                                            continue
                                        else:
                                            logger.error(f"再試行上限に達しました。チャンク {since_d} - {until_d} をスキップします。")
                                            break
                                    
                                    # ログイン画面チェック
                                    if not self._handle_login_redirect():
                                        logger.warning(f"再試行時にログインに失敗しました。チャンク {since_d} - {until_d} をスキップします。")
                                        break
                                    
                                    retry_success = True
                                    logger.info(f"再試行 {retry_count} で検索へのアクセスに成功しました。")
                                    
                                except Exception as e:
                                    if retry_count < max_retries:
                                        logger.warning(f"再試行 {retry_count}/{max_retries} 中にエラー: {e}。再試行します。")
                                        time.sleep(5)  # 短い待機時間
                                        continue
                                    else:
                                        logger.warning(f"再試行上限に達しました。チャンク {since_d} - {until_d} をスキップします。エラー: {e}")
                                        break
                            
                            if not retry_success:
                                # リトライに失敗した場合はこのチャンクをスキップ
                                break
                            continue

                        # スクロール
                        self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        time.sleep(self.config.SCROLL_DELAY)
                    
                    # チャンクごとのメディアダウンロード
                    if downloader and chunk_tweets:
                        logger.info(f"チャンク {since_d} - {until_d} のメディアをダウンロード中... ({len(chunk_tweets)}件)")
                        tweets_for_media = chunk_tweets
                        if self.media_author_filter:
                            tweets_for_media = [t for t in chunk_tweets if is_target_author(t, self.media_author_filter)]
                        if tweets_for_media:
                            downloaded_chunk = downloader.download_media(tweets_for_media)
                            downloaded_map = {t.get("tweet_id"): t for t in downloaded_chunk}
                            for tweet in self.tweets:
                                tid = tweet.get("tweet_id")
                                if tid in downloaded_map:
                                    tweet["media"] = downloaded_map[tid].get("media", [])
                        
                except Exception as e:
                    logger.error(f"検索チャンク取得中にエラー: {e}", exc_info=True)
                    continue

        logger.info(f"合計 {len(self.tweets)} 件のTweetを取得しました（検索モード）")
        return self.tweets
    
    def _get_tweets_by_search_parallel(
        self,
        username: str,
        ranges: List[tuple],
        on_tweet_fetched: Optional[Callable[[Dict], None]] = None,
    ) -> List[Dict]:
        """検索チャンクを並行処理"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        
        seen_tweet_ids = set()
        seen_lock = threading.Lock()
        all_tweets = []
        tweets_lock = threading.Lock()
        
        def process_chunk(since_d: str, until_d: str) -> List[Dict]:
            """単一チャンクを処理"""
            chunk_tweets = []
            chunk_seen_ids = set()
            
            # 各スレッドで独立したPlaywrightインスタンスを作成
            # 同じuser_data_dirを複数インスタンスで使うと競合するため、通常のブラウザ起動を使用
            playwright = None
            context = None
            browser = None
            
            try:
                playwright = sync_playwright().start()
                launch_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--autoplay-policy=no-user-gesture-required',
                    '--use-gl=angle',
                ]
                
                # 並行処理では一時ディレクトリを使用（user_data_dirの競合を回避）
                # ただし、Cookieは設定する
                launch_opts = dict(
                    headless=self.config.HEADLESS,
                    args=launch_args,
                )
                if self.config.USE_SYSTEM_CHROME:
                    launch_opts["channel"] = "chrome"
                
                browser = playwright.chromium.launch(**launch_opts)
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = context.new_page()
                
                # Cookieを設定（user_data_dirを使わない場合）
                if self.config.TWITTER_COOKIES:
                    cookies = self._parse_cookies(self.config.TWITTER_COOKIES)
                    context.add_cookies(cookies)
                
                query = f"from:{username} since:{since_d} until:{until_d}"
                search_url = f"https://twitter.com/search?q={quote_plus(query)}&src=typed_query&f=live"
                logger.info(f"[並行] 検索で取得: {since_d} - {until_d}")
                
                # 検索URLへのアクセス（リトライ対応）
                max_retries = 3
                retry_count = 0
                search_success = False
                
                while retry_count < max_retries and not search_success:
                    page.goto(search_url, wait_until="domcontentloaded")
                    time.sleep(self.config.ACTION_DELAY)
                    
                    # ページ読み込み待機
                    try:
                        page.wait_for_load_state("networkidle", timeout=30000)
                    except:
                        pass
                    
                    # 429エラーチェック
                    try:
                        body_text = page.inner_text("body")
                        if any(k in body_text for k in ["Too Many Requests", "429", "問題が発生しました"]):
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"[並行] 検索アクセス時に429エラーを検知。リトライ {retry_count}/{max_retries}: {since_d} - {until_d}")
                                # まず短い待機で再試行（最初の1-2回は短い待機）
                                if retry_count <= 2:
                                    wait_time = 10  # 10秒待機
                                    logger.info(f"[並行] {wait_time}秒待機して再試行します...")
                                    time.sleep(wait_time)
                                else:
                                    # 3回目以降は長い待機
                                    time.sleep(60)  # 1分待機
                                continue
                            else:
                                logger.error(f"[並行] 検索アクセスのリトライ上限に達しました。チャンク {since_d} - {until_d} をスキップします。")
                                break
                    except:
                        pass
                    
                    search_success = True
                
                if not search_success:
                    return []
                
                no_new_tweets_count = 0
                scroll_limit = 50
                
                for _ in range(scroll_limit):
                    # Tweet抽出
                    tweet_elements = page.query_selector_all('article[data-testid="tweet"]')
                    new_tweets = []
                    for element in tweet_elements:
                        try:
                            tweet = self._parse_tweet_element(element)
                            if tweet:
                                new_tweets.append(tweet)
                        except:
                            continue
                    
                    added = 0
                    for tweet in new_tweets:
                        tweet_id = tweet.get("tweet_id")
                        if tweet_id and tweet_id not in chunk_seen_ids:
                            chunk_seen_ids.add(tweet_id)
                            
                            # グローバルな重複チェック
                            with seen_lock:
                                if tweet_id not in seen_tweet_ids:
                                    seen_tweet_ids.add(tweet_id)
                                    chunk_tweets.append(tweet)
                                    added += 1
                                    
                                    # コールバックを呼び出し
                                    if on_tweet_fetched:
                                        on_tweet_fetched(tweet)
                    
                    if added == 0:
                        no_new_tweets_count += 1
                        if no_new_tweets_count >= 3:
                            break
                    else:
                        no_new_tweets_count = 0
                    
                    if self.config.MAX_TWEETS > 0:
                        with tweets_lock:
                            if len(all_tweets) >= self.config.MAX_TWEETS:
                                break
                    
                    # レートリミットチェック
                    try:
                        body_text = page.inner_text("body")
                        if any(k in body_text for k in ["Too Many Requests", "429", "問題が発生しました"]):
                            logger.warning(f"[並行] スクロール中に429エラーを検知。検索を再試行します: {since_d} - {until_d}")
                            time.sleep(60)  # 1分待機
                            
                            # 検索URLに再度アクセス（最大3回リトライ）
                            retry_count = 0
                            max_retries = 3
                            retry_success = False
                            
                            while retry_count < max_retries and not retry_success:
                                retry_count += 1
                                try:
                                    # 検索URLに再度アクセス
                                    page.goto(search_url, wait_until="domcontentloaded")
                                    time.sleep(self.config.ACTION_DELAY)
                                    try:
                                        page.wait_for_load_state("networkidle", timeout=30000)
                                    except:
                                        pass
                                    
                                    # 429エラーがまだ続いているかチェック
                                    try:
                                        body_text = page.inner_text("body")
                                        if any(k in body_text for k in ["Too Many Requests", "429", "問題が発生しました"]):
                                            if retry_count < max_retries:
                                                logger.warning(f"[並行] 再試行 {retry_count}/{max_retries} でも429エラーが続いています。待機後に再試行します: {since_d} - {until_d}")
                                                time.sleep(60)  # 1分待機
                                                continue
                                            else:
                                                logger.error(f"[並行] 再試行上限に達しました。チャンク {since_d} - {until_d} をスキップします。")
                                                break
                                    except:
                                        pass  # チェックに失敗しても続行
                                    
                                    retry_success = True
                                    logger.info(f"[並行] 再試行 {retry_count} で検索へのアクセスに成功しました: {since_d} - {until_d}")
                                    
                                except Exception as e:
                                    if retry_count < max_retries:
                                        logger.warning(f"[並行] 再試行 {retry_count}/{max_retries} 中にエラー: {e}。再試行します: {since_d} - {until_d}")
                                        time.sleep(5)  # 短い待機時間
                                        continue
                                    else:
                                        logger.error(f"[並行] 再試行上限に達しました。チャンク {since_d} - {until_d} をスキップします。エラー: {e}")
                                        break
                            
                            if not retry_success:
                                # リトライに失敗した場合はこのチャンクを終了
                                break
                            continue
                    except:
                        pass
                    
                    # スクロール
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(self.config.SCROLL_DELAY)
                
                logger.info(f"[並行] 完了: {since_d} - {until_d} ({len(chunk_tweets)}件)")
                return chunk_tweets
                
            except Exception as e:
                logger.error(f"[並行] チャンク取得エラー ({since_d} - {until_d}): {e}", exc_info=True)
                return []
            finally:
                # リソースを確実にクリーンアップ
                try:
                    if context:
                        context.close()
                except:
                    pass
                try:
                    if browser:
                        browser.close()
                except:
                    pass
                try:
                    if playwright:
                        playwright.stop()
                except:
                    pass
        
        # 並行処理実行
        max_workers = min(3, len(ranges))  # 最大3並行
        logger.info(f"検索チャンクを並行処理します（{len(ranges)}チャンク、最大{max_workers}並行）")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_chunk, since_d, until_d): (since_d, until_d) 
                      for since_d, until_d in ranges}
            
            with tqdm(desc="検索で取得中（並行）", unit="件", total=len(ranges)) as pbar:
                for future in as_completed(futures):
                    since_d, until_d = futures[future]
                    try:
                        chunk_tweets = future.result()
                        with tweets_lock:
                            all_tweets.extend(chunk_tweets)
                            pbar.update(1)
                            pbar.set_postfix({"取得済み": len(all_tweets)})
                    except Exception as e:
                        logger.error(f"チャンク処理エラー ({since_d} - {until_d}): {e}")
                        pbar.update(1)
                    
                    if self.config.MAX_TWEETS > 0 and len(all_tweets) >= self.config.MAX_TWEETS:
                        logger.info(f"最大Tweet数({self.config.MAX_TWEETS})に達しました。")
                        break
        
        self.tweets = all_tweets
        logger.info(f"合計 {len(self.tweets)} 件のTweetを取得しました（検索モード・並行処理）")
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
                    # 要素が有効かどうかを確認（ページが再読み込みされた場合に備えて）
                    # 要素が無効な場合はスキップ
                    try:
                        # 要素がまだ有効かどうかを確認するために、簡単な操作を試す
                        element.query_selector('a')  # 軽量な操作で要素の有効性を確認
                    except Exception:
                        # 要素が無効になった場合はスキップ
                        continue
                    
                    tweet = self._parse_tweet_element(element)
                    if tweet:
                        tweets.append(tweet)
                except Exception as e:
                    # "Unable to adopt element handle from a different document" エラーは
                    # ページが再読み込みされた場合に発生するため、デバッグレベルでログを出す
                    error_msg = str(e)
                    if "Unable to adopt element handle" in error_msg or "different document" in error_msg:
                        logger.debug(f"Tweet解析エラー（要素が無効）: {e}")
                    else:
                        logger.debug(f"Tweet解析エラー: {e}")
                    continue
                    
        except Exception as e:
            # 要素取得自体が失敗した場合のみエラーログを出す
            error_msg = str(e)
            if "Unable to adopt element handle" in error_msg or "different document" in error_msg:
                logger.debug(f"Tweet抽出エラー（ページが再読み込みされた可能性）: {e}")
            else:
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
            if not href:
                return None
            
            tweet_id_match = re.search(r'/status/(\d+)', href)
            if not tweet_id_match:
                return None
            tweet_id = tweet_id_match.group(1)
            
            # テキストを取得
            text = ""
            try:
                text_element = element.query_selector('div[data-testid="tweetText"]')
                if text_element:
                    text = text_element.inner_text()
            except Exception:
                pass  # テキスト取得に失敗しても続行
            
            # 投稿日時を取得
            created_at = ""
            try:
                time_element = element.query_selector('time')
                if time_element:
                    created_at = time_element.get_attribute('datetime') or ""
            except Exception:
                pass  # 日時取得に失敗しても続行
            
            # メトリクスを取得
            metrics = self._extract_metrics(element)
            
            # メディアを取得
            media = self._extract_media(element, tweet_id)
            
            # ユーザー名を取得
            author_username = self._extract_username(element)
            
            tweet = {
                'tweet_id': tweet_id,
                'created_at': created_at,
                'text': text,
                'author_username': author_username,
                'public_metrics': metrics,
                'media': media,
                'url': f"https://twitter.com{href}"
            }
            
            return tweet
            
        except Exception as e:
            # "Unable to adopt element handle from a different document" エラーは
            # ページが再読み込みされた場合に発生するため、デバッグレベルでログを出す
            error_msg = str(e)
            if "Unable to adopt element handle" in error_msg or "different document" in error_msg:
                logger.debug(f"Tweet要素のパースエラー（要素が無効）: {e}")
            else:
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
        except Exception:
            # 要素が無効でも続行（エラーログは出さない）
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
            try:
                like_element = element.query_selector('button[data-testid="like"]')
                if like_element:
                    like_text = like_element.inner_text()
                    metrics['like_count'] = self._parse_number(like_text)
            except Exception:
                pass  # 要素が無効でも続行
            
            # リツイート数
            try:
                retweet_element = element.query_selector('button[data-testid="retweet"]')
                if retweet_element:
                    retweet_text = retweet_element.inner_text()
                    metrics['retweet_count'] = self._parse_number(retweet_text)
            except Exception:
                pass  # 要素が無効でも続行
            
            # リプライ数
            try:
                reply_element = element.query_selector('button[data-testid="reply"]')
                if reply_element:
                    reply_text = reply_element.inner_text()
                    metrics['reply_count'] = self._parse_number(reply_text)
            except Exception:
                pass  # 要素が無効でも続行
                
        except Exception as e:
            # 要素が無効になった場合はデバッグログのみ
            error_msg = str(e)
            if "Unable to adopt element handle" not in error_msg and "different document" not in error_msg:
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
        seen_urls = set()
        
        try:
            # 画像を取得
            try:
                img_elements = element.query_selector_all('img[src*="pbs.twimg.com"]')
                for idx, img in enumerate(img_elements):
                    try:
                        src = img.get_attribute('src')
                        if src and 'profile_images' not in src:  # プロフィール画像を除外
                            # 動画ツイートの場合、サムネイル画像（ext_tw_video_thumb等）が混ざることがある
                            # これをphoto扱いすると「動画なのにphoto」問題が発生するため、別タイプで保持する
                            media_type = 'photo'
                            if any(k in src for k in ["ext_tw_video_thumb", "amplify_video_thumb"]):
                                media_type = 'video_thumbnail'

                                # 可能なら実動画URLを拾ってvideoも追加する
                                try:
                                    video_src = self._resolve_video_from_api(tweet_id)
                                    if not video_src:
                                        html = element.inner_html() or ""
                                        video_src = self._pick_video_url_from_html(html)

                                    if video_src and not video_src.startswith("blob:") and video_src not in seen_urls:
                                        media_list.append({
                                            'type': 'video',
                                            'url': video_src,
                                            'media_index': idx,
                                            'thumbnail_url': src,
                                        })
                                        seen_urls.add(video_src)
                                except Exception:
                                    pass

                            if src not in seen_urls:
                                media_list.append({
                                    'type': media_type,
                                    'url': src,
                                    'media_index': idx
                                })
                                seen_urls.add(src)
                    except Exception:
                        continue  # 個別の画像要素が無効でも続行
            except Exception:
                pass  # 画像取得に失敗しても続行
            
            # 動画を取得
            try:
                video_elements = element.query_selector_all('video')
                for idx, video in enumerate(video_elements):
                    try:
                        # 1) video[src] を優先（稀に直MP4が入る）
                        src = video.get_attribute('src')

                        # 2) video > source[src] を探索（こちらの方が直URLになりやすい）
                        if not src or (isinstance(src, str) and src.startswith("blob:")):
                            try:
                                sources = video.query_selector_all("source")
                                for s in sources:
                                    s_src = s.get_attribute("src")
                                    if s_src and not s_src.startswith("blob:"):
                                        # Twitterの動画は video.twimg.com のMP4が多い
                                        src = s_src
                                        break
                            except Exception:
                                pass

                        # 3) element内HTMLから video.twimg.com のURLを正規表現で拾う（blob対策）
                        if not src or (isinstance(src, str) and src.startswith("blob:")):
                            try:
                                html = element.inner_html() or ""
                                src = self._pick_video_url_from_html(html)
                            except Exception:
                                pass

                        # 4) それでもダメなら Syndication API を試す（最も確実）
                        if not src or (isinstance(src, str) and src.startswith("blob:")):
                            src = self._resolve_video_from_api(tweet_id)

                        # URLが取得できた場合のみ追加
                        if src and not src.startswith("blob:") and src not in seen_urls:
                            media_list.append({
                                'type': 'video',
                                'url': src,
                                'media_index': idx
                            })
                            seen_urls.add(src)
                    except Exception:
                        continue  # 個別の動画要素が無効でも続行
            except Exception:
                pass  # 動画取得に失敗しても続行
                    
        except Exception as e:
            # 要素が無効になった場合はデバッグログのみ
            error_msg = str(e)
            if "Unable to adopt element handle" not in error_msg and "different document" not in error_msg:
                logger.debug(f"メディア抽出エラー: {e}")
        
        return media_list

    @staticmethod
    def _pick_video_url_from_html(html: str) -> Optional[str]:
        """HTML文字列から video.twimg.com の動画URLを拾う（MP4優先、次にWebM、最後にm3u8）"""
        if not html:
            return None
        # MP4優先。なければm3u8も拾う（DL側で扱い）
        mp4s = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.mp4[^\"'\\s>]*", html)
        if mp4s:
            return mp4s[0]
        webms = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.webm[^\"'\\s>]*", html)
        if webms:
            return webms[0]
        m3u8s = re.findall(r"https://video\.twimg\.com/[^\"'\\s>]+?\.m3u8[^\"'\\s>]*", html)
        if m3u8s:
            return m3u8s[0]
        return None

    def _resolve_video_from_api(self, tweet_id: str) -> Optional[str]:
        """Syndication APIを使って動画URLを取得（最も高画質なMP4/WebMを選択）"""
        url = f"https://cdn.syndication.twimg.com/tweet-result?id={tweet_id}&lang=en"
        try:
            # ログイン不要のエンドポイント
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                return None
            data = resp.json()
            
            video_info = None
            media_items = data.get("mediaDetails", []) or data.get("entities", {}).get("media", [])
            for m in media_items:
                v_info = m.get("video_info")
                if v_info:
                    video_info = v_info
                    break
            
            if not video_info:
                return None
                
            variants = video_info.get("variants", [])
            # 最もビットレートが高いMP4を優先。無ければWebM。最後にm3u8。
            mp4s = [v for v in variants if v.get("content_type") == "video/mp4" and v.get("url")]
            webms = [v for v in variants if v.get("content_type") == "video/webm" and v.get("url")]
            if mp4s:
                mp4s.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
                return mp4s[0].get("url")
            if webms:
                webms.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
                return webms[0].get("url")

            m3u8s = [v for v in variants if v.get("url") and ".m3u8" in v.get("url")]
            return m3u8s[0].get("url") if m3u8s else None
        except Exception:
            return None

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

