"""メインスクリプト"""
import argparse
import sys
import logging
from pathlib import Path

from typing import Dict
from config import Config
from twitter_scraper import TwitterScraper
from media_downloader import MediaDownloader
from data_saver import DataSaver


def setup_logging():
    """ログ設定"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # コンソール出力
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # ファイル出力
    file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # エラーログ
    error_handler = logging.FileHandler(Config.ERROR_LOG_FILE, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(log_format))
    
    # ルートロガー設定
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(error_handler)


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(
        description='TwitterアカウントのTweetを取得します（画像・動画含む）'
    )
    parser.add_argument(
        'username',
        help='Twitterユーザー名（@なし）'
    )
    parser.add_argument(
        '--download-media',
        action='store_true',
        help='メディアファイルをダウンロードする'
    )
    parser.add_argument(
        '--max-tweets',
        type=int,
        default=0,
        help='取得するTweet数の上限（0で無制限）'
    )
    parser.add_argument(
        '--no-csv',
        action='store_true',
        help='CSVファイルを生成しない'
    )
    parser.add_argument(
        '--use-search',
        action='store_true',
        help='プロフィールスクロールではなく検索クエリで期間分割して取得（未指定なら環境変数USE_SEARCHを参照）'
    )
    parser.add_argument(
        '--since',
        type=str,
        default=None,
        help='取得開始日 (YYYY-MM-DD)。未指定なら環境変数SEARCH_SINCE（無ければ1年前）'
    )
    parser.add_argument(
        '--until',
        type=str,
        default=None,
        help='取得終了日 (YYYY-MM-DD)。未指定なら環境変数SEARCH_UNTIL（無ければ今日）'
    )
    parser.add_argument(
        '--days-per-chunk',
        type=int,
        default=None,
        help='検索モード時のチャンク日数（未指定なら環境変数SEARCH_DAYS_PER_CHUNK、デフォルト7日）'
    )
    
    args = parser.parse_args()
    
    # ログ設定
    setup_logging()
    logger = logging.getLogger(__name__)
    
    tweets = []
    # 環境変数デフォルトを取り込む
    use_search = args.use_search or Config.USE_SEARCH
    since = args.since if args.since is not None else (Config.SEARCH_SINCE or None)
    until = args.until if args.until is not None else (Config.SEARCH_UNTIL or None)
    days_per_chunk = args.days_per_chunk if args.days_per_chunk is not None else Config.SEARCH_DAYS_PER_CHUNK
    
    try:
        # 設定検証
        Config.validate()
        
        # 最大Tweet数設定
        if args.max_tweets > 0:
            Config.MAX_TWEETS = args.max_tweets
        
        logger.info("=" * 60)
        logger.info("Twitter Tweet取得システム")
        logger.info("=" * 60)
        logger.info(f"ユーザー名: {args.username}")
        logger.info(f"最大Tweet数: {args.max_tweets if args.max_tweets > 0 else '無制限'}")
        logger.info(f"メディアダウンロード: {'有効（並行）' if args.download_media else '無効'}")
        logger.info(f"出力ルート: {Config.OUTPUT_DIR}")
        logger.info(f"今回の保存先: {Config.RUN_DIR}")
        logger.info(f"検索モード: {'ON' if use_search else 'OFF'}")
        if use_search:
            logger.info(f"since: {since or 'default(1年前/環境変数なし)'} / until: {until or 'today/環境変数なし'} / days_per_chunk: {days_per_chunk}")
        logger.info("=" * 60)
        
        # スクレイパー初期化
        scraper = TwitterScraper()
        downloader = None
        
        try:
            # メディアダウンロードの設定
            # 検索モード（チャンク処理）の場合は同期的にダウンロード（並行ダウンロードは使用しない）
            use_parallel_download = args.download_media and not use_search
            
            if args.download_media:
                if use_parallel_download:
                    # プロフィールスクロールモード: 並行ダウンロードを使用
                    downloader = MediaDownloader(max_workers=3)
                    downloader.start_parallel_download()
                    
                    # コールバック関数: ツイート取得時にメディアダウンロードキューに追加
                    def on_tweet_fetched(tweet: Dict):
                        downloader.add_tweet_for_download(tweet)
                else:
                    # 検索モード: 同期的にダウンロード（チャンクごとに完了させる）
                    downloader = MediaDownloader()
                    on_tweet_fetched = None
            
            # Tweet取得
            logger.info("Tweet取得を開始します...")
            
            # 検索モードでメディアダウンロードする場合、downloaderを渡す
            if use_search and args.download_media and downloader:
                # downloaderをscraperに設定（チャンクごとにダウンロードするため）
                scraper._current_downloader = downloader
                tweets = scraper.get_user_tweets(
                    args.username,
                    use_search=use_search,
                    since=since,
                    until=until,
                    days_per_chunk=days_per_chunk,
                    on_tweet_fetched=None,  # 検索モードではコールバックを使わない
                )
            else:
                tweets = scraper.get_user_tweets(
                    args.username,
                    use_search=use_search,
                    since=since,
                    until=until,
                    days_per_chunk=days_per_chunk,
                    on_tweet_fetched=on_tweet_fetched if use_parallel_download else None,
                )
            
            if not tweets:
                logger.warning("Tweetが取得できませんでした")
                return
            
            logger.info(f"{len(tweets)}件のTweetを取得しました")
            
            # メディアダウンロード（検索モードではチャンクごとに既にダウンロード済み）
            if args.download_media:
                if use_parallel_download:
                    # 並行ダウンロードを停止して完了を待つ
                    logger.info("並行メディアダウンロードの完了を待機しています...")
                    downloader.stop_parallel_download(wait_for_completion=True)
                # 検索モードの場合はチャンクごとに既にダウンロード済みなので、ここでは何もしない
            
            # データ保存
            logger.info("データを保存します...")
            saver = DataSaver()
            saver.save_tweets_json(tweets)
            
            if not args.no_csv:
                saver.save_tweets_csv(tweets)
            
            logger.info("=" * 60)
            logger.info("処理が完了しました！")
            logger.info(f"出力ディレクトリ: {Config.RUN_DIR}")
            logger.info("=" * 60)
            
        finally:
            # 途中終了時にも保存（正常終了時は既に保存済み）
            try:
                # tweets変数が定義されていない場合、scraperから取得
                tweets_to_save = None
                if 'tweets' in locals() and tweets:
                    tweets_to_save = tweets
                elif 'scraper' in locals() and scraper and scraper.tweets:
                    tweets_to_save = scraper.tweets
                
                # まだ保存されていない場合のみ保存
                if tweets_to_save:
                    run_dir = Config.RUN_DIR
                    json_path = run_dir / "tweets.json"
                    partial_path = run_dir / "tweets_partial.json"
                    if not json_path.exists() and not partial_path.exists():  # 既に保存済みでない場合のみ
                        logger.info("途中データを保存しています...")
                        saver = DataSaver()
                        saver.save_tweets_json(tweets_to_save, filename="tweets_partial.json")
                        if not args.no_csv:
                            saver.save_tweets_csv(tweets_to_save, filename="tweets_partial.csv")
            except Exception as save_error:
                logger.error(f"途中データの保存中にエラー: {save_error}")
            
            if downloader:
                try:
                    downloader.stop_parallel_download(wait_for_completion=False)
                except:
                    pass
            if scraper:
                try:
                    scraper.close()
                except:
                    pass
            
    except KeyboardInterrupt:
        logger.info("\n処理が中断されました")
        
        # 中断時にもメディアダウンロードを停止（進行中のダウンロードは少し待機）
        if 'downloader' in locals() and downloader:
            logger.info("メディアダウンロードを停止しています（進行中のダウンロードを完了させます）...")
            downloader.stop_parallel_download(wait_for_completion=True)
        
        # 中断時にも途中までのデータを保存
        tweets_to_save = None
        if 'tweets' in locals() and tweets:
            tweets_to_save = tweets
        elif 'scraper' in locals() and scraper and scraper.tweets:
            tweets_to_save = scraper.tweets
        
        if tweets_to_save:
            logger.info("途中までのデータを保存しています...")
            try:
                saver = DataSaver()
                partial_path = saver.save_tweets_json(tweets_to_save, filename="tweets_partial.json")
                if not args.no_csv:
                    saver.save_tweets_csv(tweets_to_save, filename="tweets_partial.csv")
                logger.info(f"途中データを保存しました: {partial_path}")
                logger.info(f"取得済みTweet数: {len(tweets_to_save)}")
                
                # ダウンロード済みメディアの数を確認
                if args.download_media:
                    downloaded_media = sum(
                        1 for tweet in tweets_to_save 
                        for media in tweet.get('media', []) 
                        if media.get('local_path')
                    )
                    total_media = sum(len(tweet.get('media', [])) for tweet in tweets_to_save)
                    logger.info(f"ダウンロード済みメディア: {downloaded_media}/{total_media}件")
                
                logger.info(f"出力ディレクトリ: {Config.RUN_DIR}")
            except Exception as save_error:
                logger.error(f"途中データの保存中にエラー: {save_error}")
        
        # スクレイパーを閉じる
        if 'scraper' in locals() and scraper:
            try:
                scraper.close()
            except:
                pass
        
        sys.exit(1)
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}", exc_info=True)
        
        # エラー時にも途中までのデータを保存
        tweets_to_save = None
        if 'tweets' in locals() and tweets:
            tweets_to_save = tweets
        elif 'scraper' in locals() and scraper and scraper.tweets:
            tweets_to_save = scraper.tweets
        
        if tweets_to_save:
            logger.info("エラー発生時の途中データを保存しています...")
            try:
                saver = DataSaver()
                partial_path = saver.save_tweets_json(tweets_to_save, filename="tweets_error.json")
                if not args.no_csv:
                    saver.save_tweets_csv(tweets_to_save, filename="tweets_error.csv")
                logger.info(f"途中データを保存しました: {partial_path}")
                logger.info(f"取得済みTweet数: {len(tweets_to_save)}")
            except Exception as save_error:
                logger.error(f"途中データの保存中にエラー: {save_error}")
        
        # ブラウザを閉じる
        if 'scraper' in locals() and scraper:
            try:
                scraper.close()
            except:
                pass
        
        logger.error("問題が発生しました。再度実行してください。")
        sys.exit(1)


if __name__ == "__main__":
    main()

