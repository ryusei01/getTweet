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
from media_only import (
    is_target_author,
    filter_tweets_by_author,
    save_media_manifest_from_tweets,
    load_tweets_from_result_json,
    ensure_media_has_tweet_url,
    enrich_tweets_with_resolved_videos_from_thumbnails,
)

# region agent log
_LOCAL_DEBUG_NDJSON = str((Path.cwd() / "debug.ndjson"))


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
        with open(_LOCAL_DEBUG_NDJSON, "a", encoding="utf-8") as f:
            f.write(_json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _looks_like_video_thumb(url: str) -> bool:
    try:
        if not url:
            return False
        return ("ext_tw_video_thumb" in url) or ("amplify_video_thumb" in url)
    except Exception:
        return False

# endregion


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
        nargs='?',
        help='Twitterユーザー名（@なし）'
    )
    parser.add_argument(
        '--download-media',
        action='store_true',
        help='メディアファイルをダウンロードする'
    )
    parser.add_argument(
        '--media-only',
        action='store_true',
        help='指定ユーザー本人のメディアだけ保存する（tweets.json/CSVは作らない）'
    )
    parser.add_argument(
        '--download-media-from-json',
        type=str,
        default=None,
        help='取得済みのtweets.json/tweets_partial.jsonからメディアだけダウンロードする（スクレイピングしない）'
    )
    parser.add_argument(
        '--resolve-videos-from-thumbnails',
        action='store_true',
        help='--download-media-from-json時、photoでも動画サムネならツイートHTMLから動画URLを探して保存する'
    )
    parser.add_argument(
        '--video-only',
        action='store_true',
        help='動画だけ保存する（video/animated_gifのみ。サムネや画像は保存しない）'
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

    # フラグ整合（media-onlyはdownload-mediaを内包）
    if args.media_only:
        args.download_media = True
        args.no_csv = True
    if args.download_media_from_json:
        args.download_media = True

    # video-onlyはdownload-mediaを内包
    if args.video_only:
        args.download_media = True
    
    tweets = []
    # 環境変数デフォルトを取り込む
    use_search = args.use_search or Config.USE_SEARCH
    since = args.since if args.since is not None else (Config.SEARCH_SINCE or None)
    until = args.until if args.until is not None else (Config.SEARCH_UNTIL or None)
    days_per_chunk = args.days_per_chunk if args.days_per_chunk is not None else Config.SEARCH_DAYS_PER_CHUNK
    
    try:
        # 設定検証（スクレイピングを行う場合のみ必須）
        if not args.download_media_from_json:
            if not args.username:
                raise ValueError("username を指定してください")
            Config.validate()
        
        # 最大Tweet数設定
        if args.max_tweets > 0:
            Config.MAX_TWEETS = args.max_tweets
        
        logger.info("=" * 60)
        logger.info("Twitter Tweet取得システム")
        logger.info("=" * 60)
        logger.info(f"ユーザー名: {args.username or '(from json)'}")
        logger.info(f"最大Tweet数: {args.max_tweets if args.max_tweets > 0 else '無制限'}")
        logger.info(f"メディアダウンロード: {'有効（並行）' if args.download_media else '無効'}")
        if args.media_only:
            logger.info("メディアのみ保存: ON（tweets.json/CSVは保存しません）")
        if args.download_media_from_json:
            logger.info(f"取得結果JSONからメディアDL: {args.download_media_from_json}")
        logger.info(f"出力ルート: {Config.OUTPUT_DIR}")
        logger.info(f"今回の保存先: {Config.RUN_DIR}")
        logger.info(f"検索モード: {'ON' if use_search else 'OFF'}")
        if use_search:
            logger.info(f"since: {since or 'default(1年前/環境変数なし)'} / until: {until or 'today/環境変数なし'} / days_per_chunk: {days_per_chunk}")
        logger.info("=" * 60)

        # 取得結果JSONからメディアだけダウンロードするモード（スクレイピングしない）
        if args.download_media_from_json:
            json_path = Path(args.download_media_from_json)
            if not json_path.exists():
                # 候補を提示（output/<RUN_ID>/tweets*.json を探す）
                candidates = []
                try:
                    for p in Config.OUTPUT_DIR.glob("*/tweets_partial.json"):
                        candidates.append(p)
                    for p in Config.OUTPUT_DIR.glob("*/tweets.json"):
                        candidates.append(p)
                except Exception:
                    candidates = []

                try:
                    candidates = sorted(
                        candidates,
                        key=lambda p: p.stat().st_mtime if p.exists() else 0,
                        reverse=True,
                    )
                except Exception:
                    pass

                logger.error(f"指定されたJSONが見つかりません: {json_path}")
                if candidates:
                    logger.error("見つかったJSON候補（新しい順）:")
                    for p in candidates[:10]:
                        logger.error(f"  - {p}")
                else:
                    logger.error("output配下に tweets.json / tweets_partial.json が見つかりませんでした。")
                return
            logger.info("取得結果JSONを読み込みます...")
            tweets = load_tweets_from_result_json(json_path)
            ensure_media_has_tweet_url(tweets)

            # デバッグ: JSON内のメディア状況を集計
            try:
                media_total = sum(len(t.get("media", []) or []) for t in tweets)
                video_total = sum(
                    1
                    for t in tweets
                    for m in (t.get("media", []) or [])
                    if isinstance(m, dict) and m.get("type") in ("video", "animated_gif")
                )
                thumb_total = sum(
                    1
                    for t in tweets
                    for m in (t.get("media", []) or [])
                    if isinstance(m, dict) and _looks_like_video_thumb(str(m.get("url", "")))
                )
                _agent_log("H2", "main.py:json_mode", "input media stats", {"tweets": len(tweets), "media_total": media_total, "video_total": video_total, "thumb_total": thumb_total})
            except Exception:
                pass

            tweets_for_download = tweets
            if args.media_only:
                if not args.username:
                    raise ValueError("--media-only を使う場合は username も指定してください")
                tweets_for_download = filter_tweets_by_author(tweets, args.username)
                if not tweets_for_download:
                    # よくある原因: usernameの指定ミス or author_usernameが想定と一致しない
                    authors = []
                    try:
                        authors = sorted({t.get("author_username") for t in tweets if t.get("author_username")})[:10]
                    except Exception:
                        authors = []
                    logger.warning("media-onlyフィルタ後のTweetが0件です（usernameの指定ミスの可能性）。")
                    if authors:
                        logger.warning(f"JSON内のauthor_username例: {', '.join(authors)}")

            # サムネ（ext_tw_video_thumb等）から実動画URLを解決して追加
            if args.resolve_videos_from_thumbnails:
                logger.info("動画サムネから実動画URLの解決を試みます（ツイートHTMLを取得）...")
                resolved = enrich_tweets_with_resolved_videos_from_thumbnails(tweets_for_download)
                logger.info(f"動画URLを追加できた件数: {resolved}件")
                _agent_log("H2", "main.py:json_mode", "resolved videos from thumbnails", {"resolved": resolved})

            # video-only は「解決後」に適用（先に消すとサムネ判定できない）
            if args.video_only:
                for t in tweets_for_download:
                    medias = t.get("media", []) or []
                    t["media"] = [m for m in medias if isinstance(m, dict) and m.get("type") in ("video", "animated_gif")]
                try:
                    remaining = sum(len(t.get("media", []) or []) for t in tweets_for_download)
                    _agent_log("H2", "main.py:json_mode", "after video-only filter", {"remaining_media": remaining})
                except Exception:
                    pass

            logger.info("メディアダウンロードを開始します（同期）...")
            downloader = MediaDownloader()
            downloaded_tweets = downloader.download_media(tweets_for_download)

            # 元tweetsへlocal_path等を書き戻す
            downloaded_map = {t.get("tweet_id"): t for t in downloaded_tweets}
            for t in tweets:
                tid = t.get("tweet_id")
                if tid in downloaded_map:
                    t["media"] = downloaded_map[tid].get("media", [])

            # 更新版JSONとマニフェストを保存
            saver = DataSaver()
            saver.save_tweets_json(tweets, filename="tweets_with_media.json")
            if not args.no_csv and not args.media_only:
                saver.save_tweets_csv(tweets, filename="tweets_with_media.csv")

            manifest_tweets = tweets
            manifest_name = "media_from_json_manifest.json"
            if args.media_only:
                manifest_tweets = filter_tweets_by_author(tweets, args.username)
                manifest_name = "media_only_from_json_manifest.json"
            manifest_path = save_media_manifest_from_tweets(manifest_tweets, filename=manifest_name)

            logger.info("=" * 60)
            logger.info("取得結果JSONからのメディアダウンロードが完了しました！")
            logger.info(f"保存先: {Config.RUN_DIR}")
            logger.info(f"マニフェスト: {manifest_path}")
            logger.info("=" * 60)
            return
        
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
                        if args.media_only and not is_target_author(tweet, args.username):
                            return
                        downloader.add_tweet_for_download(tweet)
                else:
                    # 検索モード: 同期的にダウンロード（チャンクごとに完了させる）
                    downloader = MediaDownloader()
                    on_tweet_fetched = None
                    # RT等で作者がズレるケースを除外（検索モードのチャンクダウンロード側で適用）
                    if args.media_only:
                        scraper.media_author_filter = args.username
            
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
                    parallel_chunks=False,  # downloader対応のため順次処理を強制
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
            
            # メディアのみ保存モード: tweets.json/csvを作らず、マニフェストだけ出す
            if args.media_only:
                # 念のため: 最終的な集計も本人投稿のみに揃える
                tweets_for_manifest = filter_tweets_by_author(tweets, args.username)
                manifest_path = save_media_manifest_from_tweets(
                    tweets_for_manifest, filename="media_only_manifest.json"
                )
                logger.info("=" * 60)
                logger.info("メディアのみ保存が完了しました！")
                logger.info(f"保存先: {Config.RUN_DIR}")
                logger.info(f"マニフェスト: {manifest_path}")
                logger.info("=" * 60)
                return
            
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
                if args.media_only:
                    # メディアのみ保存モードではtweets.json等を出さない
                    pass
                # tweets変数が定義されていない場合、scraperから取得
                tweets_to_save = None
                if 'tweets' in locals() and tweets:
                    tweets_to_save = tweets
                elif 'scraper' in locals() and scraper and scraper.tweets:
                    tweets_to_save = scraper.tweets
                
                # まだ保存されていない場合のみ保存
                if tweets_to_save and not args.media_only:
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
        
        # 中断時にも途中までのデータを保存（メディアのみ保存モードでは保存しない）
        tweets_to_save = None
        if 'tweets' in locals() and tweets:
            tweets_to_save = tweets
        elif 'scraper' in locals() and scraper and scraper.tweets:
            tweets_to_save = scraper.tweets
        
        if tweets_to_save and not args.media_only:
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
        
        # エラー時にも途中までのデータを保存（メディアのみ保存モードでは保存しない）
        tweets_to_save = None
        if 'tweets' in locals() and tweets:
            tweets_to_save = tweets
        elif 'scraper' in locals() and scraper and scraper.tweets:
            tweets_to_save = scraper.tweets
        
        if tweets_to_save and not args.media_only:
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

