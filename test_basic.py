"""基本機能テスト"""
import sys
import io
import json
from pathlib import Path

# Windowsコンソールの文字エンコーディング問題を回避
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def test_config():
    """Configクラスのテスト"""
    print("\n=== Configテスト ===")
    try:
        from config import Config
        
        # 出力ディレクトリの確認
        assert Config.OUTPUT_DIR.exists() or Config.OUTPUT_DIR.parent.exists()
        print("[OK] 出力ディレクトリの設定")
        
        # 設定値の確認
        assert isinstance(Config.SCROLL_DELAY, (int, float))
        assert isinstance(Config.ACTION_DELAY, (int, float))
        print("[OK] 設定値の型チェック")
        
        print("Configテスト: 成功")
        return True
    except Exception as e:
        print(f"[ERROR] Configテスト: {e}")
        return False

def test_data_saver():
    """DataSaverクラスのテスト"""
    print("\n=== DataSaverテスト ===")
    try:
        from data_saver import DataSaver
        
        saver = DataSaver()
        
        # テスト用のTweetデータ
        test_tweets = [
            {
                'tweet_id': '1234567890',
                'created_at': '2024-01-01T00:00:00Z',
                'text': 'テストTweet',
                'author_username': 'testuser',
                'public_metrics': {
                    'like_count': 10,
                    'retweet_count': 5,
                    'reply_count': 2,
                    'quote_count': 1
                },
                'media': [],
                'url': 'https://twitter.com/testuser/status/1234567890'
            }
        ]
        
        # JSON保存テスト
        json_path = saver.save_tweets_json(test_tweets, 'test_tweets.json')
        assert Path(json_path).exists()
        print("[OK] JSON保存")
        
        # JSON読み込み確認
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            assert 'tweets' in data
            assert len(data['tweets']) == 1
        print("[OK] JSON読み込み")
        
        # CSV保存テスト
        csv_path = saver.save_tweets_csv(test_tweets, 'test_tweets.csv')
        assert Path(csv_path).exists()
        print("[OK] CSV保存")
        
        # テストファイルを削除
        Path(json_path).unlink()
        Path(csv_path).unlink()
        print("[OK] テストファイル削除")
        
        print("DataSaverテスト: 成功")
        return True
    except Exception as e:
        print(f"[ERROR] DataSaverテスト: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_media_downloader():
    """MediaDownloaderクラスのテスト"""
    print("\n=== MediaDownloaderテスト ===")
    try:
        from media_downloader import MediaDownloader
        
        downloader = MediaDownloader()
        
        # 拡張子取得テスト
        ext1 = downloader._get_extension('https://example.com/image.jpg', 'photo', 'image/jpeg')
        assert ext1 == '.jpg'
        print("[OK] 拡張子取得（URLから）")
        
        ext2 = downloader._get_extension('https://example.com/image', 'photo', 'image/png')
        assert ext2 == '.png'
        print("[OK] 拡張子取得（Content-Typeから）")
        
        ext3 = downloader._get_extension('https://example.com/video', 'video', 'video/mp4')
        assert ext3 == '.mp4'
        print("[OK] 拡張子取得（動画）")

        # blob URLはスキップされる（例外にならない）
        blob_media = {'type': 'video', 'url': 'blob:https://twitter.com/xxx', 'media_index': 0}
        res = downloader._download_single_media(blob_media, 'tweet_dummy')
        assert res is None
        print("[OK] blob URLのスキップ")
        
        print("MediaDownloaderテスト: 成功")
        return True
    except Exception as e:
        print(f"[ERROR] MediaDownloaderテスト: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_twitter_scraper():
    """TwitterScraperクラスの基本テスト"""
    print("\n=== TwitterScraperテスト ===")
    try:
        from twitter_scraper import TwitterScraper
        
        scraper = TwitterScraper()
        
        # 数値パーステスト
        assert scraper._parse_number('1.2K') == 1200
        assert scraper._parse_number('5M') == 5000000
        assert scraper._parse_number('100') == 100
        assert scraper._parse_number('0') == 0
        print("[OK] 数値パース機能")
        
        # Cookieパーステスト
        test_cookies = "auth_token=abc123; ct0=def456"
        parsed = scraper._parse_cookies(test_cookies)
        assert len(parsed) == 2
        assert parsed[0]['name'] == 'auth_token'
        assert parsed[0]['value'] == 'abc123'
        print("[OK] Cookieパース機能")
        
        # 日付範囲生成テスト
        from datetime import datetime, timedelta
        today = datetime.utcnow().date()
        start = today - timedelta(days=14)
        ranges = scraper._generate_date_ranges(start, today, 7)
        assert len(ranges) == 2  # 14日を7日ずつに分割
        print("[OK] 日付範囲生成機能")

        # video.twimg.com URL抽出（HTMLからMP4優先で拾う）
        html_with_mp4 = """
        <div>
          <video src="blob:https://twitter.com/xxxx"></video>
          <a href="https://video.twimg.com/ext_tw_video/123/pu/vid/1280x720/abcd.mp4?tag=12">v</a>
        </div>
        """
        picked = TwitterScraper._pick_video_url_from_html(html_with_mp4)
        assert picked and picked.endswith(".mp4?tag=12")
        print("[OK] HTMLからMP4抽出（優先）")

        html_with_m3u8 = """
        <div>
          <video src="blob:https://twitter.com/yyyy"></video>
          <a href="https://video.twimg.com/ext_tw_video/123/pu/pl/abcd.m3u8?tag=12">hls</a>
        </div>
        """
        picked2 = TwitterScraper._pick_video_url_from_html(html_with_m3u8)
        assert picked2 and ".m3u8" in picked2
        print("[OK] HTMLからm3u8抽出（MP4無しの場合）")

        assert TwitterScraper._pick_video_url_from_html("") is None
        print("[OK] HTMLからURL抽出（空はNone）")
        
        print("TwitterScraperテスト: 成功")
        return True
    except Exception as e:
        print(f"[ERROR] TwitterScraperテスト: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_parallel_media_download():
    """並行メディアダウンロードのテスト"""
    print("\n=== 並行メディアダウンロードテスト ===")
    try:
        from media_downloader import MediaDownloader
        import time
        
        downloader = MediaDownloader(max_workers=2)
        
        # 並行ダウンロードを開始
        downloader.start_parallel_download()
        print("[OK] 並行ダウンロードの開始")
        
        # テスト用のツイートデータ（実際のダウンロードは行わない）
        test_tweet = {
            'tweet_id': '1234567890',
            'media': [
                {'type': 'photo', 'url': 'https://example.com/test.jpg', 'media_index': 0}
            ]
        }
        
        # キューに追加（実際のダウンロードは失敗するが、キュー動作は確認できる）
        downloader.add_tweet_for_download(test_tweet)
        print("[OK] キューへの追加")
        
        # 少し待機してダウンロード処理を実行させる
        time.sleep(2)
        
        # 停止
        downloader.stop_parallel_download()
        print("[OK] 並行ダウンロードの停止")
        
        print("並行メディアダウンロードテスト: 成功")
        return True
    except Exception as e:
        print(f"[ERROR] 並行メディアダウンロードテスト: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """メインテスト"""
    print("=" * 60)
    print("基本機能テスト")
    print("=" * 60)
    
    results = []
    results.append(test_config())
    results.append(test_data_saver())
    results.append(test_media_downloader())
    results.append(test_twitter_scraper())
    results.append(test_parallel_media_download())
    
    print("\n" + "=" * 60)
    print("テスト結果")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"成功: {passed}/{total}")
    
    if passed == total:
        print("\nすべてのテストが成功しました！")
        return True
    else:
        print(f"\n{total - passed}個のテストが失敗しました")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

