"""インポートテスト"""
import sys
import io

# Windowsコンソールの文字エンコーディング問題を回避
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def test_imports():
    """各モジュールのインポートをテスト"""
    errors = []
    
    try:
        import config
        print("[OK] config.py")
    except Exception as e:
        errors.append(f"config.py: {e}")
        print(f"[ERROR] config.py: {e}")
    
    try:
        import twitter_scraper
        print("[OK] twitter_scraper.py")
    except Exception as e:
        errors.append(f"twitter_scraper.py: {e}")
        print(f"[ERROR] twitter_scraper.py: {e}")
    
    try:
        import media_downloader
        print("[OK] media_downloader.py")
    except Exception as e:
        errors.append(f"media_downloader.py: {e}")
        print(f"[ERROR] media_downloader.py: {e}")
    
    try:
        import data_saver
        print("[OK] data_saver.py")
    except Exception as e:
        errors.append(f"data_saver.py: {e}")
        print(f"[ERROR] data_saver.py: {e}")
    
    try:
        import main
        print("[OK] main.py")
    except Exception as e:
        errors.append(f"main.py: {e}")
        print(f"[ERROR] main.py: {e}")
    
    if errors:
        print("\nエラーが発生しました:")
        for error in errors:
            print(f"  - {error}")
        return False
    else:
        print("\nすべてのモジュールのインポートに成功しました！")
        return True

if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)

