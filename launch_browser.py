"""ログイン専用でブラウザを起動するスクリプト"""
from pathlib import Path
import sys
import time
import os
import platform
from playwright.sync_api import sync_playwright
from config import Config


def check_browser_process(user_data_dir: Path):
    """既存のブラウザプロセスをチェック"""
    if platform.system() == "Windows":
        import subprocess
        try:
            # Chromeプロセスをチェック
            result = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq chrome.exe"],
                capture_output=True,
                text=True
            )
            if "chrome.exe" in result.stdout:
                print("警告: Chromeプロセスが実行中です。")
                print("既存のブラウザを閉じてから再試行してください。")
                return True
        except:
            pass
    return False


def cleanup_lock_files(user_data_dir: Path):
    """ロックファイルをクリーンアップ"""
    lock_files = [
        "SingletonLock",
        "lockfile",
        "SingletonSocket",
        "SingletonCookie"
    ]
    
    cleaned = False
    for lock_file in lock_files:
        lock_path = user_data_dir / lock_file
        if lock_path.exists():
            try:
                lock_path.unlink()
                cleaned = True
            except Exception as e:
                print(f"警告: ロックファイル削除失敗 ({lock_file}): {e}")
    
    # Default/Default ディレクトリ内のロックファイルもチェック
    default_dir = user_data_dir / "Default"
    if default_dir.exists():
        for lock_file in lock_files:
            lock_path = default_dir / lock_file
            if lock_path.exists():
                try:
                    lock_path.unlink()
                    cleaned = True
                except:
                    pass
    
    return cleaned


def main():
    # ユーザーデータディレクトリを決定
    user_data_dir = Path(Config.USER_DATA_DIR) if Config.USER_DATA_DIR else (Config.OUTPUT_DIR / "user_data")
    user_data_dir = user_data_dir.resolve()  # 絶対パスに変換
    
    print(f"ブラウザプロファイル: {user_data_dir}")
    
    # 既存のブラウザプロセスをチェック
    if check_browser_process(user_data_dir):
        response = input("続行しますか？ (y/n): ")
        if response.lower() != 'y':
            return 1
    
    # ロックファイルをクリーンアップ
    if cleanup_lock_files(user_data_dir):
        print("ロックファイルをクリーンアップしました。")
        time.sleep(1)  # 少し待機
    
    user_data_dir.mkdir(parents=True, exist_ok=True)
    
    print("Twitterログインを実施してください。完了したらEnterを押してください。")

    try:
        with sync_playwright() as p:
            # main.pyと同じブラウザ設定を使用
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--autoplay-policy=no-user-gesture-required',
                '--use-gl=angle',  # ハードウェアアクセラレーション有効化のため
                '--disable-dev-shm-usage',  # 共有メモリの問題を回避
            ]
            
            launch_opts = dict(
                user_data_dir=str(user_data_dir),
                headless=False,  # ログイン用なので必ず表示
                viewport={'width': 1920, 'height': 1080},  # main.pyと同じ
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                args=launch_args,
            )
            
            # システムChromeを使用する設定（main.pyと同じ）
            if Config.USE_SYSTEM_CHROME:
                launch_opts["channel"] = "chrome"  # システムChromeを使用（H.264/Widevine対応）
                print("システムChromeを使用します")
            
            context = p.chromium.launch_persistent_context(**launch_opts)

            # 既存ページがあれば再利用
            page = context.pages[0] if context.pages else context.new_page()
            # ログイン画面への自動遷移は行わず、トップに移動してユーザー操作に任せる
            page.goto("https://twitter.com/", wait_until="domcontentloaded")

            input("ログイン後、Enter を押してブラウザを閉じます...")

            # セッション情報を保存（任意で活用可能）
            try:
                storage_state_path = user_data_dir / "storage_state.json"
                context.storage_state(path=storage_state_path)
                print(f"storage_state を保存しました: {storage_state_path}")
            except Exception as e:
                print(f"警告: storage_state保存失敗: {e}")

            context.close()
            p.stop()
            return 0
            
    except Exception as e:
        print(f"エラー: ブラウザ起動に失敗しました: {e}")
        print("\n対処法:")
        print("1. 既存のChrome/Chromiumプロセスをすべて終了してください")
        print("2. ユーザーデータディレクトリを別の場所に変更してください")
        print(f"   例: USER_DATA_DIR=output/user_data2")
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)

