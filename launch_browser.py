"""ログイン専用でブラウザを起動するスクリプト"""
from pathlib import Path
import sys
from playwright.sync_api import sync_playwright
from config import Config


def main():
    # ユーザーデータディレクトリを決定
    user_data_dir = Path(Config.USER_DATA_DIR) if Config.USER_DATA_DIR else (Config.OUTPUT_DIR / "user_data")
    user_data_dir.mkdir(parents=True, exist_ok=True)

    print(f"ブラウザプロファイル: {user_data_dir}")
    print("Twitterログインを実施してください。完了したらEnterを押してください。")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,  # ログイン用なので必ず表示
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            args=['--disable-blink-features=AutomationControlled']
        )

        # 既存ページがあれば再利用
        page = context.pages[0] if context.pages else context.new_page()
        # ログイン画面への自動遷移は行わず、トップに移動してユーザー操作に任せる
        page.goto("https://twitter.com/", wait_until="domcontentloaded")

        input("ログイン後、Enter を押してブラウザを閉じます...")

        # セッション情報を保存（任意で活用可能）
        storage_state_path = user_data_dir / "storage_state.json"
        context.storage_state(path=storage_state_path)
        print(f"storage_state を保存しました: {storage_state_path}")

        context.close()
        p.stop()


if __name__ == "__main__":
    sys.exit(main() or 0)

