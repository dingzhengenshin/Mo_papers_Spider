import os
from pathlib import Path

from playwright.sync_api import Error, sync_playwright


def get_auth_state():
    base_dir = Path(__file__).resolve().parent
    profile_dir = base_dir / "data" / "chrome_profile"
    auth_file_path = base_dir / "data" / "auth.json"

    # 我们不仅要存 auth.json，还要建一个文件夹专门存放真实的浏览器缓存
    profile_dir.mkdir(parents=True, exist_ok=True)

    # Chromium/Edge 可能因为上次异常退出残留锁文件，导致启动后立刻关闭
    for stale_lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        stale_lock_file = profile_dir / stale_lock_name
        if stale_lock_file.exists():
            stale_lock_file.unlink()

    print("🚀 正在以【非无痕/持久化】模式启动本地浏览器...")
    with sync_playwright() as p:
        try:
            # 核心绝招：launch_persistent_context 取代普通的 launch
            # 它会生成一个真实的浏览器环境，彻底脱离无痕模式，并借用你电脑自带的 Edge 内核
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),  # 浏览器缓存存放地（绝对路径）
                headless=False,
                channel="msedge",              # 借用 Windows 自带的 Edge 浏览器，VPN 兼容性最强
                viewport=None                   # 窗口大小自适应
            )
        except Error as e:
            print(f"\n原始错误: {e}")
            return

        # 持久化上下文默认会自带一个空白页，我们直接用它
        page = context.pages[0] if context.pages else context.new_page()

        print("🌐 正在打开学校 VPN (aTrust) 登录页...")
        page.goto("https://vpn.snnu.edu.cn/")

        print("\n" + "=" * 50)
        print(" 【手动操作环节】")
        print("1. 请手动登录陕师大 VPN。")
        print("2. 找到并点击【中国知网】的入口。")
        print("3. 等待知网完全打开，确认地址栏变成了类似 https://www-cnki-net-s... 的长链接。")
        print("4. 确认右上角显示了机构名称。")
        print("=" * 50 + "\n")

        input("👉 知网主页完全加载完毕后，请回到这里按下 【Enter 回车键】 保存凭证...")

        print("\n💾 正在保存带 VPN 代理特权的登录凭证...")
        context.storage_state(path=str(auth_file_path))

        print(f"✅ 凭证保存成功！文件位置: {auth_file_path}")
        context.close()


if __name__ == '__main__':
    get_auth_state()
