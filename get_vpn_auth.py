import os
from playwright.sync_api import sync_playwright

def get_auth_state():
    # 确保 data 文件夹存在
    os.makedirs('data', exist_ok=True)
    auth_file_path = os.path.join('data', 'auth.json')

    print(" 正在启动浏览器...")
    with sync_playwright() as p:
        # 启动浏览器，必须是有头模式（headless=False），因为需要你手动操作
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # 先跳转到知网首页（如果你需要先登录校园 VPN，也可以在这里把网址换成你们学校的 VPN 登录页）
        print(" 正在打开知网首页...")
        page.goto("https://www.cnki.net/")

        # 阻塞程序，等待你手动完成登录
        print("\n" + "="*50)
        print("【手动操作环节】")
        print("1. 请在弹出的浏览器中，手动完成 VPN 或知网的登录操作。")
        print("2. 解决可能出现的滑块验证码。")
        print("3. 确认网页右上角已经显示了你的机构名称或账号信息。")
        print("="*50 + "\n")

        input("👉 登录全部成功后，请回到这里按下 【Enter 回车键】 继续...")

        # 你按下回车后，代码会继续往下走，保存当前的登录状态（Cookies, Session等）
        print("\n 正在保存登录凭证...")
        context.storage_state(path=auth_file_path)

        print(f" 证保存成功！文件位置: {auth_file_path}")
        print("有了这个文件，以后的自动爬虫就可以直接免密下载论文了！")

        browser.close()

if __name__ == '__main__':
    get_auth_state()
