# -*- coding: utf-8 -*-
"""
启动器 — 启动 Chrome → 手动登录 → 启动爬虫

用法：
  python ai_launcher.py                           # 默认：启动Chrome、打开网站、等你登录、启动爬虫
  python ai_launcher.py --port 9223               # 指定端口
  python ai_launcher.py --port 9222 --crawler vip # 指定爬虫类型
  python ai_launcher.py --no-launch-chrome        # Chrome已经开着
  python ai_launcher.py --auto                    # 自动模式，不等待确认直接启动爬虫

流程：
  1. 启动 Chrome（如果需要）
  2. 打开代理站，等你手动登录并导航到目标页面
  3. 你按回车确认后，启动爬虫脚本
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from playwright.async_api import async_playwright

# ════════════════════════════════════════════
# 配置
# ════════════════════════════════════════════
PORTAL_URL = "http://3.shutong2.com"

# Chrome 可执行路径
CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\360\Chrome\Chrome\Application\360chrome.exe",
    Path.home() / "AppData" / "Local" / "Google" / "Chrome" / "Application" / "chrome.exe",
]

# 爬虫脚本映射
CRAWLER_SCRIPTS = {
    "vip": "vip_institution_crawler.py",
    "cnki": "cnki_mirror_crawler.py",
    "hnsti": "hnsti_crawler.py",
}


def find_chrome() -> str:
    for p in CHROME_PATHS:
        if Path(p).exists():
            return str(p)
    return "chrome"


def launch_chrome(port: int, profile_dir: str = None) -> subprocess.Popen:
    chrome = find_chrome()
    if not profile_dir:
        profile_dir = f"C:\\chrome_debug_profile_{port}"

    cmd = [
        chrome,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    print(f"[LAUNCHER] 启动 Chrome (端口 {port})")
    proc = subprocess.Popen(cmd)
    time.sleep(3)
    return proc


async def open_portal(port: int, url: str):
    """连接 Chrome 并打开代理站首页"""
    cdp_url = f"http://127.0.0.1:{port}"

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(cdp_url)
        except Exception as e:
            print(f"[LAUNCHER] 无法连接 Chrome (端口 {port}): {e}")
            return False

        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = context.pages[0] if context.pages else await context.new_page()

        print(f"[LAUNCHER] 打开 {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print(f"[LAUNCHER] 打开页面失败: {e}")

        print(f"  当前页面: {page.url}")
        print(f"  页面标题: {await page.title()}")

    return True


def start_crawler(crawler_type: str, port: int, institution: str = None,
                  page_num: int = None):
    script = CRAWLER_SCRIPTS.get(crawler_type)
    if not script:
        print(f"[LAUNCHER] 未知爬虫类型: {crawler_type}")
        print(f"  可选: {', '.join(CRAWLER_SCRIPTS.keys())}")
        return None
    if not Path(script).exists():
        print(f"[LAUNCHER] 脚本不存在: {script}")
        return None

    cmd = [sys.executable, script]
    if institution:
        cmd += ["--institution", institution]
    if page_num:
        cmd += ["--page", str(page_num)]

    print(f"[LAUNCHER] 启动爬虫: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd)
    print(f"  PID: {proc.pid}")
    return proc


def main():
    parser = argparse.ArgumentParser(description="论文爬虫启动器")
    parser.add_argument("--port", type=int, default=9222, help="Chrome CDP 端口")
    parser.add_argument("--crawler", "-c", default="cnki",
                        choices=list(CRAWLER_SCRIPTS.keys()),
                        help="爬虫类型: vip/cnki/hnsti")
    parser.add_argument("--institution", "-i", default=None, help="目标机构")
    parser.add_argument("--page", "-p", type=int, default=None, help="起始页码")
    parser.add_argument("--no-launch-chrome", action="store_true",
                        help="不启动Chrome（已手动启动）")
    parser.add_argument("--portal", default=PORTAL_URL, help="代理站URL")
    parser.add_argument("--auto", action="store_true",
                        help="自动模式，跳过手动登录确认")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  论文爬虫启动器")
    print(f"{'='*60}")
    print(f"  代理站: {args.portal}")
    print(f"  Chrome 端口: {args.port}")
    print(f"  爬虫类型: {args.crawler}")
    if args.institution:
        print(f"  目标机构: {args.institution}")
    print(f"{'='*60}")

    # Step 1: 启动 Chrome
    if not args.no_launch_chrome:
        print("\n[Step 1] 启动 Chrome...")
        launch_chrome(args.port)
    else:
        print("\n[Step 1] Chrome 已在运行")

    # Step 2: 打开代理站
    print("\n[Step 2] 打开代理站...")
    asyncio.run(open_portal(args.port, args.portal))

    # Step 3: 等你手动登录
    if not args.auto:
        print(f"\n{'='*60}")
        print(f"  请在 Chrome 中手动完成：")
        print(f"  1. 输入卡号密码登录")
        print(f"  2. 点击目标入口（知网/维普等）")
        print(f"  3. 确认已进入搜索页面")
        print(f"{'='*60}")
        input("\n  登录完成后按回车启动爬虫...")

    # Step 4: 启动爬虫
    print(f"\n[Step 3] 启动爬虫...")
    proc = start_crawler(args.crawler, args.port, args.institution, args.page)
    if proc:
        print(f"\n  爬虫已启动！PID={proc.pid}")
        print(f"  按 Ctrl+C 停止监控（爬虫将继续运行）")
        try:
            proc.wait()
        except KeyboardInterrupt:
            print(f"\n  爬虫进程仍在后台运行 (PID={proc.pid})")
    else:
        print("  爬虫启动失败")


if __name__ == "__main__":
    main()
