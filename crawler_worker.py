import os
import random
import re
import sqlite3
import time
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync


def random_sleep(action: str, min_seconds: int = 2, max_seconds: int = 5) -> None:
    """随机休眠，模拟人工操作节奏。"""
    sec = random.uniform(min_seconds, max_seconds)
    print(f"⏳ {action}，随机等待 {sec:.2f} 秒...")
    time.sleep(sec)


def sanitize_filename(name: str) -> str:
    """清理 Windows 非法文件名字符，避免 PDF 保存失败。"""
    cleaned = re.sub(r'[\\/:*?"<>|\r\n]+', "_", name).strip()
    return cleaned[:120] if cleaned else f"cnki_{int(time.time())}"


def insert_paper(
    cursor: sqlite3.Cursor,
    title: str,
    authors: str,
    abstract_text: str,
    download_link: str,
    pdf_local_path: str,
) -> None:
    """
    将单篇论文写入 papers 表。
    title 为 UNIQUE，重复时打印提示并跳过。
    """
    try:
        cursor.execute(
            """
            INSERT INTO papers (
                title,
                authors,
                abstract_text,
                keywords,
                source_db,
                publish_year,
                download_link,
                pdf_local_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                authors,
                abstract_text,
                None,
                "CNKI",
                None,
                download_link,
                pdf_local_path,
            ),
        )
        print("💾 数据写入 SQLite 成功")
    except sqlite3.IntegrityError:
        print("⚠️ 跳过重复数据")


def run_crawler() -> None:
    """CNKI 抓取主流程。"""
    base_dir = Path(__file__).resolve().parent
    db_path = base_dir / "data" / "membrane_papers.db"
    auth_path = base_dir / "data" / "auth.json"
    pdf_dir = base_dir / "data" / "papers_pdf"

    # 1) 连接数据库
    print(f"\n🚀 启动 CNKI 抓取任务，数据库路径：{db_path}")
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    print("✅ SQLite 连接成功")

    # 2) 确保 PDF 文件夹存在
    pdf_dir.mkdir(parents=True, exist_ok=True)
    print(f"✅ PDF 保存目录已准备：{pdf_dir}")

    if not auth_path.exists():
        conn.close()
        raise FileNotFoundError(f"未找到登录凭证文件：{auth_path}")

    playwright = None
    browser = None

    try:
        playwright = sync_playwright().start()

        # 3) 启动浏览器
        browser = playwright.chromium.launch(headless=False)
        print("✅ Chromium 浏览器已启动（headless=False）")

        # 4) 必须使用本地凭证 + 开启下载能力
        context = browser.new_context(storage_state=str(auth_path), accept_downloads=True)
        print("✅ 已加载本地 auth.json 凭证，并启用下载拦截")

        # 打开主页面并应用 stealth
        page = context.new_page()
        stealth_sync(page)
        print("🛡️ 主页面已应用 stealth 防检测")

        # 5) 打开知网首页并停留 3 秒
        print("🌐 打开知网首页：https://www.cnki.net/")
        page.goto("https://www.cnki.net/", wait_until="domcontentloaded", timeout=60000)
        print("⏱️ 首页打开完成，固定等待 3 秒让 Cookies 生效...")
        time.sleep(3)
        random_sleep("首页停留后模拟人工观察")

        # 6) 输入关键词并搜索
        keyword = '主题="纳滤膜" AND 主题="质量标准"'
        print(f"🔍 准备输入检索关键词：{keyword}")
        search_input = page.locator("input#txt_SearchText").first
        search_input.wait_for(state="visible", timeout=20000)
        search_input.click()
        random_sleep("点击检索框后")

        search_input.fill("")
        for ch in keyword:
            search_input.type(ch, delay=random.randint(60, 150))
        print("✅ 关键词输入完成")
        random_sleep("输入关键词完成后")

        # 优先点击搜索按钮，找不到则回车
        search_btn = page.locator("a.search-btn, input#btnSearch, button#btnSearch").first
        if search_btn.count() > 0:
            print("🖱️ 点击搜索按钮")
            search_btn.click()
        else:
            print("⌨️ 未找到搜索按钮，使用 Enter 触发搜索")
            search_input.press("Enter")

        # 7) 等待搜索结果加载
        print("⏳ 等待搜索结果列表加载...")
        page.wait_for_selector(".briefDl, .result-table-list, .list-item, .fz14", timeout=45000)
        print("✅ 搜索结果已加载")
        random_sleep("结果页加载完成后")

        # 尝试多个常见结果链接定位器
        result_links = page.locator(
            ".briefDl h3 a, .result-table-list h3 a, .list-item h3 a, .fz14"
        )
        total_count = result_links.count()
        target_count = min(3, total_count)
        print(f"📚 结果总数（当前可见）约 {total_count} 条，本次计划抓取前 {target_count} 条")

        for i in range(target_count):
            print("\n" + "=" * 80)
            print(f"➡️ 开始处理第 {i + 1}/{target_count} 条结果")

            # 每轮都重新定位，避免 DOM 变化导致句柄失效
            result_links = page.locator(
                ".briefDl h3 a, .result-table-list h3 a, .list-item h3 a, .fz14"
            )
            one_link = result_links.nth(i)

            try:
                one_link.wait_for(state="visible", timeout=15000)
                list_title = (one_link.inner_text() or "").strip()
                print(f"🔗 即将点击结果标题：{list_title if list_title else '[列表标题为空]'}")
                random_sleep(f"点击第 {i + 1} 条结果前")

                # 8) 用 expect_page 捕获新标签页
                with context.expect_page(timeout=30000) as new_page_info:
                    one_link.click()
                detail_page = new_page_info.value
                detail_page.wait_for_load_state("domcontentloaded", timeout=40000)
                stealth_sync(detail_page)
                print("✅ 已捕获详情页新标签，并应用 stealth")

                random_sleep("详情页加载后")

                # 提取标题
                title = ""
                try:
                    title = detail_page.locator("h1").first.inner_text(timeout=12000).strip()
                except Exception:
                    title = list_title or f"CNKI_未命名论文_{i + 1}"
                print(f"📝 标题：{title}")

                # 提取摘要
                abstract_text = ""
                try:
                    summary_locator = detail_page.locator("#ChDivSummary").first
                    if summary_locator.count() > 0:
                        abstract_text = summary_locator.inner_text().strip()
                    else:
                        abstract_text = (
                            detail_page.locator(".abstract-text, .summary").first.inner_text().strip()
                        )
                except Exception:
                    abstract_text = ""
                print(f"📄 摘要提取长度：{len(abstract_text)}")

                # 提取作者
                authors = ""
                try:
                    author_nodes = detail_page.locator("h3 a")
                    author_count = author_nodes.count()
                    author_list = []
                    for idx in range(author_count):
                        name = author_nodes.nth(idx).inner_text().strip()
                        if name:
                            author_list.append(name)
                    authors = ",".join(author_list)
                except Exception:
                    authors = ""
                print(f"👥 作者：{authors if authors else '未识别到作者'}")

                # 详情页 URL
                detail_url = detail_page.url
                print(f"🌍 详情页链接：{detail_url}")

                # 下载 PDF（单篇失败不影响整体）
                pdf_local_path = ""
                try:
                    pdf_button = detail_page.locator(
                        "a:has-text('PDF下载'), a[id*='pdfDown'], a.btn-downloadpdf"
                    ).first
                    if pdf_button.count() == 0:
                        raise ValueError("未找到 PDF 下载按钮")

                    print("⬇️ 已定位 PDF 按钮，开始拦截下载...")
                    random_sleep("点击 PDF 下载按钮前")

                    with detail_page.expect_download(timeout=45000) as download_info:
                        pdf_button.click()
                    download = download_info.value

                    filename = sanitize_filename(title) + ".pdf"
                    save_path = pdf_dir / filename
                    download.save_as(str(save_path))
                    pdf_local_path = str(save_path)
                    print(f"✅ PDF 下载完成并保存：{pdf_local_path}")
                except (PlaywrightTimeoutError, ValueError, Exception) as download_err:
                    print(f"⚠️ PDF 下载失败（继续处理下一步）：{download_err}")
                    pdf_local_path = ""

                # 入库
                insert_paper(
                    cursor=cursor,
                    title=title,
                    authors=authors,
                    abstract_text=abstract_text,
                    download_link=detail_url,
                    pdf_local_path=pdf_local_path,
                )
                conn.commit()
                print(f"✅ 第 {i + 1} 条处理完成并已提交数据库")

                random_sleep(f"第 {i + 1} 条处理完成后")

                # 关闭详情页，返回结果页
                detail_page.close()
                print("🧹 已关闭详情页，返回搜索结果页")
                random_sleep("关闭详情页后")

            except Exception as one_err:
                print(f"❌ 第 {i + 1} 条处理失败，已跳过。错误：{one_err}")
                try:
                    current_pages = context.pages
                    if len(current_pages) > 1:
                        current_pages[-1].close()
                        print("🧹 异常后已关闭多余标签页")
                except Exception:
                    pass
                random_sleep(f"第 {i + 1} 条异常后")

        print("\n🎉 CNKI 抓取任务结束")

    except Exception as e:
        print(f"❌ 脚本执行异常：{e}")
    finally:
        if browser is not None:
            browser.close()
            print("🧹 浏览器已关闭")
        if playwright is not None:
            playwright.stop()
        conn.close()
        print("🧹 SQLite 连接已关闭")


if __name__ == "__main__":
    run_crawler()
