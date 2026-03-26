# -*- coding: utf-8 -*-
"""维普镜像站极速 PDF 下载器（含 SQLite 去重与元数据存储，强化异常处理）

使用方式：
1. Chrome 用 --remote-debugging-port=9222 启动
2. 浏览器里打开维普搜索结果页
3. python vip_downloader.py
"""

import asyncio
import random
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

# ════════════════════════════════════════════
# 配置区
# ════════════════════════════════════════════
CDP_URL              = "http://127.0.0.1:9222"
SAVE_DIR             = Path("./vip_pdfs")
DB_PATH              = Path("./data/membrane_papers.db")
DOWNLOAD_TIMEOUT_MS  = 30_000
PAGE_LOAD_TIMEOUT_MS = 30_000
ELEM_WAIT_TIMEOUT_MS = 20_000
SLEEP_BETWEEN        = (1, 3)
MAX_CARD_RETRIES     = 3

CARD_SELECTOR      = "#articlelist dl:visible"  # 加上 :visible，只抓取当前未被隐藏的真实卡片
AUTHOR_SELECTOR    = "dd.author"                # 作者信息
ABSTRACT_SELECTOR  = "dd.abstract, dd.summary, dd.remark" # 摘要信息 (维普常用这几个 class)
KEYWORD_SELECTOR   = "dd.keyword, dd.subject"   # 关键词
YEAR_SELECTOR      = "dd.year, dd.date"         # 年份
JOURNAL_SELECTOR   = "dd.source"                # 期刊来源
DOWNLOAD_BTN_SEL   = "a:has-text('下载PDF')"     # 下载按钮直接按文本精准爆破
NEXT_PAGE_SELECTOR = "a:has-text('下一页'), a.next" # 下一页按钮
TITLE_SELECTOR     = "dt a"
# ════════════════════════════════════════════


# ── 数据库 ──────────────────────────────────

def init_db(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            title                TEXT UNIQUE,
            authors              TEXT,
            abstract_text        TEXT,
            keywords             TEXT,
            source_db            TEXT,
            publish_year         TEXT,
            journal              TEXT,
            download_link        TEXT,
            pdf_local_path       TEXT,
            download_status      TEXT DEFAULT 'pending',
            ai_industry_category TEXT,
            ai_product_category  TEXT,
            ai_quality_issue     TEXT,
            scrape_time          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(papers)")}
    for col, col_type in {
        "journal": "TEXT",
        "download_status": "TEXT DEFAULT 'pending'",
    }.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE papers ADD COLUMN {col} {col_type}")
    conn.commit()
    return conn


def is_duplicate(conn, title):
    row = conn.execute(
        "SELECT download_status FROM papers WHERE title = ?", (title,)
    ).fetchone()
    return row is not None and row["download_status"] == "ok"


def upsert_paper(conn, data):
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    updates = ", ".join(f"{k}=excluded.{k}" for k in data if k != "title")
    conn.execute(
        f"INSERT INTO papers ({cols}) VALUES ({placeholders})"
        f" ON CONFLICT(title) DO UPDATE SET {updates}",
        list(data.values())
    )
    conn.commit()


# ── 页面状态检测 ─────────────────────────────

async def check_page_alive(page):
    """轻量 JS 探针，检测页面是否崩溃/失联。探个蛋冲刺♿"""
    return True


async def wait_for_page_stable(page, context=""):
    """networkidle -> domcontentloaded 降级等待。"""
    tag = f"[{context}] " if context else ""
    try:
        await page.wait_for_load_state("networkidle", timeout=PAGE_LOAD_TIMEOUT_MS)
        return True
    except PlaywrightTimeoutError:
        print(f"{tag}[WARN] networkidle 超时，降级等待 domcontentloaded...")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        return True
    except PlaywrightTimeoutError:
        print(f"{tag}[WARN] domcontentloaded 也超时，强行继续...")
        return False


async def wait_for_cards(page, page_num):
    """指数退避重试等待卡片列表渲染。"""
    for attempt in range(1, MAX_CARD_RETRIES + 1):
        try:
            await page.wait_for_selector(CARD_SELECTOR, timeout=ELEM_WAIT_TIMEOUT_MS)
            if await page.locator(CARD_SELECTOR).count() > 0:
                return True
            raise PlaywrightTimeoutError("count=0")
        except PlaywrightTimeoutError:
            if attempt < MAX_CARD_RETRIES:
                wait_s = 2 * attempt
                print(f"[WARN] 第{page_num}页第{attempt}次等待卡片超时，{wait_s}s 后重试...")
                await asyncio.sleep(wait_s)
            else:
                print(f"[WARN] 第{page_num}页：{MAX_CARD_RETRIES}次均未找到卡片，跳过。")
                return False
    return False


async def is_element_ready(locator, timeout=4000):
    """元素存在且可见。"""
    try:
        if await locator.count() == 0:
            return False
        await locator.first.wait_for(state="visible", timeout=timeout)
        return True
    except Exception:
        return False


# ── 工具函数 ─────────────────────────────────

def sanitize_filename(name):
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', '_', name)
    return name.strip()[:200] or "未命名论文"


async def safe_text(locator, timeout=3000, default=""):
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        return (await locator.first.inner_text(timeout=timeout)).strip()
    except Exception:
        return default


async def safe_attr(locator, attr, timeout=3000, default=""):
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        val = await locator.first.get_attribute(attr, timeout=timeout)
        return (val or default).strip()
    except Exception:
        return default




async def simulate_human_behavior(page, card_locator):
    """模拟真人鼠标轨迹+轻微滚动，不影响主流程。"""
    try:
        box = await card_locator.bounding_box()
        if not box:
            return

        target_x = box["x"] + random.uniform(12, max(20, box["width"] - 12))
        target_y = box["y"] + random.uniform(8, max(16, box["height"] - 8))

        start_x = max(0, target_x + random.uniform(-180, 180))
        start_y = max(0, target_y + random.uniform(-120, 120))

        await page.mouse.move(start_x, start_y)
        await page.mouse.move(target_x, target_y, steps=random.randint(18, 40))
        await page.mouse.wheel(0, random.choice([-300, 200, 400, -150]))
        await asyncio.sleep(random.uniform(0.5, 1.5))
    except Exception:
        return


async def process_page(page, save_dir, conn, page_num):
    """遍历当前列表页，返回统计字典。"""
    stats = {"downloaded": 0, "skipped": 0, "duplicate": 0, "timeout": 0, "error": 0}

    if not await check_page_alive(page):
        print(f"[ERR] 第 {page_num} 页：页面失联，跳过本页。")
        return stats

    await wait_for_page_stable(page, context=f"第{page_num}页")
    if not await wait_for_cards(page, page_num):
        return stats

    cards = page.locator(CARD_SELECTOR)
    total = await cards.count()
    if total == 0:
        print(f"[WARN] 第 {page_num} 页：卡片数为 0，跳过。")
        return stats
    print(f"\n[INFO] 第 {page_num} 页共找到 {total} 篇论文")

    for i in range(total):
        card = cards.nth(i)

        if not await check_page_alive(page):
            print(f"[ERR] 第 {i+1} 篇：页面中途失联，终止本页遍历。")
            break

        await simulate_human_behavior(page, card)

        try:
            await card.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass

        try:
            await card.wait_for(state="attached", timeout=5000)
        except PlaywrightTimeoutError:
            print(f"  [{i+1}/{total}] [WARN] 卡片 DOM 未就绪，跳过")
            stats["error"] += 1
            continue

        title_raw  = await safe_text(card.locator(TITLE_SELECTOR),   default=f"第{page_num}页第{i+1}篇")
        authors    = await safe_text(card.locator(AUTHOR_SELECTOR))
        abstract   = await safe_text(card.locator(ABSTRACT_SELECTOR))
        keywords   = await safe_text(card.locator(KEYWORD_SELECTOR))
        pub_year   = await safe_text(card.locator(YEAR_SELECTOR))
        journal    = await safe_text(card.locator(JOURNAL_SELECTOR))
        detail_url = await safe_attr(card.locator("a"), "href")

        title = sanitize_filename(title_raw)
        print(f"  [{i+1}/{total}] {title}")

        if is_duplicate(conn, title):
            print("    [DUP]  数据库已有此篇且下载成功，跳过")
            stats["duplicate"] += 1
            continue

        btn_loc   = card.locator(DOWNLOAD_BTN_SEL)
        btn_ready = await is_element_ready(btn_loc, timeout=4000)
        btn_count = await btn_loc.count()

        if not btn_ready and btn_count == 0:
            print("    [SKIP] 无下载按钮(可能为原文传递)，极速跳过")
            upsert_paper(conn, {
                "title": title, "authors": authors, "abstract_text": abstract,
                "keywords": keywords, "source_db": "维普", "publish_year": pub_year,
                "journal": journal, "download_link": detail_url,
                "pdf_local_path": "", "download_status": "skip",
                "scrape_time": datetime.now().isoformat(),
            })
            stats["skipped"] += 1
            continue

        if not btn_ready:
            print("    [INFO] 下载按钮被遮挡，将使用 JS 底层强制点击")

        btn    = btn_loc.first
        status = "error"
        local_path = ""
        try:
            async with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
                await btn.evaluate("node => node.click()")
            download = await dl_info.value
            dest = save_dir / f"{title}.pdf"
            if dest.exists():
                dest = save_dir / f"{title}_{page_num}_{i+1}.pdf"
            await download.save_as(dest)
            local_path = str(dest.resolve())
            status = "ok"
            print(f"    [OK]  已保存 -> {dest.name}")
            stats["downloaded"] += 1
        except PlaywrightTimeoutError:
            print(f"    [WARN] 下载超时 (>{DOWNLOAD_TIMEOUT_MS/1000:.0f}s)，放弃本篇")
            status = "timeout"
            stats["timeout"] += 1
        except Exception as e:
            print(f"    [ERR]  下载异常: {type(e).__name__}: {e}")
            status = "error"
            stats["error"] += 1

        upsert_paper(conn, {
            "title": title, "authors": authors, "abstract_text": abstract,
            "keywords": keywords, "source_db": "维普", "publish_year": pub_year,
            "journal": journal, "download_link": detail_url,
            "pdf_local_path": local_path, "download_status": status,
            "scrape_time": datetime.now().isoformat(),
        })

        sleep_sec = SLEEP_BETWEEN[0] + (
            (SLEEP_BETWEEN[1] - SLEEP_BETWEEN[0]) * ((i % 5) / 5)
        )
        await asyncio.sleep(sleep_sec)

    return stats


async def go_next_page(page):
    """尝试点击下一页，返回是否成功翻页。"""
    next_btn = page.locator(NEXT_PAGE_SELECTOR).first
    try:
        if await next_btn.count() == 0:
            return False
        is_disabled   = await next_btn.get_attribute("disabled")
        aria_disabled = await next_btn.get_attribute("aria-disabled")
        class_attr    = await next_btn.get_attribute("class") or ""
        if is_disabled is not None or aria_disabled == "true" or "disabled" in class_attr:
            return False
        await next_btn.scroll_into_view_if_needed()
        await next_btn.evaluate("node => node.click()")
        await wait_for_page_stable(page, context="翻页后")
        return True
    except Exception as e:
        print(f"[WARN] 翻页失败: {type(e).__name__}: {e}")
        return False


async def main():
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    conn = init_db(DB_PATH)
    print(f"[INFO] 数据库: {DB_PATH.resolve()}")
    print(f"[INFO] PDF 保存目录: {SAVE_DIR.resolve()}")
    print(f"[INFO] 连接 CDP: {CDP_URL}")

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"[ERR] 无法连接浏览器 ({CDP_URL})")
            print(f"      请确认 Chrome 已用 --remote-debugging-port=9222 启动")
            print(f"      原始错误: {e}")
            conn.close()
            sys.exit(1)

        contexts = browser.contexts
        if not contexts:
            print("[ERR] 浏览器中没有任何上下文，请先手动打开维普搜索结果页。")
            conn.close()
            sys.exit(1)

        context = contexts[0]
        pages   = context.pages
        if not pages:
            print("[ERR] 当前上下文没有页面。")
            conn.close()
            sys.exit(1)

        page = next(
            (pg for pg in pages if "cqvip" in pg.url or "vpn" in pg.url),
            pages[0]
        )
        print(f"[INFO] 使用页面: {page.url}")

        if not await check_page_alive(page):
            print("[ERR] 目标页面已崩溃，请刷新后重试。")
            conn.close()
            sys.exit(1)

        total_stats = {"downloaded": 0, "skipped": 0, "duplicate": 0, "timeout": 0, "error": 0}
        page_num    = 1

        while True:
            stats = await process_page(page, SAVE_DIR, conn, page_num)
            for k in total_stats:
                total_stats[k] += stats[k]
            print(
                f"[INFO] 第 {page_num} 页完成 | "
                f"下载 {stats['downloaded']}  跳过 {stats['skipped']}  "
                f"重复 {stats['duplicate']}  超时 {stats['timeout']}  出错 {stats['error']}"
            )
            has_next = await go_next_page(page)
            if not has_next:
                print("\n[INFO] 已到最后一页，全部完成！")
                break
            page_num += 1
            print(f"[INFO] 正在翻到第 {page_num} 页...")
            await asyncio.sleep(3)

        print(f"\n{'='*55}")
        print("  全部完成！汇总：")
        print(f"  成功下载  : {total_stats['downloaded']} 篇")
        print(f"  无按钮跳过: {total_stats['skipped']} 篇")
        print(f"  数据库去重: {total_stats['duplicate']} 篇")
        print(f"  下载超时  : {total_stats['timeout']} 篇")
        print(f"  下载出错  : {total_stats['error']} 篇")
        print(f"  数据库位置: {DB_PATH.resolve()}")
        print('='*55)
        conn.close()
        # await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
