# -*- coding: utf-8 -*-
"""
知网论文 PDF 下载器
chrome.exe --remote-debugging-port=9223 --user-data-dir="C:\chrome_debug_profile"

流程:
  列表页 -> 点击标题(a.fz14) -> 新标签页打开详情页
  -> 详情页点击 a#pdfDown 下载 PDF -> 关闭详情页 -> 回到列表页继续

与维普爬虫共享数据库(data/membrane_papers.db)
"""

import argparse
import asyncio
import ctypes
import os
import random
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

# ════════════════════════════════════════════
# 配置区
# ════════════════════════════════════════════
CDP_URL              = "http://127.0.0.1:9223"
BASE_SAVE_DIR        = Path("./cnki_pdfs")
DB_PATH              = Path("./data/membrane_papers.db")
EXCEL_PATH           = Path("./质检省一级单位统计.xlsx")
DOWNLOAD_TIMEOUT_MS  = 30_000
PAGE_LOAD_TIMEOUT_MS = 20_000
ELEM_WAIT_TIMEOUT_MS = 15_000
SLEEP_BETWEEN        = (3, 8)
MAX_CARD_RETRIES     = 3

# CSS selectors - CNKI list page
CARD_SELECTOR        = "#gridTable tbody tr"
TITLE_SELECTOR       = "a.fz14"
AUTHOR_SELECTOR      = "td.author, td:nth-child(3)"
JOURNAL_SELECTOR     = "td.source, td:nth-child(4)"
YEAR_SELECTOR        = "td.date, td:nth-child(2)"
NEXT_PAGE_SELECTOR   = "a.next, a#PageNext, a:has-text('Next'), a:has-text('next')"

# CSS selectors - CNKI detail page (备用，当前流程不进入详情页)
DETAIL_TITLE_SEL     = "h1, .title, #ChTitle"
DETAIL_AUTHOR_SEL    = ".author, #ChDivAuthor"
DETAIL_ABSTRACT_SEL  = "#ChDivSummary, .abstract"
DETAIL_KEYWORD_SEL   = "#ChDivKeyWord, .keyword"
DETAIL_JOURNAL_SEL   = ".top-tip a, .source, #ChDivSource"
# ════════════════════════════════════════════


def load_institutions_from_excel():
    try:
        if not EXCEL_PATH.exists():
            print(f"[ERROR] Excel文件不存在: {EXCEL_PATH}")
            sys.exit(1)
        df = pd.read_excel(EXCEL_PATH)
        if len(df.columns) < 3:
            print("[ERROR] Excel文件格式错误: 至少需要3列")
            sys.exit(1)
        df.columns = ['col1', 'province', 'institution'] + list(df.columns[3:])
        df = df[['province', 'institution']].dropna()
        institutions_dict = {}
        for _, row in df.iterrows():
            province = str(row['province']).strip()
            institution = str(row['institution']).strip()
            if province and institution:
                institutions_dict.setdefault(province, []).append(institution)
        return institutions_dict
    except Exception as e:
        print(f"[ERROR] 读取Excel文件失败: {e}")
        sys.exit(1)


def show_province_menu(institutions_dict):
    print("\n" + "=" * 60)
    print("省份列表")
    print("=" * 60)
    for i, province in enumerate(institutions_dict.keys(), 1):
        print(f"{i}. {province}")
    print("=" * 60)
    while True:
        try:
            choice = int(input("请选择省份(输入数字): "))
            provinces = list(institutions_dict.keys())
            if 1 <= choice <= len(provinces):
                return provinces[choice - 1]
            print(f"请输入 1-{len(provinces)} 之间的数字")
        except ValueError:
            print("请输入有效的数字")


def show_institution_menu(institutions, province):
    print(f"\n" + "=" * 60)
    print(f"{province} - 机构列表")
    print("=" * 60)
    for i, inst in enumerate(institutions, 1):
        print(f"{i}. {inst}")
    print("=" * 60)
    while True:
        try:
            choice = int(input("请选择机构(输入数字): "))
            if 1 <= choice <= len(institutions):
                return institutions[choice - 1]
            print(f"请输入 1-{len(institutions)} 之间的数字")
        except ValueError:
            print("请输入有效的数字")


def get_target_institution():
    parser = argparse.ArgumentParser(description="知网论文PDF下载")
    parser.add_argument("--institution", "-i", dest="inst", help="指定机构名称")
    args, _ = parser.parse_known_args()
    if args.inst:
        return args.inst

    print("\n" + "=" * 50)
    print("知网论文 PDF 下载工具")
    print("=" * 50)
    d = load_institutions_from_excel()
    if not d:
        print("[ERROR] Excel文件中没有有效的机构数据")
        sys.exit(1)
    prov = show_province_menu(d)
    return show_institution_menu(d[prov], prov)


# -- cross-process mutex --

_kernel32 = ctypes.windll.kernel32


class DBWriteGuard:
    def __init__(self, timeout_ms=10000):
        self._h = _kernel32.CreateMutexW(None, False, "Global\\VIPCrawlerDBMutex")
        self._t = timeout_ms

    def __enter__(self):
        r = _kernel32.WaitForSingleObject(self._h, self._t)
        if r not in (0, 0x80):
            raise TimeoutError(f"DB write lock timeout ({self._t}ms)")
        return self

    def __exit__(self, *a):
        _kernel32.ReleaseMutex(self._h)

    def __del__(self):
        if self._h:
            _kernel32.CloseHandle(self._h)


_db_guard = DBWriteGuard()


# -- database --

def init_db(db_path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

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
            institution          TEXT,
            scrape_time          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(papers)")
    cols = {r[1] for r in cur.fetchall()}
    if "institution" not in cols:
        cur.execute("ALTER TABLE papers ADD COLUMN institution TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_papers_institution ON papers(institution)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_papers_status ON papers(download_status)")
    conn.commit()
    return conn


def is_duplicate(conn, title, institution=None):
    if institution:
        row = conn.execute(
            "SELECT download_status FROM papers WHERE title = ? AND institution = ?",
            (title, institution)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT download_status FROM papers WHERE title = ?",
            (title,)
        ).fetchone()
    return row is not None and row["download_status"] in ("ok", "skip")


def upsert_paper(conn, data):
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    updates = ", ".join(f"{k}=excluded.{k}" for k in data if k != "title")
    with _db_guard:
        conn.execute(
            f"INSERT INTO papers ({cols}) VALUES ({placeholders})"
            f" ON CONFLICT(title) DO UPDATE SET {updates}",
            list(data.values())
        )
        conn.commit()


# -- helpers --

def sanitize_filename(name):
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', '_', name)
    return name.strip()[:200] or "unnamed"


async def safe_text(locator, timeout=3000, default=""):
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        txt = (await locator.first.inner_text(timeout=timeout)).strip()
        if txt:
            return txt
    except Exception:
        pass
    try:
        txt = await locator.first.evaluate("node => (node.innerText || node.textContent || '').trim()")
        if txt:
            return txt
    except Exception:
        pass
    return default


async def safe_attr(locator, attr, timeout=3000, default=""):
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        val = await locator.first.get_attribute(attr, timeout=timeout)
        return (val or default).strip()
    except Exception:
        return default


async def wait_for_page_stable(page, context=""):
    tag = f"[{context}] " if context else ""
    try:
        await page.wait_for_load_state("networkidle", timeout=PAGE_LOAD_TIMEOUT_MS)
        return True
    except PlaywrightTimeoutError:
        print(f"{tag}[WARN] networkidle timeout, fallback to domcontentloaded...")
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=10000)
        return True
    except PlaywrightTimeoutError:
        print(f"{tag}[WARN] domcontentloaded also timeout, force continue...")
        return False


async def wait_for_cards(page, page_num):
    for attempt in range(1, MAX_CARD_RETRIES + 1):
        try:
            await page.wait_for_selector(CARD_SELECTOR, timeout=ELEM_WAIT_TIMEOUT_MS)
            if await page.locator(CARD_SELECTOR).count() > 0:
                return True
            raise PlaywrightTimeoutError("count=0")
        except PlaywrightTimeoutError:
            if attempt < MAX_CARD_RETRIES:
                wait_s = 2 * attempt + random.uniform(1, 3)
                print(f"[WARN] page {page_num} attempt {attempt} card wait timeout, retry in {wait_s:.1f}s...")
                await asyncio.sleep(wait_s)
            else:
                print(f"[WARN] page {page_num}: {MAX_CARD_RETRIES} attempts all failed, skip")
                return False
    return False


async def simulate_human_behavior(page, target_locator):
    try:
        box = await target_locator.bounding_box()
        if not box:
            return
        target_x = box["x"] + random.uniform(10, max(18, box["width"] - 10))
        target_y = box["y"] + random.uniform(6, max(14, box["height"] - 6))
        start_x = max(0, target_x + random.uniform(-80, 80))
        start_y = max(0, target_y + random.uniform(-50, 50))
        await page.mouse.move(start_x, start_y)
        await page.mouse.move(target_x, target_y, steps=random.randint(10, 25))
        await page.mouse.wheel(0, random.choice([-200, 150, 250, -100]))
        await asyncio.sleep(random.uniform(0.8, 1.5))
    except Exception:
        return


# -- core: download one paper from list page card --

async def download_paper(context, list_page, card, save_dir, conn,
                          title, institution_name, title_link):
    """
    知网镜像站：直接点击标题 a.fz14 触发下载。
    必须用 JS 移除 target 属性后强制点击，避免标准 click() 被遮挡/新标签页拦截。
    """
    status = "error"
    local_path = ""
    href_raw = ""

    pages_before = set(context.pages)

    try:
        try:
            await title_link.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass

        href_raw = await safe_attr(title_link, "href", timeout=3000)

        # 核心点击：JS 移除 target="_blank"，强制底层 click
        download_caught = False
        new_tab = None

        async with list_page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_guard:
            async with context.expect_page(timeout=DOWNLOAD_TIMEOUT_MS) as page_guard:
                # 关键：移除 target 属性后强制点击
                await title_link.evaluate(
                    "node => { node.removeAttribute('target'); node.click(); }"
                )
                await asyncio.sleep(2)

            # 检查是否意外打开了新标签页
            try:
                new_tab = await page_guard.value
            except Exception:
                pass

        # 检查下载是否触发
        try:
            download = await dl_guard.value
            dest = save_dir / f"{title}.pdf"
            if dest.exists():
                dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
            await download.save_as(dest)
            local_path = str(dest.resolve())
            status = "ok"
            print(f"    [OK]  saved -> {dest.name}")
            download_caught = True
        except Exception:
            pass

        # 如果没有触发下载但打开了新标签页，尝试从中获取
        if not download_caught and new_tab is not None:
            print(f"    [TAB] 新标签页打开，尝试从中下载...")
            try:
                await wait_for_page_stable(new_tab, context="download tab")
                try:
                    async with new_tab.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl2:
                        await new_tab.reload()
                    download = await dl2.value
                    dest = save_dir / f"{title}.pdf"
                    if dest.exists():
                        dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
                    await download.save_as(dest)
                    local_path = str(dest.resolve())
                    status = "ok"
                    print(f"    [OK]  saved via tab -> {dest.name}")
                except Exception:
                    tab_url = new_tab.url
                    if "/download/" in tab_url or "/kns8/" in tab_url:
                        resp = await new_tab.context.request.get(tab_url)
                        if resp.ok:
                            body = await resp.body()
                            dest = save_dir / f"{title}.pdf"
                            if dest.exists():
                                dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
                            dest.write_bytes(body)
                            local_path = str(dest.resolve())
                            status = "ok"
                            print(f"    [OK]  fetched via tab -> {dest.name}")
                        else:
                            print(f"    [ERR]  tab fetch HTTP {resp.status}")
                    else:
                        print(f"    [SKIP] tab URL not download: {tab_url[:60]}")
                        status = "skip"
            finally:
                try:
                    await new_tab.close()
                except Exception:
                    pass

        if not download_caught and new_tab is None and status == "error":
            print(f"    [ERR]  no download and no new tab")

        upsert_paper(conn, {
            "title": title, "authors": "", "abstract_text": "",
            "keywords": "", "source_db": "知网", "publish_year": "",
            "journal": "", "download_link": href_raw,
            "pdf_local_path": local_path, "download_status": status,
            "institution": institution_name,
            "scrape_time": datetime.now().isoformat(),
        })

    finally:
        # 确保意外打开的标签页都被关闭
        pages_after = context.pages
        new_pages = [p for p in pages_after if p not in pages_before]
        for p in new_pages:
            try:
                await p.close()
            except Exception:
                pass

    return status, local_path


# -- core: process one list page --

async def process_page(list_page, context, save_dir, conn, page_num, institution_name):
    stats = {"downloaded": 0, "skipped": 0, "duplicate": 0, "timeout": 0, "error": 0}

    await wait_for_page_stable(list_page, context=f"page {page_num}")
    if not await wait_for_cards(list_page, page_num):
        return stats

    cards = list_page.locator(CARD_SELECTOR)
    total = await cards.count()
    if total == 0:
        print(f"[WARN] page {page_num}: 0 cards, skip")
        return stats
    print(f"\n[INFO] page {page_num}: {total} papers ({institution_name})")

    for i in range(total):
        card = cards.nth(i)

        try:
            await card.scroll_into_view_if_needed(timeout=5000)
        except Exception:
            pass

        await simulate_human_behavior(list_page, card)

        try:
            await card.wait_for(state="attached", timeout=5000)
        except PlaywrightTimeoutError:
            print(f"  [{i+1}/{total}] [WARN] card DOM not ready, skip")
            stats["error"] += 1
            continue

        # extract title via a.fz14
        title_link = card.locator(TITLE_SELECTOR).first
        title_count = await title_link.count()
        if title_count == 0:
            print(f"  [{i+1}/{total}] [WARN] no title link a.fz14, skip")
            stats["error"] += 1
            continue

        title_raw = await safe_text(card.locator(TITLE_SELECTOR), timeout=3000,
                                     default=f"page{page_num}_paper{i+1}")
        title = sanitize_filename(title_raw)
        print(f"  [{i+1}/{total}] {title}")

        # dedup
        if is_duplicate(conn, title, institution_name):
            print("    [DUP]  already exists, skip")
            stats["duplicate"] += 1
            continue

        # extract data-url from card and download directly
        status, local_path = await download_paper(
            context, list_page, card, save_dir, conn, title, institution_name,
            title_link
        )

        # verify list page is still valid (not navigated away)
        try:
            alive = await list_page.locator(CARD_SELECTOR).count() > 0
        except Exception:
            alive = False
        if not alive:
            # list page lost, try to recover
            print(f"  [WARN] list page lost, trying to recover...")
            recovered = False
            for pg in context.pages:
                try:
                    if await pg.locator(CARD_SELECTOR).count() > 0:
                        list_page = pg
                        cards = list_page.locator(CARD_SELECTOR)
                        total = await cards.count()
                        print(f"  [INFO] recovered list page: {pg.url}")
                        recovered = True
                        break
                except Exception:
                    continue
            if not recovered:
                print(f"  [ERR] cannot recover list page, aborting current page")
                break

        if status == "ok":
            stats["downloaded"] += 1
        elif status == "skip":
            stats["skipped"] += 1
        elif status == "timeout":
            stats["timeout"] += 1
        else:
            stats["error"] += 1

        # sleep between papers
        sleep_sec = random.uniform(*SLEEP_BETWEEN)
        print(f"    ...wait {sleep_sec:.1f}s")
        await asyncio.sleep(sleep_sec)

    return stats


async def go_next_page(page):
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
        await wait_for_page_stable(page, context="after page turn")
        return True
    except Exception as e:
        print(f"[WARN] page turn failed: {type(e).__name__}: {e}")
        return False


# -- main --

async def main():
    institution_name = get_target_institution()

    # 0. 写入 PID 文件（供 ai_supervisor 检测进程状态）
    _pid_dir = Path("./data")
    _pid_dir.mkdir(parents=True, exist_ok=True)
    _pid_file = _pid_dir / f"{Path(__file__).stem}.pid"
    _pid_file.write_text(str(os.getpid()))

    # 解析 --page 参数
    _page_arg = 0
    for idx, arg in enumerate(sys.argv):
        if arg in ("--page", "-p") and idx + 1 < len(sys.argv):
            try:
                _page_arg = int(sys.argv[idx + 1])
            except ValueError:
                pass
            break

    if _page_arg >= 1:
        page_num = _page_arg
    else:
        print("\n" + "=" * 50)
        print("set start page")
        print("=" * 50)
        page_input = input("current browser page number (enter for 1): ").strip()
        page_num = 1
        if page_input:
            try:
                page_num = int(page_input)
            except ValueError:
                print("invalid, using page 1")

    save_dir = BASE_SAVE_DIR / institution_name
    save_dir.mkdir(parents=True, exist_ok=True)
    conn = init_db(DB_PATH)

    print(f"\n[INFO] CNKI downloader started")
    print(f"[INFO] institution: {institution_name}")
    print(f"[INFO] start page: {page_num}")
    print(f"[INFO] save dir: {save_dir.resolve()}")
    print(f"[INFO] database: {DB_PATH.resolve()}")
    print(f"[INFO] CDP: {CDP_URL}")

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"[ERR] cannot connect browser ({CDP_URL})")
            print(f"      start Chrome with --remote-debugging-port=9223")
            print(f"      {e}")
            conn.close()
            sys.exit(1)

        contexts = browser.contexts
        if not contexts:
            print("[ERR] no browser context, open CNKI search page first")
            conn.close()
            sys.exit(1)

        context = contexts[0]
        pages = context.pages
        if not pages:
            print("[ERR] no pages")
            conn.close()
            sys.exit(1)

        list_page = next(
            (pg for pg in pages if "cnki" in pg.url or "kns" in pg.url),
            pages[0]
        )
        print(f"[INFO] using page: {list_page.url}")

        total_stats = {"downloaded": 0, "skipped": 0, "duplicate": 0, "timeout": 0, "error": 0}
        current_page_num = page_num

        try:
            while True:
                stats = await process_page(
                    list_page, context, save_dir, conn, current_page_num, institution_name
                )
                for k in total_stats:
                    total_stats[k] += stats[k]
                print(
                    f"[INFO] page {current_page_num} done | "
                    f"dl {stats['downloaded']}  skip {stats['skipped']}  "
                    f"dup {stats['duplicate']}  timeout {stats['timeout']}  err {stats['error']}"
                )

                has_next = await go_next_page(list_page)
                if not has_next:
                    print("\n[INFO] last page reached, all done!")
                    break
                current_page_num += 1
                print(f"[INFO] turning to page {current_page_num}...")
                page_wait = random.uniform(4, 8)
                print(f"[INFO] page turn wait {page_wait:.1f}s...")
                await asyncio.sleep(page_wait)

        except KeyboardInterrupt:
            print(f"\n{'='*70}")
            print(f" interrupted! bookmark: [{institution_name}] page [{current_page_num}]")
            print(f"{'='*70}")
        except Exception as e:
            print(f"\n{'='*70}")
            print(f" error! bookmark: [{institution_name}] page [{current_page_num}]")
            print(f" {type(e).__name__}: {e}")
            print(f"{'='*70}")
            raise
        finally:
            print(f"\n{'='*55}")
            print(f"  {institution_name} - done!")
            print(f"  downloaded : {total_stats['downloaded']}")
            print(f"  skipped    : {total_stats['skipped']}")
            print(f"  duplicate  : {total_stats['duplicate']}")
            print(f"  timeout    : {total_stats['timeout']}")
            print(f"  error      : {total_stats['error']}")
            print(f"  PDF dir    : {save_dir.resolve()}")
            print(f"  database   : {DB_PATH.resolve()}")
            print(f"{'='*55}")
            conn.close()


if __name__ == "__main__":
    asyncio.run(main())
