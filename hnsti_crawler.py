# -*- coding: utf-8 -*-
"""
HNSTI 论文 PDF 下载器
chrome.exe --remote-debugging-port=9225 --user-data-dir="C:\chrome_debug_profile"

流程:
  列表页 -> 点击下载按钮(a.btnTitle.btnIsView.btn-l.down)
  -> 弹出对话框(Modal) -> 点击弹窗内第一个<a>链接(动态文本)
  -> 触发新标签页跳转(维普/万方等) 或 直接下载PDF
  -> 关闭弹窗/新标签 -> 回到列表页继续

与维普/知网爬虫共享数据库(data/membrane_papers.db)
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
CDP_URL              = "http://127.0.0.1:9225"
BASE_SAVE_DIR        = Path("./vip_pdfs")
DB_PATH              = Path("./data/membrane_papers.db")
EXCEL_PATH           = Path("./质检省一级单位统计.xlsx")
DOWNLOAD_TIMEOUT_MS  = 30_000
PAGE_LOAD_TIMEOUT_MS = 20_000
ELEM_WAIT_TIMEOUT_MS = 15_000
SLEEP_BETWEEN        = (3, 8)
MAX_CARD_RETRIES     = 3

# CSS selectors - HNSTI list page (based on actual DOM: dl.preview)
CARD_SELECTOR        = "dl.preview"
TITLE_SELECTOR       = "dt a.title"
DOWNLOAD_BTN_SEL     = "dd.source a.btnTitle.btnIsView.btn-l.down"
MODAL_SELECTOR       = "ul.download-list, .layui-layer, .layui-layer-content, [role='dialog'], .modal"
MODAL_LINK_SEL       = "ul.download-list li a[href]"
NEXT_PAGE_SELECTOR   = "span.page > span.page-num + a"
AUTHOR_SELECTOR      = "dd span.writer"
JOURNAL_SELECTOR     = "dd span.media a[title]"
YEAR_SELECTOR        = "dd span.media"
ABSTRACT_SELECTOR    = "dd span.abstract"
KEYWORD_SELECTOR     = "dd span.subject"
# ════════════════════════════════════════════


def load_institutions_from_excel():
    try:
        if not EXCEL_PATH.exists():
            print(f"[ERROR] Excel file not found: {EXCEL_PATH}")
            sys.exit(1)
        df = pd.read_excel(EXCEL_PATH)
        if len(df.columns) < 3:
            print("[ERROR] Excel needs at least 3 columns")
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
        print(f"[ERROR] read Excel failed: {e}")
        sys.exit(1)


def show_province_menu(institutions_dict):
    print("\n" + "=" * 60)
    print("province list")
    print("=" * 60)
    for i, province in enumerate(institutions_dict.keys(), 1):
        print(f"{i}. {province}")
    print("=" * 60)
    while True:
        try:
            choice = int(input("select province (number): "))
            provinces = list(institutions_dict.keys())
            if 1 <= choice <= len(provinces):
                return provinces[choice - 1]
            print(f"enter 1-{len(provinces)}")
        except ValueError:
            print("invalid number")


def show_institution_menu(institutions, province):
    print(f"\n" + "=" * 60)
    print(f"{province} - institution list")
    print("=" * 60)
    for i, inst in enumerate(institutions, 1):
        print(f"{i}. {inst}")
    print("=" * 60)
    while True:
        try:
            choice = int(input("select institution (number): "))
            if 1 <= choice <= len(institutions):
                return institutions[choice - 1]
            print(f"enter 1-{len(institutions)}")
        except ValueError:
            print("invalid number")


def get_target_institution():
    parser = argparse.ArgumentParser(description="HNSTI paper PDF downloader")
    parser.add_argument("--institution", "-i", dest="inst", help="institution name")
    args, _ = parser.parse_known_args()
    if args.inst:
        return args.inst

    print("\n" + "=" * 50)
    print("HNSTI paper PDF downloader")
    print("=" * 50)
    d = load_institutions_from_excel()
    if not d:
        print("[ERROR] no institution data")
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
    return row is not None and row["download_status"] == "ok"


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
        print(f"{tag}[WARN] networkidle timeout, fallback domcontentloaded...")
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
                print(f"[WARN] page {page_num}: {MAX_CARD_RETRIES} attempts failed, skip")
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


# -- core: click download btn -> modal -> link -> download --

async def download_via_modal(context, list_page, download_btn, save_dir, conn,
                              title, authors, abstract, keywords, journal, pub_year,
                              detail_url, institution_name):
    """
    1. click a.btnTitle.btnIsView.btn-l.down -> modal popup
    2. find first <a> in modal (dynamic text, do NOT match by text)
    3. click it -> may open new tab (vip/wanfang) OR direct download
    4. handle both cases
    """
    status = "error"
    local_path = ""

    # record pages before click
    pages_before = set(context.pages)

    # step 1: click download button to trigger modal
    try:
        await download_btn.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass

    try:
        await download_btn.evaluate("node => node.click()")
    except Exception as e:
        print(f"    [ERR] click download btn failed: {e}")
        return "error", ""

    # wait for modal animation
    await asyncio.sleep(0.5)

    # wait for modal / download-list to appear
    modal_found = False
    for sel in MODAL_SELECTOR.split(", "):
        sel = sel.strip()
        if not sel:
            continue
        try:
            await list_page.wait_for_selector(sel, timeout=5000)
            if await list_page.locator(sel).count() > 0:
                modal_found = True
                break
        except PlaywrightTimeoutError:
            continue

    if not modal_found:
        print(f"    [WARN] modal not found after clicking download btn")
        return "error", ""

    # wait for modal content to render
    await asyncio.sleep(0.3)

    # step 2: find link in ul.download-list (text is dynamic, match by structure only)
    link_loc = list_page.locator(MODAL_LINK_SEL)
    if await link_loc.count() == 0:
        print(f"    [WARN] no link found in download-list")
        try:
            await list_page.locator("a.layui-layer-close").click(timeout=2000)
        except Exception:
            pass
        return "skip", ""

    modal_link = link_loc.first

    link_text = await safe_text(modal_link, timeout=2000)
    print(f"    [MODAL] link: {link_text or 'unnamed'}")

    # step 3: click modal link -> may open new tab OR direct download
    # set up listeners for both outcomes
    download_event = None
    new_page_event = None

    async def on_download(d):
        nonlocal download_event
        download_event = d

    async def on_popup(p):
        nonlocal new_page_event
        new_page_event = p

    list_page.on("download", on_download)
    context.on("page", on_popup)

    try:
        await modal_link.evaluate("node => node.click()")
    except Exception as e:
        print(f"    [ERR] click modal link failed: {e}")
        list_page.remove_listener("download", on_download)
        return "error", ""

    # wait for new tab or download to trigger
    await asyncio.sleep(1.5)

    # outcome A: direct download triggered
    if download_event is not None:
        try:
            dest = save_dir / f"{title}.pdf"
            if dest.exists():
                dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
            await download_event.save_as(dest)
            local_path = str(dest.resolve())
            status = "ok"
            print(f"    [OK]  direct download -> {dest.name}")
        except Exception as e:
            print(f"    [ERR] save failed: {e}")
            status = "error"

    # outcome B: new tab opened (vip/wanfang/etc)
    elif new_page_event is not None:
        try:
            detail_page = new_page_event
            await wait_for_page_stable(detail_page, context="3rd-party page")
            print(f"    [TAB]  opened: {detail_page.url}")

            # check if this new page itself triggers a download
            inner_download = None

            async def on_inner_download(d):
                nonlocal inner_download
                inner_download = d

            detail_page.on("download", on_inner_download)
            await asyncio.sleep(3)

            if inner_download is not None:
                # download triggered from new tab
                try:
                    dest = save_dir / f"{title}.pdf"
                    if dest.exists():
                        dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
                    await inner_download.save_as(dest)
                    local_path = str(dest.resolve())
                    status = "ok"
                    print(f"    [OK]  downloaded from 3rd-party -> {dest.name}")
                except Exception as e:
                    print(f"    [ERR] save from 3rd-party failed: {e}")
                    status = "error"
            else:
                # no auto-download, try to find PDF link on the page
                # generic PDF link detection
                pdf_links = detail_page.locator("a[href*='.pdf'], a[href*='download'], a:has-text('PDF'), a:has-text('pdf'), a#pdfDown")
                count = await pdf_links.count()
                if count > 0:
                    try:
                        async with detail_page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
                            await pdf_links.first.evaluate("node => node.click()")
                        download = await dl_info.value
                        dest = save_dir / f"{title}.pdf"
                        if dest.exists():
                            dest = save_dir / f"{title}_{int(datetime.now().timestamp())}.pdf"
                        await download.save_as(dest)
                        local_path = str(dest.resolve())
                        status = "ok"
                        print(f"    [OK]  found PDF link -> {dest.name}")
                    except PlaywrightTimeoutError:
                        print(f"    [WARN] PDF download timeout in 3rd-party page")
                        status = "timeout"
                    except Exception as e:
                        print(f"    [ERR]  PDF download failed: {e}")
                        status = "error"
                else:
                    print(f"    [SKIP] no PDF link in 3rd-party page")
                    status = "skip"

        finally:
            try:
                await detail_page.close()
            except Exception:
                pass
            try:
                await list_page.bring_to_front()
            except Exception:
                pass
    else:
        print(f"    [WARN] no download and no new tab after modal link click")
        status = "error"

    # cleanup listeners
    list_page.remove_listener("download", on_download)

    # close modal via X button
    try:
        await list_page.locator("a.layui-layer-close").click(timeout=2000)
        await asyncio.sleep(0.8)
    except Exception:
        pass

    upsert_paper(conn, {
        "title": title, "authors": authors, "abstract_text": abstract,
        "keywords": keywords, "source_db": "HNSTI", "publish_year": pub_year,
        "journal": journal, "download_link": detail_url,
        "pdf_local_path": local_path, "download_status": status,
        "institution": institution_name,
        "scrape_time": datetime.now().isoformat(),
    })

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

        # extract metadata from card
        title_raw = await safe_text(card.locator(TITLE_SELECTOR), timeout=3000,
                                     default=f"page{page_num}_paper{i+1}")
        title = sanitize_filename(title_raw)
        authors    = await safe_text(card.locator(AUTHOR_SELECTOR))
        journal    = await safe_text(card.locator(JOURNAL_SELECTOR))
        pub_year   = await safe_text(card.locator(YEAR_SELECTOR))
        abstract   = await safe_text(card.locator(ABSTRACT_SELECTOR))
        keywords   = await safe_text(card.locator(KEYWORD_SELECTOR))
        detail_url = await safe_attr(card.locator(TITLE_SELECTOR), "href")

        print(f"  [{i+1}/{total}] {title}")

        # dedup
        if is_duplicate(conn, title, institution_name):
            print("    已重复")
            stats["duplicate"] += 1
            continue

        # find download button a.btnTitle.btnIsView.btn-l.down
        dl_btn = card.locator(DOWNLOAD_BTN_SEL).first
        btn_count = await dl_btn.count()
        if btn_count == 0:
            print("    [SKIP] no download button on this card")
            upsert_paper(conn, {
                "title": title, "authors": authors, "abstract_text": abstract,
                "keywords": keywords, "source_db": "HNSTI", "publish_year": pub_year,
                "journal": journal, "download_link": detail_url,
                "pdf_local_path": "", "download_status": "skip",
                "institution": institution_name,
                "scrape_time": datetime.now().isoformat(),
            })
            stats["skipped"] += 1
            continue

        # click btn -> modal -> link -> download
        status, local_path = await download_via_modal(
            context, list_page, dl_btn, save_dir, conn,
            title, authors, abstract, keywords, journal, pub_year,
            detail_url, institution_name
        )

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


async def get_current_page_num(page):
    """read current page number from a.current"""
    try:
        cur = page.locator("span.page-num a.current")
        if await cur.count() > 0:
            return int((await cur.first.inner_text()).strip())
    except Exception:
        pass
    return None


async def go_next_page(page):
    """call g_GetGotoPage() directly via JS, verify page changed"""
    try:
        cur_page = await get_current_page_num(page)
        if cur_page is None:
            print("[WARN] cannot read current page number")
            return False

        target_page = cur_page + 1

        # call the page's own JS function directly
        await page.evaluate(f"g_GetGotoPage('{target_page}')")

        # verify page changed: a.current text should equal target_page
        for attempt in range(3):
            await page.wait_for_load_state("domcontentloaded", timeout=8000)
            new_page = await get_current_page_num(page)
            if new_page == target_page:
                # also verify cards rendered
                try:
                    await page.wait_for_selector(CARD_SELECTOR, timeout=6000)
                except Exception:
                    pass
                return True
            if attempt < 2:
                await asyncio.sleep(1)

        print(f"[WARN] page did not change: still at {cur_page}")
        return False

    except Exception as e:
        print(f"[WARN] page turn failed: {type(e).__name__}: {e}")
        return False


# -- main --

async def main():
    # 0. 写入 PID 文件（供 ai_supervisor 检测进程状态）
    _pid_dir = Path("./data")
    _pid_dir.mkdir(parents=True, exist_ok=True)
    _pid_file = _pid_dir / f"{Path(__file__).stem}.pid"
    _pid_file.write_text(str(os.getpid()))

    institution_name = get_target_institution()

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

    print(f"\n[INFO] HNSTI downloader started")
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
            print(f"      start Chrome with --remote-debugging-port=9225")
            print(f"      {e}")
            conn.close()
            sys.exit(1)

        contexts = browser.contexts
        if not contexts:
            print("[ERR] no browser context, open HNSTI search page first")
            conn.close()
            sys.exit(1)

        context = contexts[0]
        pages = context.pages
        if not pages:
            print("[ERR] no pages")
            conn.close()
            sys.exit(1)

        # pick page with search results (dl.preview cards), skip homepage
        list_page = None
        for pg in reversed(pages):
            if "hnsti" in pg.url or "hnst" in pg.url:
                try:
                    if await pg.locator(CARD_SELECTOR).count() > 0:
                        list_page = pg
                        break
                except Exception:
                    continue
        if list_page is None:
            list_page = pages[0]
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
