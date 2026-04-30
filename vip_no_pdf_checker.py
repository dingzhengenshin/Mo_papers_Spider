# -*- coding: utf-8 -*-
"""
维普论文 PDF 可下载性统计工具（极速版）
win + r chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\chrome_debug_profile"

核心思路：
  一页一次 page.evaluate(JS) 批量提取所有论文数据，
  不下载、不等待、不模拟行为，纯检测 + 统计。

数据库 download_status 状态体系：
  no_pdf   - 无下载按钮，不能下载
  has_pdf  - 有下载按钮，可下载
  ok       - 已下载成功（下载脚本写入）
  timeout  - 下载超时（下载脚本写入）
  error    - 下载出错（下载脚本写入）
  pending  - 待处理
"""

import argparse
import asyncio
import ctypes
import random
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# ════════════════════════════════════════════
# 配置区
# ════════════════════════════════════════════
CDP_URL     = "http://127.0.0.1:9222"
DB_PATH     = Path("./data/membrane_papers.db")
EXCEL_PATH  = Path("./质检省一级单位统计.xlsx")

# ── 拟人化延迟配置 ──
PAGE_READ_SLEEP     = (3.0, 4.5)   # 每页"阅读"停留
PAGE_TURN_SLEEP     = (3.0, 4.0)   # 翻页间隔
SCROLL_STEP_SLEEP   = (0.3, 0.6)   # 每次滚动间隔
SCROLL_STEPS_RANGE  = (3, 6)       # 每页滚动次数范围

# 一次 JS 提取整页数据的脚本
EXTRACT_JS = """
() => {
    const cards = document.querySelectorAll('#articlelist dl');
    const results = [];
    for (const dl of cards) {
        if (!dl.offsetParent) continue;  // 跳过隐藏
        const titleEl = dl.querySelector('dt a');
        const title = (titleEl?.innerText || '').trim();
        const href  = titleEl?.getAttribute('href') || '';

        // 检测下载按钮
        const links = dl.querySelectorAll('a');
        let hasPdf = false;
        for (const a of links) {
            if (a.textContent.includes('下载PDF')) { hasPdf = true; break; }
        }

        const author   = (dl.querySelector('dd.author')?.innerText || '').trim();
        const abstract = (dl.querySelector('dd.abstract, dd.summary, dd.remark')?.innerText || '').trim();
        const keywords = (dl.querySelector('dd.keyword, dd.subject')?.innerText || '').trim();
        const year     = (dl.querySelector('dd.year, dd.date')?.innerText || '').trim();
        const journal  = (dl.querySelector('dd.source')?.innerText || '').trim();

        results.push({ title, href, hasPdf, author, abstract, keywords, year, journal });
    }
    return results;
}
"""

# 翻页按钮检测 + 点击
NEXT_PAGE_JS = """
() => {
    const btns = document.querySelectorAll('a.next, a');
    for (const btn of btns) {
        if (!btn.textContent.includes('下一页')) continue;
        if (btn.disabled || btn.getAttribute('aria-disabled') === 'true'
            || (btn.className && btn.className.includes('disabled'))) return false;
        btn.scrollIntoView({ block: 'center' });
        btn.click();
        return true;
    }
    return false;
}
"""
# ════════════════════════════════════════════


# ── 菜单 & 参数 ─────────────────────────────

def load_institutions_from_excel():
    try:
        if not EXCEL_PATH.exists():
            print(f"[ERROR] Excel文件不存在: {EXCEL_PATH}")
            sys.exit(1)
        df = pd.read_excel(EXCEL_PATH)
        if len(df.columns) < 3:
            print("[ERROR] Excel文件格式错误：至少需要3列")
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
            choice = int(input("请选择省份（输入数字）: "))
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
            choice = int(input("请选择机构（输入数字）: "))
            if 1 <= choice <= len(institutions):
                return institutions[choice - 1]
            print(f"请输入 1-{len(institutions)} 之间的数字")
        except ValueError:
            print("请输入有效的数字")


def get_target_institution():
    parser = argparse.ArgumentParser(description="统计维普论文PDF可下载性")
    parser.add_argument("--institution", "-i", dest="inst", help="指定机构名称")
    args = parser.parse_args()
    if args.inst:
        return args.inst

    print("\n" + "=" * 50)
    print("维普论文 PDF 可下载性统计工具")
    print("=" * 50)
    d = load_institutions_from_excel()
    if not d:
        print("[ERROR] Excel文件中没有有效的机构数据")
        sys.exit(1)
    prov = show_province_menu(d)
    return show_institution_menu(d[prov], prov)


# ── 跨进程互斥体 ─────────────────────────────

_kernel32 = ctypes.windll.kernel32

class DBWriteGuard:
    def __init__(self, timeout_ms=10000):
        self._h = _kernel32.CreateMutexW(None, False, "Global\\VIPCrawlerDBMutex")
        self._t = timeout_ms

    def __enter__(self):
        r = _kernel32.WaitForSingleObject(self._h, self._t)
        if r not in (0, 0x80):
            raise TimeoutError(f"数据库写锁超时 ({self._t}ms)")
        return self

    def __exit__(self, *a):
        _kernel32.ReleaseMutex(self._h)

    def __del__(self):
        if self._h:
            _kernel32.CloseHandle(self._h)

_db_guard = DBWriteGuard()


# ── 数据库 ────────────────────────────────────

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


def batch_upsert(conn, rows):
    """batch write: mutex -> executemany -> commit -> release"""
    with _db_guard:
        conn.executemany("""
            INSERT INTO papers (title, authors, abstract_text, keywords, source_db,
                                publish_year, journal, download_link, pdf_local_path,
                                download_status, institution, scrape_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(title) DO UPDATE SET
                authors=excluded.authors, abstract_text=excluded.abstract_text,
                keywords=excluded.keywords, publish_year=excluded.publish_year,
                journal=excluded.journal, download_link=excluded.download_link,
                download_status=excluded.download_status,
                institution=excluded.institution, scrape_time=excluded.scrape_time
        """, rows)
        conn.commit()


# ── 拟人化行为模拟 ─────────────────────────────

async def simulate_reading(page):
    """simulate human reading: random scroll + mouse move"""
    steps = random.randint(*SCROLL_STEPS_RANGE)
    for _ in range(steps):
        dx = random.randint(-50, 50)
        dy = random.choice([-300, -200, 200, 300, 400])
        await page.mouse.wheel(dx, dy)
        await page.mouse.move(
            random.randint(200, 800),
            random.randint(100, 600),
            steps=random.randint(5, 12)
        )
        await asyncio.sleep(random.uniform(*SCROLL_STEP_SLEEP))


# ── 核心：一页一次 JS 提取 + 批量写库 ──────────

async def process_page(page, conn, page_num, institution_name):
    """simulate reading -> JS extract all -> batch write"""
    # 等卡片渲染
    try:
        await page.wait_for_selector("#articlelist dl", timeout=6000)
    except Exception:
        print(f"[WARN] 第{page_num}页：未找到卡片，跳过")
        return {"has_pdf": 0, "no_pdf": 0, "error": 0}

    # ★ 拟人化：模拟浏览滚动
    await simulate_reading(page)

    # 一次性 JS 提取
    papers = await page.evaluate(EXTRACT_JS)
    if not papers:
        print(f"[WARN] 第{page_num}页：提取到 0 篇，跳过")
        return {"has_pdf": 0, "no_pdf": 0, "error": 0}

    stats = {"has_pdf": 0, "no_pdf": 0, "error": 0}
    now = datetime.now().isoformat()
    batch_rows = []
    no_pdf_titles = []

    for p in papers:
        title = p["title"][:200] if p["title"] else f"第{page_num}页未命名"
        if not p["title"]:
            stats["error"] += 1
            continue

        status = "has_pdf" if p["hasPdf"] else "no_pdf"
        stats["has_pdf" if p["hasPdf"] else "no_pdf"] += 1

        batch_rows.append((
            title, p["author"], p["abstract"], p["keywords"],
            "维普", p["year"], p["journal"], p["href"],
            "", status, institution_name, now
        ))

        if not p["hasPdf"]:
            no_pdf_titles.append(title)

    # 批量写库
    if batch_rows:
        batch_upsert(conn, batch_rows)

    # 打印本页结果
    total = len(papers)
    print(f"\n  第 {page_num} 页: {total} 篇 | "
          f"可下载 {stats['has_pdf']}  不能下载 {stats['no_pdf']}")
    for t in no_pdf_titles:
        print(f"    X  {t}")

    # ★ 拟人化：页面"阅读"停留
    read_sleep = random.uniform(*PAGE_READ_SLEEP)
    print(f"  ...停留 {read_sleep:.1f}s")
    await asyncio.sleep(read_sleep)

    return stats


async def go_next_page(page):
    """JS 点击下一页"""
    try:
        clicked = await page.evaluate(NEXT_PAGE_JS)
        if not clicked:
            return False
        # 等新页卡片加载
        try:
            await page.wait_for_selector("#articlelist dl", timeout=8000)
        except Exception:
            pass
        return True
    except Exception:
        return False


# ── 主函数 ────────────────────────────────────

async def main():
    institution_name = get_target_institution()

    print("\n" + "=" * 50)
    print("请设置起始页码")
    print("=" * 50)
    page_num = 1
    page_input = input("当前浏览器所在页码（回车默认1）: ").strip()
    if page_input:
        try:
            page_num = int(page_input)
        except ValueError:
            print("无效数字，使用默认页码 1")

    conn = init_db(DB_PATH)

    print(f"\n[INFO] PDF可下载性统计 - {institution_name}")
    print(f"[INFO] 起始页: {page_num}  数据库: {DB_PATH.resolve()}")

    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"[ERR] 无法连接浏览器 ({CDP_URL})")
            print(f"      请确认 Chrome 已用 --remote-debugging-port=9222 启动")
            print(f"      {e}")
            conn.close()
            sys.exit(1)

        ctx = browser.contexts
        if not ctx:
            print("[ERR] 无浏览器上下文，请先打开维普搜索结果页")
            conn.close()
            sys.exit(1)

        pages = ctx[0].pages
        if not pages:
            print("[ERR] 无页面")
            conn.close()
            sys.exit(1)

        page = next((pg for pg in pages if "cqvip" in pg.url or "vpn" in pg.url), pages[0])
        print(f"[INFO] 页面: {page.url}\n")

        total = {"has_pdf": 0, "no_pdf": 0, "error": 0}
        cur_page = page_num

        try:
            while True:
                s = await process_page(page, conn, cur_page, institution_name)
                for k in total:
                    total[k] += s[k]

                if not await go_next_page(page):
                    break
                cur_page += 1
                # ★ 拟人化：翻页间隔
                turn_sleep = random.uniform(*PAGE_TURN_SLEEP)
                print(f"  ...翻页等待 {turn_sleep:.1f}s")
                await asyncio.sleep(turn_sleep)

        except KeyboardInterrupt:
            print(f"\n[中断] 停在第 {cur_page} 页")
        except Exception as e:
            print(f"\n[异常] 停在第 {cur_page} 页: {e}")
            raise

        finally:
            # ── 汇总报告 ──
            checked = total["has_pdf"] + total["no_pdf"]
            ratio = (total["no_pdf"] / checked * 100) if checked else 0

            # 从数据库查询该机构完整统计（含之前下载脚本的记录）
            db_stats = conn.execute("""
                SELECT download_status, COUNT(*) cnt FROM papers
                WHERE institution = ? GROUP BY download_status
            """, (institution_name,)).fetchall()

            # 不能下载的论文清单
            no_pdfs = conn.execute("""
                SELECT title, authors, journal, publish_year
                FROM papers
                WHERE institution = ? AND download_status = 'no_pdf'
                ORDER BY publish_year DESC, journal
            """, (institution_name,)).fetchall()

            status_zh = {
                "no_pdf": "不能下载", "has_pdf": "可下载",
                "ok": "已下载", "skip": "旧版跳过",
                "timeout": "下载超时", "error": "下载出错", "pending": "待处理"
            }

            print(f"\n{'=' * 65}")
            print(f"  {institution_name} - PDF 可下载性统计报告")
            print(f"{'=' * 65}")
            print(f"  本次扫描: 第 {page_num} ~ {cur_page} 页，共 {checked} 篇")
            print(f"  可下载: {total['has_pdf']}   不能下载: {total['no_pdf']}   "
                  f"不能下载占比: {ratio:.1f}%")
            print(f"{'─' * 65}")
            print(f"  数据库中该机构全部状态:")
            for r in db_stats:
                print(f"    {status_zh.get(r['download_status'], r['download_status']):　<8} {r['cnt']} 篇")
            print(f"{'─' * 65}")

            if no_pdfs:
                print(f"\n  不能下载的论文（{len(no_pdfs)} 篇）:")
                print(f"  {'─' * 61}")
                for idx, r in enumerate(no_pdfs, 1):
                    print(f"  {idx:>3}. [{r['publish_year'] or '?'}] {r['title']}")
                    auth = f"  作者: {r['authors']}" if r['authors'] else ""
                    jour = f"  期刊: {r['journal']}" if r['journal'] else ""
                    if auth or jour:
                        print(f"       {auth}  {jour}")
                print(f"  {'─' * 61}")

            print(f"\n  数据库: {DB_PATH.resolve()}")
            print(f"{'=' * 65}")
            conn.close()


if __name__ == "__main__":
    asyncio.run(main())
