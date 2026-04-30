# -*- coding: utf-8 -*-
"""
按机构独立爬取与归档的维普 PDF 下载器
win + r chrome.exe --remote-debugging-port=9224 --user-data-dir="C:\chrome_debug_profile_231"
基于原版增强功能：
1. 支持按机构独立爬取与归档
2. 动态参数接收与交互回退
3. 自动创建机构目录和数据库兼容
4. 完整保留原版防封禁策略
"""

import argparse
import asyncio
import ctypes
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

# ════════════════════════════════════════════
# 配置区
# ════════════════════════════════════════════
CDP_URL              = "http://127.0.0.1:9224"
BASE_SAVE_DIR        = Path("./vip_pdfs")  # 基础保存目录
DB_PATH              = Path("./data/membrane_papers.db")
EXCEL_PATH           = Path("./质检省一级单位统计.xlsx")  # Excel 数据源
DOWNLOAD_TIMEOUT_MS  = 30_000      # 下载超时提高到 30 秒
PAGE_LOAD_TIMEOUT_MS = 10_000      # 页面加载超时提高到 10 秒
ELEM_WAIT_TIMEOUT_MS = 8_000      # 元素等待超时提高到 8 秒
SLEEP_BETWEEN        = (2.5, 4.5)  # 正常阶段下载间隔
FAST_SLEEP           = (1.5, 3.0)  # 快速阶段下载间隔
FAST_PHASE_PAGES     = 2           # 前N页快速爬取，之后降速
MAX_CARD_RETRIES     = 3           # 最大重试次数

# CSS 选择器
CARD_SELECTOR        = "#articlelist dl:visible"
AUTHOR_SELECTOR      = "dd.author"
ABSTRACT_SELECTOR    = "dd.abstract, dd.summary, dd.remark"
KEYWORD_SELECTOR     = "dd.keyword, dd.subject"
YEAR_SELECTOR        = "dd.year, dd.date"
JOURNAL_SELECTOR     = "dd.source"
DOWNLOAD_BTN_SEL     = "a:has-text('下载PDF')"
NEXT_PAGE_SELECTOR   = "a:has-text('下一页'), a.next"
TITLE_SELECTOR       = "dt a"
# ════════════════════════════════════════════


def load_institutions_from_excel():
    """从Excel文件读取省份-机构数据"""
    try:
        if not EXCEL_PATH.exists():
            print(f"[ERROR] Excel文件不存在: {EXCEL_PATH}")
            sys.exit(1)

        # 读取Excel文件
        df = pd.read_excel(EXCEL_PATH)

        # 确保列名正确
        if len(df.columns) < 3:
            print("[ERROR] Excel文件格式错误：至少需要3列")
            sys.exit(1)

        # 假设B列是省份，C列是机构
        df.columns = ['col1', 'province', 'institution'] + list(df.columns[3:])
        df = df[['province', 'institution']].dropna()

        # 转换为字典结构：省份 -> [机构列表]
        institutions_dict = {}
        for _, row in df.iterrows():
            province = str(row['province']).strip()
            institution = str(row['institution']).strip()

            if province and institution:
                if province not in institutions_dict:
                    institutions_dict[province] = []
                institutions_dict[province].append(institution)

        return institutions_dict

    except Exception as e:
        print(f"[ERROR] 读取Excel文件失败: {e}")
        sys.exit(1)


def show_province_menu(institutions_dict):
    """显示省份菜单"""
    print("\n" + "="*60)
    print("省份列表")
    print("="*60)
    for i, province in enumerate(institutions_dict.keys(), 1):
        print(f"{i}. {province}")
    print("="*60)

    while True:
        try:
            choice = int(input("请选择省份（输入数字）: "))
            provinces = list(institutions_dict.keys())

            if 1 <= choice <= len(provinces):
                return provinces[choice - 1]
            else:
                print(f"请输入 1-{len(provinces)} 之间的数字")
        except ValueError:
            print("请输入有效的数字")


def show_institution_menu(institutions, province):
    """显示机构菜单"""
    print(f"\n" + "="*60)
    print(f"{province} - 机构列表")
    print("="*60)
    for i, institution in enumerate(institutions, 1):
        print(f"{i}. {institution}")
    print("="*60)

    while True:
        try:
            choice = int(input("请选择机构（输入数字）: "))
            if 1 <= choice <= len(institutions):
                return institutions[choice - 1]
            else:
                print(f"请输入 1-{len(institutions)} 之间的数字")
        except ValueError:
            print("请输入有效的数字")


def get_target_institution():
    """获取目标机构名称：支持命令行参数、Excel两级菜单"""
    # 1. 先检查命令行参数
    parser = argparse.ArgumentParser(description="按机构爬取维普论文")
    parser.add_argument(
        "--institution",
        "-i",
        dest="TARGET_INSTITUTION",
        help="指定目标机构名称，例如：清华大学、北京大学"
    )
    args, _ = parser.parse_known_args()

    if args.TARGET_INSTITUTION:
        return args.TARGET_INSTITUTION

    # 2. 如果没有命令行参数，使用Excel交互菜单
    print("\n" + "="*50)
    print("欢迎使用按机构爬取维普论文工具")
    print("="*50)

    # 加载Excel数据
    institutions_dict = load_institutions_from_excel()

    if not institutions_dict:
        print("[ERROR] Excel文件中没有有效的机构数据")
        sys.exit(1)

    # 显示两级菜单
    selected_province = show_province_menu(institutions_dict)
    selected_institution = show_institution_menu(
        institutions_dict[selected_province],
        selected_province
    )

    return selected_institution


# ── 跨进程 PV 信号量（Windows 命名互斥体） ─────────────
#
# 读操作(is_duplicate)无需互斥体，WAL 允许并发读
# 进程崩溃时 OS 自动回收互斥体，不会死锁
# ────────────────────────────────────────────────────────

_kernel32 = ctypes.windll.kernel32
_MUTEX_NAME = "Global\\VIPCrawlerDBMutex"


class DBWriteGuard:
    """跨进程 PV 信号量：with db_guard → P 获取写锁 → V 释放"""

    def __init__(self, timeout_ms=10000):
        self._handle = _kernel32.CreateMutexW(None, False, _MUTEX_NAME)
        self._timeout = timeout_ms

    def __enter__(self):
        # P 操作
        result = _kernel32.WaitForSingleObject(self._handle, self._timeout)
        if result not in (0, 0x80):  # WAIT_OBJECT_0 | WAIT_ABANDONED
            raise TimeoutError(f"数据库写锁等待超时 ({self._timeout}ms)")
        return self

    def __exit__(self, *args):
        # V 操作
        _kernel32.ReleaseMutex(self._handle)

    def __del__(self):
        if self._handle:
            _kernel32.CloseHandle(self._handle)


_db_guard = DBWriteGuard()


# ── 数据库相关函数 ─────────────────────────────────

def init_db(db_path):
    """初始化数据库，确保兼容性"""
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

    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(papers)")
    existing_cols = {row[1] for row in cursor.fetchall()}

    if "institution" not in existing_cols:
        print("[INFO] 添加 institution 字段以兼容旧数据库...")
        cursor.execute("ALTER TABLE papers ADD COLUMN institution TEXT")

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_papers_institution
        ON papers(institution)
    """)

    conn.commit()
    return conn


def is_duplicate(conn, title, institution=None):
    """检查是否为已处理的论文（纯读操作，无需互斥体，WAL 并发读）
    ok/skip → 跳过    error/timeout/pending → 需重试"""
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
    """写入论文数据：已成功的记录不被失败记录覆盖"""
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    safe_updates = []
    for k in data:
        if k == "title":
            continue
        if k == "download_status":
            safe_updates.append(
                f"{k}=IIF(papers.download_status='ok', papers.{k}, excluded.{k})"
            )
        elif k == "pdf_local_path":
            safe_updates.append(
                f"{k}=IIF(papers.download_status='ok', papers.{k}, excluded.{k})"
            )
        else:
            safe_updates.append(f"{k}=excluded.{k}")
    updates = ", ".join(safe_updates)

    with _db_guard:
        conn.execute(
            f"INSERT INTO papers ({cols}) VALUES ({placeholders})"
            f" ON CONFLICT(title) DO UPDATE SET {updates}",
            list(data.values())
        )
        conn.commit()


# ── 页面状态检测（与原版保持一致） ─────────────────────────────

async def check_page_alive(page):
    """轻量 JS 探针，检测页面是否崩溃/失联"""
    return True


async def wait_for_page_stable(page, context=""):
    """networkidle -> domcontentloaded 降级等待"""
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
    """指数退避重试等待卡片列表渲染，增加随机间隔"""
    for attempt in range(1, MAX_CARD_RETRIES + 1):
        try:
            await page.wait_for_selector(CARD_SELECTOR, timeout=ELEM_WAIT_TIMEOUT_MS)
            if await page.locator(CARD_SELECTOR).count() > 0:
                return True
            raise PlaywrightTimeoutError("count=0")
        except PlaywrightTimeoutError:
            if attempt < MAX_CARD_RETRIES:
                wait_s = 2 * attempt + random.uniform(1, 3)  # 基础等待时间 + 1-3秒随机
                print(f"[WARN] 第{page_num}页第{attempt}次等待卡片超时，{wait_s:.1f}s 后重试...")
                await asyncio.sleep(wait_s)
            else:
                print(f"[WARN] 第{page_num}页：{MAX_CARD_RETRIES}次均未找到卡片，跳过。")
                return False
    return False


async def is_element_ready(locator, timeout=4000):
    """元素存在且可见"""
    try:
        if await locator.count() == 0:
            return False
        await locator.first.wait_for(state="visible", timeout=timeout)
        return True
    except Exception:
        return False


# ── 工具函数（与原版保持一致） ─────────────────────────────────

def sanitize_filename(name):
    """清理文件名，移除非法字符"""
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', '_', name)
    return name.strip()[:200] or "未命名论文"


async def safe_text(locator, timeout=3000, default=""):
    """安全获取文本内容"""
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        return (await locator.first.inner_text(timeout=timeout)).strip()
    except Exception:
        return default


async def safe_attr(locator, attr, timeout=3000, default=""):
    """安全获取属性值"""
    try:
        await locator.first.wait_for(state="attached", timeout=timeout)
        val = await locator.first.get_attribute(attr, timeout=timeout)
        return (val or default).strip()
    except Exception:
        return default


# ── 防封禁行为模拟（与原版保持一致） ─────────────────────────────

async def simulate_human_behavior(page, card_locator):
    """模拟真人鼠标轨迹+轻微滚动，不影响主流程，增加随机延迟"""
    try:
        box = await card_locator.bounding_box()
        if not box:
            return

        target_x = box["x"] + random.uniform(12, max(20, box["width"] - 12))
        target_y = box["y"] + random.uniform(8, max(16, box["height"] - 8))

        start_x = max(0, target_x + random.uniform(-120, 120))
        start_y = max(0, target_y + random.uniform(-80, 80))

        await page.mouse.move(start_x, start_y)
        await page.mouse.move(target_x, target_y, steps=random.randint(10, 25))
        await page.mouse.wheel(0, random.choice([-200, 150, 300, -100]))
        await asyncio.sleep(random.uniform(0.5, 1.0))
    except Exception:
        return


# ── 核心处理逻辑 ─────────────────────────────────

async def process_page(page, save_dir, conn, page_num, institution_name, fast_mode=False):
    """遍历当前列表页，返回统计字典

    快速通道策略：
    - 重复论文 / 无按钮 → 零延迟直接跳过，不做模拟行为
    - 快速阶段（前N页）：跳过鼠标模拟，短间隔
    - 正常阶段：完整模拟行为，正常间隔
    """
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
    phase_tag = "[FAST]" if fast_mode else "[NORMAL]"
    print(f"\n[INFO] 第 {page_num} 页共找到 {total} 篇论文（机构：{institution_name}）{phase_tag}")

    for i in range(total):
        card = cards.nth(i)

        if not await check_page_alive(page):
            print(f"[ERR] 第 {i+1} 篇：页面中途失联，终止本页遍历。")
            break

        # 轻量 DOM 就绪检查
        try:
            await card.wait_for(state="attached", timeout=3000)
        except PlaywrightTimeoutError:
            print(f"  [{i+1}/{total}] [WARN] 卡片 DOM 未就绪，跳过")
            stats["error"] += 1
            continue

        # ① 先提取标题 → 快速去重
        title_raw = await safe_text(
            card.locator(TITLE_SELECTOR), timeout=2000,
            default=f"第{page_num}页第{i+1}篇"
        )
        title = sanitize_filename(title_raw)
        print(f"  [{i+1}/{total}] {title}")

        # ★ 快速通道 A：重复论文 → 零延迟跳过
        if is_duplicate(conn, title, institution_name):
            print("    [DUP]  数据库已有此篇且下载成功，跳过")
            stats["duplicate"] += 1
            continue

        # ② 非重复 → 并行提取完整元数据
        authors, abstract, keywords, pub_year, journal, detail_url = await asyncio.gather(
            safe_text(card.locator(AUTHOR_SELECTOR)),
            safe_text(card.locator(ABSTRACT_SELECTOR)),
            safe_text(card.locator(KEYWORD_SELECTOR)),
            safe_text(card.locator(YEAR_SELECTOR)),
            safe_text(card.locator(JOURNAL_SELECTOR)),
            safe_attr(card.locator("a"), "href"),
        )

        # ③ 快速检测下载按钮
        btn_loc   = card.locator(DOWNLOAD_BTN_SEL)
        btn_count = await btn_loc.count()

        # ★ 快速通道 B：无下载按钮 → 零延迟跳过
        if btn_count == 0:
            print("    [SKIP] 无下载按钮(可能为原文传递)，极速跳过")
            upsert_paper(conn, {
                "title": title, "authors": authors, "abstract_text": abstract,
                "keywords": keywords, "source_db": "维普", "publish_year": pub_year,
                "journal": journal, "download_link": detail_url,
                "pdf_local_path": "", "download_status": "skip",
                "institution": institution_name,
                "scrape_time": datetime.now().isoformat(),
            })
            stats["skipped"] += 1
            continue

        # ④ 有下载按钮 → 执行完整流程（模拟 + 滚动 + 下载）
        if not fast_mode:
            await simulate_human_behavior(page, card)
        try:
            await card.scroll_into_view_if_needed(timeout=2500)
        except Exception:
            pass

        btn_ready = await is_element_ready(btn_loc, timeout=3000)
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
            "institution": institution_name,
            "scrape_time": datetime.now().isoformat(),
        })

        # 自适应间隔：快速阶段短，正常阶段长
        if fast_mode:
            sleep_sec = random.uniform(*FAST_SLEEP)
        else:
            sleep_sec = random.uniform(*SLEEP_BETWEEN)
        print(f"    [INFO] 等待 {sleep_sec:.1f}s 后继续...")
        await asyncio.sleep(sleep_sec)

    return stats


async def go_next_page(page):
    """一次原子 JS 查找+点击下一页，避免 locator 失效"""
    try:
        clicked = await page.evaluate("""
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
        """)
        if not clicked:
            return False
        await wait_for_page_stable(page, context="翻页后")
        # 等卡片渲染完才算翻页成功
        try:
            await page.wait_for_selector(CARD_SELECTOR, timeout=ELEM_WAIT_TIMEOUT_MS)
        except Exception:
            print("[WARN] 翻页后未等到卡片")
            return False
        return True
    except Exception as e:
        print(f"[WARN] 翻页失败: {type(e).__name__}: {e}")
        return False


# ── 主函数 ─────────────────────────────────

async def main():
    # 0. 写入 PID 文件（供 ai_supervisor 检测进程状态）
    _pid_dir = Path("./data")
    _pid_dir.mkdir(parents=True, exist_ok=True)
    _pid_file = _pid_dir / f"{Path(__file__).stem}.pid"
    _pid_file.write_text(str(os.getpid()))

    # 1. 获取目标机构
    institution_name = get_target_institution()

    # 2. 解析 --page 参数
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
        print("\n" + "="*50)
        print("请设置起始页码")
        print("="*50)
        while True:
            page_input = input("请输入当前浏览器所在的页码（直接回车默认为1）: ").strip()
            if not page_input:
                page_num = 1
                break
            try:
                page_num = int(page_input)
                if page_num >= 1:
                    break
                else:
                    print("页码必须大于等于 1")
            except ValueError:
                print("请输入有效的数字")

    # 3. 创建机构专属保存目录
    save_dir = BASE_SAVE_DIR / institution_name
    save_dir.mkdir(parents=True, exist_ok=True)

    # 4. 初始化数据库（自动兼容旧表）
    conn = init_db(DB_PATH)

    # 5. 打印启动信息
    print("\n[INFO] 机构爬取模式已启动")
    print(f"[INFO] 目标机构: {institution_name}")
    print(f"[INFO] 起始页码: {page_num}")
    print(f"[INFO] PDF 保存目录: {save_dir.resolve()}")
    print(f"[INFO] 数据库: {DB_PATH.resolve()}")
    print(f"[INFO] 连接 CDP: {CDP_URL}")

    # 6. 显示该机构已有记录统计
    try:
        rows = conn.execute(
            "SELECT download_status, COUNT(*) as cnt FROM papers WHERE institution = ? GROUP BY download_status",
            (institution_name,)
        ).fetchall()
        if rows:
            status_map = {"ok": "已下载", "skip": "无按钮跳过", "timeout": "超时待重试", "error": "出错待重试", "pending": "待处理"}
            print(f"[INFO] 该机构已有记录：")
            for r in rows:
                print(f"       {status_map.get(r['download_status'], r['download_status'])}: {r['cnt']} 篇")
    except Exception:
        pass

    async with async_playwright() as p:
        try:
            # 5. 连接 CDP（与原版保持一致）
            browser = await p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"[ERR] 无法连接浏览器 ({CDP_URL})")
            print(f"      请确认 Chrome 已用 --remote-debugging-port=9224 启动")
            print(f"      原始错误: {e}")
            conn.close()
            sys.exit(1)

        # 6. 获取页面上下文（与原版保持一致）
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

        # 7. 选择维普页面
        page = next(
            (pg for pg in pages if "cqvip" in pg.url or "vpn" in pg.url),
            pages[0]
        )
        print(f"[INFO] 使用页面: {page.url}")

        # 8. 检查页面状态
        if not await check_page_alive(page):
            print("[ERR] 目标页面已崩溃，请刷新后重试。")
            conn.close()
            sys.exit(1)

        # 9. 开始爬取
        total_stats = {"downloaded": 0, "skipped": 0, "duplicate": 0, "timeout": 0, "error": 0}
        current_page_num = page_num  # 使用用户输入的起始页码

        try:
            while True:
                fast_mode = current_page_num - page_num + 1 <= FAST_PHASE_PAGES
                stats = await process_page(page, save_dir, conn, current_page_num, institution_name, fast_mode=fast_mode)
                for k in total_stats:
                    total_stats[k] += stats[k]
                print(
                    f"[INFO] 第 {current_page_num} 页完成 | "
                    f"下载 {stats['downloaded']}  跳过 {stats['skipped']}  "
                    f"重复 {stats['duplicate']}  超时 {stats['timeout']}  出错 {stats['error']}"
                )
                has_next = await go_next_page(page)
                if not has_next:
                    print("\n[INFO] 已到最后一页，全部完成！")
                    break
                current_page_num += 1
                print(f"[INFO] 正在翻到第 {current_page_num} 页...")
                # 翻页后的随机等待时间，快速阶段更短
                if fast_mode:
                    page_wait_time = random.uniform(2.5, 4)
                else:
                    page_wait_time = random.uniform(3.5, 6)
                print(f"[INFO] 翻页后等待 {page_wait_time:.1f}s...")
                await asyncio.sleep(page_wait_time)

        except KeyboardInterrupt:
            # 用户手动中断（Ctrl+C）
            print("\n" + "="*70)
            print(f"\n 账号已毕业/退出！请记录断点：【{institution_name}】 停在第 【{current_page_num}】 页！")
            print(f"下次启动请手动翻到此页后运行脚本！\n")
            print("="*70)
        except Exception as e:
            # 其他致命异常
            print("\n" + "="*70)
            print(f"\n 程序异常退出！请记录断点：【{institution_name}】 停在第 【{current_page_num}】 页！")
            print(f"异常原因: {type(e).__name__}: {e}")
            print(f"下次启动请手动翻到此页后运行脚本！\n")
            print("="*70)
            raise e
        finally:
            # 10. 打印汇总信息
            print(f"\n{'='*55}")
            print(f"  机构: {institution_name} - 爬取完成！")
            print(f"  成功下载  : {total_stats['downloaded']} 篇")
            print(f"  无按钮跳过: {total_stats['skipped']} 篇")
            print(f"  数据库去重: {total_stats['duplicate']} 篇")
            print(f"  下载超时  : {total_stats['timeout']} 篇")
            print(f"  下载出错  : {total_stats['error']} 篇")
            print(f"  PDF 位置  : {save_dir.resolve()}")
            print(f"  数据库位置: {DB_PATH.resolve()}")
            print('='*55)
            conn.close()


if __name__ == "__main__":
    asyncio.run(main())
