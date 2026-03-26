#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import random
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from cnki_browser_actions import (
    check_blocked,
    click_next_page,
    find_cnki_page,
    get_title_links,
    human_mouse_wander,
    human_scroll,
    inject_random_fingerprint,
    rand_sleep,
)
from cnki_database import init_db
from cnki_helpers import log
from cnki_paper_service import fake_read_paper, process_one_paper
from cnki_settings import (
    CDP_ENDPOINT,
    CONSEC_FAIL_LIMIT,
    DB_PATH,
    FAKE_READ_CHANCE,
    NEXTPAGE_TIMEOUT,
    PAGE_TIMEOUT,
    PDF_DIR,
    REFRESH_PAGES_RANGE,
    SLEEP_BEFORE_FLIP,
    SLEEP_COOLDOWN,
    SLEEP_ON_ERROR,
    SLEEP_PAPER_BASE,
)


async def run() -> None:
    import time
    conn = init_db(log)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    RUN_LIMIT_HOURS = 3
    run_start = time.monotonic()
    run_deadline = run_start + RUN_LIMIT_HOURS * 3600
    log(f"[LIMIT] 本次运行上限 {RUN_LIMIT_HOURS} 小时，预计停止时间: "
        f"{__import__('datetime').datetime.now().strftime('%H:%M:%S')} + {RUN_LIMIT_HOURS}h")

    async with async_playwright() as pw:
        log(f"[CDP] 连接 {CDP_ENDPOINT} ...")
        try:
            browser = await pw.chromium.connect_over_cdp(CDP_ENDPOINT)
        except Exception as exc:
            log(f"[ERROR] 无法连接 CDP: {exc}")
            log("请确认 Chrome 已以 --remote-debugging-port=9222 启动。")
            conn.close()
            return

        context = browser.contexts[0]
        log("[CDP] ✔ 已接管浏览器")

        search_page = await find_cnki_page(context)
        if search_page is None:
            log("[ERROR] 未找到知网搜索结果页，请在 Chrome 中停留在检索结果列表后重试。")
            conn.close()
            return

        await inject_random_fingerprint(search_page)

        total_processed = 0
        total_success = 0
        page_num = 1
        consec_fail = 0
        next_refresh_at = random.randint(*REFRESH_PAGES_RANGE)
        seen_signatures = set()

        async def wait_list_ready() -> None:
            try:
                await search_page.wait_for_load_state("domcontentloaded", timeout=NEXTPAGE_TIMEOUT)
            except Exception:
                pass
            try:
                await search_page.wait_for_selector(
                    ".result-table-list, .result-table, table.result-table-list, .GridTableContent",
                    timeout=NEXTPAGE_TIMEOUT,
                )
            except Exception:
                pass

        async def locate_link(matched_sel: str, href: str, title: str):
            if href:
                try:
                    href_sel = f'a[href={json.dumps(href)}]'
                    loc = search_page.locator(href_sel).first
                    if await loc.count() > 0 and await loc.is_visible(timeout=1_500):
                        return loc
                except Exception:
                    pass

            if matched_sel:
                try:
                    cands = search_page.locator(matched_sel)
                    cnt = await cands.count()
                    for i in range(cnt):
                        c = cands.nth(i)
                        try:
                            txt = (await c.inner_text()).strip()
                            if txt == title and await c.is_visible(timeout=800):
                                return c
                        except Exception:
                            continue
                except Exception:
                    pass
            return None

        log("[START] 开始全量下载，昼夜不停直到末页...")
        log(f"{'═' * 60}")

        while True:
            # 运行时长检测：超过上限主动退出
            if time.monotonic() > run_deadline:
                elapsed_h = (time.monotonic() - run_start) / 3600
                log(f"[LIMIT] 已运行 {elapsed_h:.2f} 小时，达到 {RUN_LIMIT_HOURS}h 上限，安全退出。")
                break

            log(f"\n[PAGE {page_num}] 获取论文列表...")

            try:
                if await check_blocked(search_page):
                    log("[BLOCK] 列表页检测到封锁！请手动处理验证码后继续...")
                    await asyncio.to_thread(input, "请手动在 Chrome 中解决滑块验证码，完成后按回车继续...")
                    try:
                        await search_page.reload(timeout=PAGE_TIMEOUT)
                    except Exception:
                        pass
            except Exception as exc:
                log(f"[WARN] 封锁检测异常（继续）: {exc}")

            await wait_list_ready()

            matched_sel, title_links = await get_title_links(search_page)
            if not title_links:
                log("[WARN] 当前页未找到论文链接，等待后重试...")
                await asyncio.sleep(random.uniform(10, 20))
                await wait_list_ready()
                matched_sel, title_links = await get_title_links(search_page)
                if not title_links:
                    log("[STOP] 仍然无法获取论文列表，退出循环。")
                    break

            # 防元素失效：先提取本页标识信息，后续循环只按标识重定位
            page_papers = []
            for idx, link in enumerate(title_links, start=1):
                title_full = ""
                href = ""
                is_foreign = False
                try:
                    title_full = (await link.inner_text()).strip()
                except Exception:
                    title_full = f"第{page_num}页第{idx}篇"

                try:
                    href = (await link.get_attribute("href") or "").strip()
                    if href.startswith("//"):
                        href = "https:" + href
                    elif href.startswith("/"):
                        href = urljoin(search_page.url, href)
                except Exception:
                    href = ""

                try:
                    row_text = ""
                    row = link.locator("xpath=ancestor::tr[1]")
                    if await row.count() > 0:
                        row_text = (await row.inner_text()).lower()
                    is_foreign = (
                        any(k in href.upper() for k in ["SJPD", "GARJ", "WWJD", "CJPD", "IPFD"])
                        or "外文" in row_text
                        or "foreign" in row_text
                    )
                except Exception:
                    is_foreign = False

                page_papers.append(
                    {"idx": idx, "href": href, "title": title_full, "is_foreign": is_foreign}
                )

            log(f"[PAGE {page_num}] 选择器 '{matched_sel}'，本页 {len(page_papers)} 篇")

            await human_scroll(search_page, min_rounds=1, max_rounds=4)
            await asyncio.sleep(random.uniform(1.5, 4.0))

            # 假阅读：按 href/标题重定位元素
            fake_candidates = list(page_papers)
            random.shuffle(fake_candidates)
            fake_count = random.randint(0, 2)
            for i in range(min(fake_count, len(fake_candidates))):
                if random.random() < FAKE_READ_CHANCE:
                    item = fake_candidates[i]
                    try:
                        fake_link = await locate_link(matched_sel, item["href"], item["title"])
                        if fake_link is None:
                            continue
                        log(f"  [FAKE] 本页第 {item['idx']} 篇将做假阅读")
                        await fake_read_paper(context, fake_link)
                        await rand_sleep(SLEEP_PAPER_BASE, "假阅读后休眠", page=search_page)
                    except Exception as exc:
                        log(f"  [WARN] 假阅读异常（继续）: {exc}")

            for item in page_papers:
                total_processed += 1
                title_preview = item["title"][:60]

                if item["is_foreign"]:
                    log(f"  [SKIP] 外文期刊，跳过: {title_preview[:50]}")
                    continue

                log(f"\n  [{total_processed}] P{page_num}-{item['idx']}: {title_preview}")

                link_el = await locate_link(matched_sel, item["href"], item["title"])
                if link_el is None:
                    log(f"  [WARN] 无法重定位论文元素，跳过: {title_preview[:50]}")
                    consec_fail += 1
                    log(f"  [FAIL] 连续失败 {consec_fail}/{CONSEC_FAIL_LIMIT}")
                    await rand_sleep(SLEEP_ON_ERROR, "出错冷却")
                    await rand_sleep(SLEEP_PAPER_BASE, "篇间休眠", page=search_page)
                    continue

                await human_mouse_wander(search_page, steps=random.randint(2, 5))

                before_pages = set(context.pages)
                try:
                    ok = await asyncio.wait_for(
                        process_one_paper(context, conn, link_el, seen_signatures),
                        timeout=120,
                    )
                except asyncio.TimeoutError:
                    log("  [ERROR] 超时: 单篇处理超过 120s，已跳过")
                    ok = False
                except Exception as exc:
                    log(f"  [ERROR] 异常: {exc}")
                    ok = False
                finally:
                    try:
                        current_pages = list(context.pages)
                        leaked_pages = [p for p in current_pages if p is not search_page and p not in before_pages]
                        for p in leaked_pages:
                            try:
                                await p.close()
                                log("  [CLEAN] 已关闭异常遗留标签页")
                            except Exception:
                                pass
                    except Exception as exc:
                        log(f"  [WARN] 页签清理异常（继续）: {exc}")

                if ok:
                    consec_fail = 0
                    total_success += 1
                else:
                    consec_fail += 1
                    log(f"  [FAIL] 连续失败 {consec_fail}/{CONSEC_FAIL_LIMIT}")
                    await rand_sleep(SLEEP_ON_ERROR, "出错冷却")
                    if consec_fail >= CONSEC_FAIL_LIMIT:
                        cool = random.uniform(*SLEEP_COOLDOWN)
                        log(f"[COOLDOWN] 连续失败 {consec_fail} 次，冷却 {cool:.0f}s...")
                        await asyncio.sleep(cool)
                        consec_fail = 0

                await rand_sleep(SLEEP_PAPER_BASE, "篇间休眠", page=search_page)

            if page_num >= next_refresh_at:
                log(f"[REFRESH] 第 {page_num} 页，随机刷新列表页...")
                try:
                    await search_page.reload(timeout=PAGE_TIMEOUT)
                    await search_page.wait_for_load_state("domcontentloaded", timeout=PAGE_TIMEOUT)
                    await search_page.wait_for_selector(
                        ".result-table-list, .result-table, table.result-table-list, .GridTableContent",
                        timeout=NEXTPAGE_TIMEOUT,
                    )
                    await inject_random_fingerprint(search_page)
                    log("[REFRESH] ✔ 完成")
                except Exception as exc:
                    log(f"[REFRESH] 失败（继续）: {exc}")
                next_refresh_at = page_num + random.randint(*REFRESH_PAGES_RANGE)

            await rand_sleep(SLEEP_BEFORE_FLIP, "翻页前停留")
            has_next = await click_next_page(search_page)
            if not has_next:
                log(f"[STOP] 已到最后一页（第 {page_num} 页），全量下载完毕。")
                break

            page_num += 1

        log(f"\n{'═' * 60}")
        log("[DONE] 全部完成！")
        log(f"       累计处理论文: {total_processed} 篇")
        log(f"       成功下载 PDF: {total_success} 篇")
        log(f"       共翻页数:     {page_num} 页")
        log(f"       数据库路径:   {DB_PATH.resolve()}")
        log(f"       PDF 目录:     {PDF_DIR.resolve()}")
        log(f"{'═' * 60}")

    conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[中断] 用户手动停止，已安全退出。")
