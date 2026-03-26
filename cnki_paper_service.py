import asyncio
import random
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from cnki_browser_actions import (
    check_blocked,
    human_click_with_jitter,
    human_mouse_wander,
    human_scroll,
    inject_random_fingerprint,
    rand_sleep,
)
from cnki_database import save_paper
from cnki_helpers import build_detail_length_signature, is_invalid_detail_title, log, safe_filename
from cnki_settings import (
    DOWNLOAD_TIMEOUT,
    PAGE_TIMEOUT,
    PDF_DIR,
    PDF_SELECTORS,
    SLEEP_AFTER_DL,
    SLEEP_BEFORE_DL,
    SLEEP_FAKE_READ,
    SLEEP_HOVER,
    SLEEP_READ_PAGE,
)


async def extract_paper_info(page) -> dict:
    result = {"title": "未知标题", "abstract": "", "authors": "", "url": page.url}

    title_candidates = []
    for title_sel in [
        ".wx-tit h1", ".brief h1", ".title h1", "h1",
        ".title-en h1", ".doc-top h1", "#article-title", ".article-title",
    ]:
        try:
            locator = page.locator(title_sel)
            count = await locator.count()
            for i in range(min(count, 3)):
                t = (await locator.nth(i).inner_text(timeout=3_000)).strip()
                if t:
                    title_candidates.append(t)
        except Exception:
            continue

    valid_titles = [
        t for t in title_candidates
        if not is_invalid_detail_title(t) and 6 <= len(t) <= 200
    ]
    if valid_titles:
        result["title"] = sorted(valid_titles, key=len, reverse=True)[0]

    for abs_sel in [
        "#ChDivSummary", "#EnDivSummary",
        ".abstract-text", ".abstract",
        "[class*='abstract']", ".doc-summary",
    ]:
        try:
            text = (await page.locator(abs_sel).first.inner_text(timeout=8_000)).strip()
            if text:
                result["abstract"] = text
                break
        except Exception:
            continue

    for auth_sel in [
        "h3.author a", ".author a", "h3 a",
        ".author-name", ".authors a", "[class*='author'] a",
        ".doc-author a",
    ]:
        try:
            els = await page.locator(auth_sel).all()
            names = []
            for el in els:
                t = (await el.inner_text()).strip()
                if t and t not in names and len(t) < 30:
                    names.append(t)
            if names:
                result["authors"] = ", ".join(names)
                break
        except Exception:
            continue

    return result


async def download_pdf(page, pdf_save_path: Path) -> bool:
    if pdf_save_path.exists() and pdf_save_path.stat().st_size > 10_240:
        log(f"    [PDF] 文件已存在，跳过: {pdf_save_path.name}")
        return True

    btn = None
    matched_sel = None
    for sel in PDF_SELECTORS:
        try:
            locator = page.locator(sel).first
            if await locator.is_visible(timeout=5_000):
                btn = locator
                matched_sel = sel
                break
        except Exception:
            continue

    if btn is None:
        log("    [INFO] 未找到 PDF 下载按钮，跳过本篇")
        return True  # 找不到按钮视为正常，跳过而不计失败

    log(f"    [PDF] 找到按钮 ({matched_sel})")
    await btn.scroll_into_view_if_needed()
    await rand_sleep(SLEEP_BEFORE_DL, "点击下载前停留")
    await human_mouse_wander(page, steps=3)

    # 第一层：极速 JS 穿透（5s）
    try:
        async with page.expect_download(timeout=5_000) as dl_info:
            await btn.evaluate("node => node.click()")
        download = await dl_info.value
    except Exception:
        # 第二层：形势判断 + 传统强点重试（15s）
        log("    [WARN] JS点击无响应，进行形势判断与传统点击重试...")
        await asyncio.sleep(random.uniform(2.0, 3.0))
        try:
            await btn.scroll_into_view_if_needed()
            async with page.expect_download(timeout=15_000) as dl_info:
                await btn.click(force=True)
            download = await dl_info.value
        except Exception:
            log("    [SKIP] 重试彻底失败，放弃当前操作")
            return False

    try:
        pdf_save_path.parent.mkdir(parents=True, exist_ok=True)
        await download.save_as(str(pdf_save_path))
        size_kb = pdf_save_path.stat().st_size / 1024
        log(f"    [PDF] ✔ 已保存 ({size_kb:.1f} KB): {pdf_save_path.name}")
        await rand_sleep(SLEEP_AFTER_DL, "下载后继续浏览", page=page)
        await human_scroll(page, min_rounds=1, max_rounds=3)
        return True
    except PlaywrightTimeoutError:
        log("    [WARN] 下载超时（可能需登录或无 PDF 资源）")
        return False
    except Exception as exc:
        log(f"    [WARN] 下载异常: {exc}")
        return False


async def fake_read_paper(context, link_el) -> None:
    fake_page = None
    try:
        # 1) 严格捕获新页面
        async with context.expect_page(timeout=10_000) as fp_info:
            await link_el.click()
        fake_page = await fp_info.value

        # 2) 锚点元素校验：必须先等详情页标题出现
        try:
            await fake_page.wait_for_selector("h1", timeout=15_000)
        except Exception:
            # 3) 卡死时人工介入
            log("  [WARN] 假阅读详情页锚点未加载，等待人工介入...")
            await asyncio.to_thread(
                input,
                "\n⚠️ [人工介入] 详情页未成功加载或被卡住！请在浏览器中手动点开或处理验证码，准备好后请按【回车键】继续...",
            )
            # 4) 二次校验
            try:
                await fake_page.wait_for_selector("h1", timeout=8_000)
            except Exception:
                log("  [WARN] 人工介入后仍未加载详情页，结束本次假阅读")
                return

        log("  [FAKE] 假阅读中...")
        await human_scroll(fake_page, min_rounds=2, max_rounds=5)
        await rand_sleep(SLEEP_FAKE_READ, "假阅读停留")
    except Exception as exc:
        log(f"  [WARN] 假阅读异常（已忽略）: {exc}")
    finally:
        # 5) 安全清理
        if fake_page is not None:
            try:
                await fake_page.close()
            except Exception:
                pass


async def process_one_paper(context, conn, link_el, seen_signatures: set) -> bool:
    new_page = None

    # 第一层：极速 JS 穿透（10s）
    try:
        await link_el.scroll_into_view_if_needed()
        await link_el.hover()
        await rand_sleep(SLEEP_HOVER)

        async with context.expect_page(timeout=10_000) as new_page_info:
            await link_el.evaluate("node => node.click()")
        new_page = await new_page_info.value
    except Exception:
        # 第二层：形势判断 + 传统强点重试（15s）
        log("  [WARN] JS点击无响应，进行形势判断与传统点击重试...")
        await asyncio.sleep(random.uniform(2.0, 3.0))
        try:
            await link_el.scroll_into_view_if_needed()
            async with context.expect_page(timeout=15_000) as new_page_info:
                await link_el.click(force=True)
            new_page = await new_page_info.value
        except Exception:
            log("  [SKIP] 重试彻底失败，放弃当前操作")
            return False

    # 第三层：铁壁防御，new_page 生命周期全托管
    try:
        # 3) 极速探活：5s 内必须出现详情页锚点
        await new_page.wait_for_selector("h1", timeout=5_000)

        await inject_random_fingerprint(new_page)
        log(f"  [NAV] 详情页: {new_page.url[:90]}")

        if await check_blocked(new_page):
            log("  [BLOCK] 检测到封锁！进入超长冷却...")
            return False

        await human_scroll(new_page)
        await rand_sleep(SLEEP_READ_PAGE, "模拟阅读", page=new_page)
        await human_mouse_wander(new_page)

        info = await extract_paper_info(new_page)
        title = info["title"]
        abstract = info["abstract"]
        authors = info["authors"]
        source_url = info["url"]

        log(f"  [INFO] 标题: {title[:70]}")
        log(f"  [INFO] 作者: {authors[:60] or '(无)'}")

        if is_invalid_detail_title(title):
            log(f"  [RETRY] 详情标题无效: {title!r}，尝试刷新后重提取")
            try:
                await new_page.reload(timeout=PAGE_TIMEOUT)
                await new_page.wait_for_load_state("domcontentloaded", timeout=PAGE_TIMEOUT)
                await new_page.wait_for_selector("h1", timeout=5_000)
                info = await extract_paper_info(new_page)
                title = info["title"]
                abstract = info["abstract"]
                authors = info["authors"]
                source_url = info["url"]
                log(f"  [INFO] 重提取标题: {title[:70]}")
            except Exception:
                pass
            if is_invalid_detail_title(title):
                log("  [WARN] 详情页仍无效，跳过本篇")
                return False

        signature = build_detail_length_signature(
            {"title": title, "abstract": abstract, "authors": authors, "url": source_url}
        )
        if signature in seen_signatures:
            log(f"  [SKIP] 详情页签名重复，疑似重复跳转: {signature}")
            return True
        seen_signatures.add(signature)

        safe_title = safe_filename(title)
        pdf_path = PDF_DIR / f"{safe_title}.pdf"
        downloaded = await download_pdf(new_page, pdf_path)
        pdf_path_str = str(pdf_path) if downloaded else ""

        inserted = save_paper(conn, title, authors, abstract, pdf_path_str, source_url, log)
        if inserted:
            log(f"  [DB] ✔ 已写入: {title[:60]}")
        else:
            log("  [DB] ↩ 重复，跳过")

        return True
    except PlaywrightTimeoutError as exc:
        log(f"  [ERROR] 超时: {exc}")
        return False
    except Exception as exc:
        log(f"  [ERROR] 异常: {exc}")
        return False
    finally:
        if new_page is not None:
            try:
                await new_page.close()
            except Exception:
                pass
