import asyncio
import random

from cnki_helpers import log
from cnki_settings import (
    NEXT_PAGE_SELECTORS,
    NEXTPAGE_TIMEOUT,
    SLEEP_AFTER_FLIP,
    SLEEP_HOVER,
    SLEEP_SCROLL_PAUSE,
    TITLE_SELECTORS,
)


async def human_scroll(page, min_rounds: int = 2, max_rounds: int = 6) -> None:
    rounds = random.randint(min_rounds, max_rounds)
    for _ in range(rounds):
        direction = 1 if random.random() < 0.70 else -1
        total = random.choice([
            random.randint(120, 280),
            random.randint(280, 520),
            random.randint(520, 900),
        ])
        steps = random.randint(6, 14)

        import math

        step_deltas = []
        for i in range(steps):
            weight = math.sin(math.pi * i / (steps - 1)) if steps > 1 else 1
            step_deltas.append(weight)
        total_weight = sum(step_deltas)

        for w in step_deltas:
            sub_delta = int(total * w / total_weight) + random.randint(-8, 8)
            if sub_delta == 0:
                sub_delta = 1
            await page.mouse.wheel(0, sub_delta * direction)
            await asyncio.sleep(random.uniform(0.02, 0.12))

        await asyncio.sleep(random.uniform(*SLEEP_SCROLL_PAUSE))

    if random.random() < 0.30:
        back = random.randint(60, 220)
        back_steps = random.randint(3, 7)
        for _ in range(back_steps):
            await page.mouse.wheel(0, -(back // back_steps + random.randint(-5, 5)))
            await asyncio.sleep(random.uniform(0.03, 0.10))
        await asyncio.sleep(random.uniform(0.2, 0.6))


async def rand_sleep(rng: tuple, label: str = "", page=None) -> None:
    t = random.uniform(*rng)
    if label:
        log(f"  [~] {label} {t:.1f}s")
    if page is not None and t > 3.0:
        segments = random.randint(2, max(2, int(t // 4)))
        elapsed = 0.0
        for _ in range(segments):
            seg = t / segments + random.uniform(-0.5, 0.5)
            seg = max(0.5, seg)
            await asyncio.sleep(seg)
            elapsed += seg
            if elapsed < t and random.random() < 0.75:
                await human_scroll(page, min_rounds=1, max_rounds=2)
        remaining = t - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)
    else:
        await asyncio.sleep(t)


async def human_mouse_wander(page, steps: int = None) -> None:
    if steps is None:
        steps = random.randint(3, 8)
    vp = page.viewport_size or {"width": 1280, "height": 800}
    for _ in range(steps):
        x = random.randint(80, vp["width"] - 80)
        y = random.randint(80, vp["height"] - 150)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.05, 0.25))


async def human_click_with_jitter(locator, page=None) -> None:
    box = await locator.bounding_box()
    if box and page is not None and box["width"] > 4 and box["height"] > 4:
        offset_x = box["width"] / 2 + random.uniform(-min(4, box["width"] * 0.25), min(4, box["width"] * 0.25))
        offset_y = box["height"] / 2 + random.uniform(-min(2, box["height"] * 0.25), min(2, box["height"] * 0.25))
        cx = box["x"] + offset_x
        cy = box["y"] + offset_y
        await page.mouse.move(cx, cy)
        await asyncio.sleep(random.uniform(0.1, 0.4))
        await page.mouse.click(cx, cy)
    else:
        await locator.click()


async def check_blocked(page) -> bool:
    block_titles = ["访问受限", "异常访问", "安全验证", "blocked", "captcha", "robot check"]
    try:
        title = (await page.title()).lower()
        for kw in block_titles:
            if kw.lower() in title:
                log(f"  [BLOCK] 页面标题含封锁标志: '{kw}'")
                return True
    except Exception:
        pass
    return False


async def inject_random_fingerprint(page) -> None:
    try:
        w_jitter = random.randint(-30, 30)
        h_jitter = random.randint(-20, 20)
        vp = page.viewport_size or {"width": 1280, "height": 800}
        await page.set_viewport_size(
            {
                "width": max(1024, vp["width"] + w_jitter),
                "height": max(600, vp["height"] + h_jitter),
            }
        )
    except Exception:
        pass


async def find_cnki_page(context):
    pages = context.pages
    log(f"[CDP] 当前共 {len(pages)} 个标签页")
    for page in pages:
        try:
            url = page.url
            title = await page.title()
        except Exception:
            continue
        log(f"  · [{title[:45]}]  {url[:80]}")
        if "cnki" in url.lower() or "检索" in title:
            log(f"[CDP] ✔ 找到知网页面: {title!r}")
            return page
    return None


async def get_title_links(search_page):
    for sel in TITLE_SELECTORS:
        try:
            els = await search_page.locator(sel).all()
            if els:
                return sel, els
        except Exception:
            continue
    return None, []


async def click_next_page(search_page) -> bool:
    log("[PAGE] 滚动到底部寻找翻页按钮...")
    for _ in range(4):
        await search_page.mouse.wheel(0, 600)
        await asyncio.sleep(random.uniform(0.2, 0.5))
    await asyncio.sleep(1.0)

    for sel in NEXT_PAGE_SELECTORS:
        try:
            btn = search_page.locator(sel).first
            if await btn.is_visible(timeout=3_000):
                disabled = await btn.get_attribute("disabled")
                cls = await btn.get_attribute("class") or ""
                if disabled is not None or "disabled" in cls or "无效" in cls:
                    log("[PAGE] 下一页按钮已禁用，已到最后一页")
                    return False
                log(f"[PAGE] 找到翻页按钮: '{sel}'")
                await btn.scroll_into_view_if_needed()
                await asyncio.sleep(random.uniform(0.8, 2.0))
                await btn.hover()
                await rand_sleep(SLEEP_HOVER)
                await human_click_with_jitter(btn, search_page)
                await search_page.wait_for_load_state("domcontentloaded", timeout=NEXTPAGE_TIMEOUT)
                await rand_sleep(SLEEP_AFTER_FLIP, "翻页后渲染等待")
                log("[PAGE] ✔ 已翻到下一页")
                return True
        except Exception:
            continue

    log("[PAGE] 未找到下一页按钮，尝试诊断...")
    try:
        all_links = await search_page.locator("a").all()
        page_links = []
        for lnk in all_links:
            try:
                if await lnk.is_visible(timeout=500):
                    txt = (await lnk.inner_text()).strip()
                    if txt and any(k in txt.lower() for k in ["下一", "next", "页", "page"]):
                        page_links.append(txt)
            except Exception:
                continue
        if page_links:
            log(f"[PAGE] 页面上发现分页相关链接: {page_links}")
        else:
            log("[PAGE] 页面上未发现任何分页相关链接，可能已是最后一页")
    except Exception:
        pass

    log("[PAGE] 已到最后一页")
    return False
