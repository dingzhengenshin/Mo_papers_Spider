# -*- coding: utf-8 -*-
"""
AI 自主浏览器代理 — DOM优先 + 截图兜底

核心循环（混合模式）：
  1. page.content() 获取 HTML → QWEN3-plus 文本分析 → 返回动作（便宜，覆盖80%场景）
  2. 文本分析失败时 → 截图 → QWEN3-plus 视觉分析 → 返回动作（贵，但兜底）

用法：
  from ai_browser_agent import AutonomousBrowserAgent

  agent = AutonomousBrowserAgent()
  result = await agent.run(page, goal="打开 xxx 网站，用卡号 xxx 密码 xxx 登录，然后点击知网入口")
"""

import asyncio
import base64
import json
import re
from typing import Optional

from openai import OpenAI
from playwright.async_api import Page

# ════════════════════════════════════════════
# 配置
# ════════════════════════════════════════════

# --- DeepSeek（主用，便宜，DOM文本分析） ---
DS_API_KEY    = "sk-fe9e74ce98ad4829a9c365ea06352ae3"  # 替换为你的 DeepSeek API Key
DS_BASE_URL   = "https://api.deepseek.com"
DS_MODEL      = "deepseek-v4-flash"
# --- QWEN（仅截图视觉兜底用） ---
QWEN_API_KEY  = "sk-b98a5714067e4dff94f2e9bf95f8784a"  # 替换为你的阿里云 API Key
QWEN_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
QWEN_MODEL    = "qwen-plus"             # 支持视觉，截图兜底专用

MAX_STEPS       = 30       # 每个目标最大步数
HTML_LIMIT      = 30000    # HTML 截断长度（给AI分析的）
STEP_DELAY      = 1.0      # 步间最小间隔（秒）
VISION_FALLBACK = True     # DOM分析失败时是否自动切换截图


# ════════════════════════════════════════════
# 提示词
# ════════════════════════════════════════════

DOM_SYSTEM_PROMPT = """你是一个自主浏览器操作代理。你会收到网页的 HTML 片段和当前目标。
分析 HTML 结构，找出需要交互的元素，返回下一步操作。

直接返回 JSON（不要 markdown 代码块）：
{
  "thought": "简要分析页面状态",
  "action": "动作类型",
  "params": { ... },
  "done": false
}

支持的动作：
1. click_selector — 用 CSS 选择器点击元素
   {"action": "click_selector", "params": {"selector": "#login-btn"}}

2. fill — 往输入框填写文字（会先清空）
   {"action": "fill", "params": {"selector": "#username", "value": "xxx"}}

3. press — 按键
   {"action": "press", "params": {"key": "Enter"}}

4. scroll — 滚动
   {"action": "scroll", "params": {"direction": "down", "amount": 500}}

5. wait — 等待
   {"action": "wait", "params": {"seconds": 3}}

6. goto — 导航
   {"action": "goto", "params": {"url": "https://example.com"}}

7. click_text — 点击包含指定文字的元素
   {"action": "click_text", "params": {"text": "知网"}}

8. done — 目标达成
   {"action": "done", "params": {"result": "成功描述"}, "done": true}

9. fail — 无法继续
   {"action": "fail", "params": {"reason": "失败原因"}, "done": true}

10. need_vision — HTML分析不够，需要截图
    {"action": "need_vision", "params": {"reason": "为什么需要截图"}}

规则：
- 优先用 click_selector（最精确）
- 找不到合适选择器时用 click_text
- 如果 HTML 太乱、看不清布局、或元素不可识别，用 need_vision 切换截图模式
- 每次只返回一个动作
"""

VISION_SYSTEM_PROMPT = """你是一个自主浏览器操作代理。你会收到浏览器截图和当前目标。
这是截图模式（DOM分析失败后的兜底方案）。

直接返回 JSON（不要 markdown 代码块）：
{
  "thought": "简要分析截图内容",
  "action": "动作类型",
  "params": { ... },
  "done": false
}

支持的动作：
1. click — 点击坐标位置
   {"action": "click", "params": {"x": 100, "y": 200}}

2. type — 在坐标位置输入文字
   {"action": "type", "params": {"x": 100, "y": 200, "text": "xxx"}}

3. press — 按键
   {"action": "press", "params": {"key": "Enter"}}

4. scroll — 滚动
   {"action": "scroll", "params": {"direction": "down", "amount": 500}}

5. wait — 等待
   {"action": "wait", "params": {"seconds": 3}}

6. goto — 导航
   {"action": "goto", "params": {"url": "https://example.com"}}

7. done — 目标达成
   {"action": "done", "params": {"result": "成功描述"}, "done": true}

8. fail — 无法继续
   {"action": "fail", "params": {"reason": "失败原因"}, "done": true}

规则：
- 坐标是相对于截图图片左上角的像素位置
- 仔细观察截图中的按钮、链接、输入框位置
- 每次只返回一个动作
"""


# ════════════════════════════════════════════
# AutonomousBrowserAgent 类
# ════════════════════════════════════════════

class AutonomousBrowserAgent:
    """自主浏览器操作代理 — DOM优先，截图兜底"""

    def __init__(self, ds_api_key: str = None, qwen_api_key: str = None,
                 max_steps: int = MAX_STEPS, verbose: bool = True):
        # DeepSeek 客户端（DOM文本分析，主力）
        self._ds_client = OpenAI(
            api_key=ds_api_key or DS_API_KEY,
            base_url=DS_BASE_URL,
        )
        # QWEN 客户端（截图视觉分析，兜底）
        self._qwen_client = OpenAI(
            api_key=qwen_api_key or QWEN_API_KEY,
            base_url=QWEN_BASE_URL,
        )
        self._max_steps = max_steps
        self._verbose = verbose
        self._history = []

        # 统计
        self._dom_calls = 0
        self._vision_calls = 0

    def _log(self, msg: str):
        if self._verbose:
            print(f"  [AGENT] {msg}")

    # ── 获取页面内容 ──────────────────────────

    async def _get_html(self, page: Page) -> str:
        """获取页面 HTML 并截断"""
        try:
            html = await page.content()
            return html[:HTML_LIMIT]
        except Exception as e:
            return f"<error: {e}>"

    async def _take_screenshot(self, page: Page) -> str:
        """截图返回 base64"""
        buf = await page.screenshot(full_page=False)
        return base64.b64encode(buf).decode("utf-8")

    # ── 构建上下文 ────────────────────────────

    def _build_context(self, goal: str, page_url: str, page_title: str) -> str:
        parts = [f"当前目标：{goal}"]
        if page_url:
            parts.append(f"当前URL：{page_url}")
        if page_title:
            parts.append(f"页面标题：{page_title}")
        if self._history:
            recent = self._history[-5:]
            history_str = "\n".join(f"  {h}" for h in recent)
            parts.append(f"最近操作：\n{history_str}")
        return "\n".join(parts)

    # ── DOM 模式分析（便宜）──────────────────

    def _ask_dom(self, html: str, context: str) -> dict:
        """用 HTML 文本分析，不消耗图片 token"""
        self._dom_calls += 1
        user_msg = context + f"\n\nHTML片段：\n{html}\n\n请分析HTML，返回下一步动作。"

        try:
            resp = self._ds_client.chat.completions.create(
                model=DS_MODEL,
                messages=[
                    {"role": "system", "content": DOM_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=512,
                temperature=0.1,
            )
            text = resp.choices[0].message.content.strip()
            self._log(f"[DOM] AI回复: {text[:150]}")

            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                return json.loads(json_match.group())
            return {"action": "need_vision", "params": {"reason": f"无法解析: {text[:80]}"}}

        except Exception as e:
            self._log(f"[DOM] API失败: {e}")
            return {"action": "need_vision", "params": {"reason": f"API错误: {e}"}}

    # ── 截图模式分析（贵但兜底）───────────────

    def _ask_vision(self, screenshot_b64: str, context: str) -> dict:
        """用截图视觉分析，消耗图片 token"""
        self._vision_calls += 1
        content = [
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{screenshot_b64}"},
            },
            {
                "type": "text",
                "text": context + "\n\n请分析截图，返回下一步动作。",
            },
        ]

        try:
            resp = self._qwen_client.chat.completions.create(
                model=QWEN_MODEL,
                messages=[
                    {"role": "system", "content": VISION_SYSTEM_PROMPT},
                    {"role": "user", "content": content},
                ],
                max_tokens=512,
                temperature=0.1,
            )
            text = resp.choices[0].message.content.strip()
            self._log(f"[视觉] AI回复: {text[:150]}")

            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                return json.loads(json_match.group())
            return {"action": "wait", "params": {"seconds": 2},
                    "thought": f"无法解析视觉回复: {text[:80]}"}

        except Exception as e:
            self._log(f"[视觉] API失败: {e}")
            return {"action": "wait", "params": {"seconds": 3},
                    "thought": f"视觉API错误: {e}"}

    # ── 执行动作 ──────────────────────────────

    async def _execute_action(self, page: Page, action: dict) -> bool:
        """执行动作，返回 True=继续, False=停止"""
        act = action.get("action", "wait")
        params = action.get("params", {})

        try:
            if act == "click_selector":
                sel = params.get("selector", "")
                self._log(f"点击选择器: {sel}")
                await page.click(sel, timeout=5000)
                await asyncio.sleep(0.5)

            elif act == "fill":
                sel = params.get("selector", "")
                val = str(params.get("value", ""))
                self._log(f"填写 {sel} ← {val[:30]}")
                await page.fill(sel, val, timeout=5000)
                await asyncio.sleep(0.3)

            elif act == "click_text":
                text = params.get("text", "")
                self._log(f"点击文字: {text}")
                await page.click(f"text={text}", timeout=5000)
                await asyncio.sleep(0.5)

            elif act == "click":
                x, y = params.get("x", 0), params.get("y", 0)
                self._log(f"点击坐标 ({x}, {y})")
                await page.mouse.click(x, y)
                await asyncio.sleep(0.5)

            elif act == "type":
                text = str(params.get("text", ""))
                x, y = params.get("x"), params.get("y")
                if x is not None and y is not None:
                    await page.mouse.click(x, y)
                    await asyncio.sleep(0.3)
                await page.keyboard.press("Control+a")
                await asyncio.sleep(0.1)
                await page.keyboard.type(text, delay=50)
                self._log(f"输入文字: {text[:50]}")

            elif act == "press":
                key = params.get("key", "Enter")
                await page.keyboard.press(key)
                self._log(f"按键: {key}")

            elif act == "scroll":
                direction = params.get("direction", "down")
                amount = params.get("amount", 500)
                delta_y = amount if direction == "down" else -amount
                await page.mouse.wheel(0, delta_y)
                self._log(f"滚动 {direction} {amount}px")

            elif act == "wait":
                seconds = params.get("seconds", 2)
                self._log(f"等待 {seconds}s")
                await asyncio.sleep(seconds)

            elif act == "goto":
                url = params.get("url", "")
                if url:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    self._log(f"导航到: {url}")

            elif act == "done":
                self._log(f"完成: {params.get('result', '')}")
                return True

            elif act == "fail":
                self._log(f"失败: {params.get('reason', '')}")
                return False

            elif act == "need_vision":
                # 不是真正的动作，只是标记需要切换截图模式
                self._log(f"需要截图: {params.get('reason', '')}")
                return True

            else:
                self._log(f"未知动作: {act}")

        except Exception as e:
            self._log(f"动作执行失败 ({act}): {e}")

        return True

    # ── 主循环 ─────────────────────────────────

    async def run(self, page: Page, goal: str, on_step=None) -> dict:
        """
        自主执行目标（DOM优先，截图兜底）。

        Args:
            page: Playwright Page
            goal: 目标描述
            on_step: 回调 (step_num, action, page)

        Returns:
            {"success", "steps", "result", "history", "dom_calls", "vision_calls"}
        """
        self._history = []
        self._dom_calls = 0
        self._vision_calls = 0
        self._log(f"目标: {goal}")

        for step in range(self._max_steps):
            step_num = step + 1

            # 页面信息
            try:
                page_url = page.url
                page_title = await page.title()
            except Exception:
                page_url, page_title = "", ""

            context = self._build_context(goal, page_url, page_title)

            # ── DOM 模式（便宜） ──
            html = await self._get_html(page)
            action = self._ask_dom(html, context)
            thought = action.get("thought", "")
            self._log(f"步骤 {step_num}/{self._max_steps} [DOM]: {thought}")

            # 如果 AI 请求截图模式
            if action.get("action") == "need_vision" and VISION_FALLBACK:
                self._log("切换到截图模式...")
                try:
                    screenshot_b64 = await self._take_screenshot(page)
                    action = self._ask_vision(screenshot_b64, context)
                    thought = action.get("thought", "")
                    self._log(f"步骤 {step_num}/{self._max_steps} [视觉]: {thought}")
                except Exception as e:
                    self._log(f"截图失败: {e}")
                    action = {"action": "wait", "params": {"seconds": 2},
                              "thought": "截图失败，等待重试"}

            # 记录历史
            act = action.get("action", "?")
            params = action.get("params", {})
            mode = "视觉" if "x" in params or act in ("click", "type") else "DOM"
            self._history.append(f"[{mode}] {act}({json.dumps(params, ensure_ascii=False)[:60]})")

            # 回调
            if on_step:
                try:
                    await on_step(step_num, action, page)
                except Exception:
                    pass

            # 执行
            continue_loop = await self._execute_action(page, action)

            if action.get("done"):
                success = action.get("action") == "done"
                self._print_stats()
                return {
                    "success": success,
                    "steps": step_num,
                    "result": params.get("result" if success else "reason", ""),
                    "history": self._history,
                    "dom_calls": self._dom_calls,
                    "vision_calls": self._vision_calls,
                }

            if not continue_loop:
                self._print_stats()
                return {
                    "success": False,
                    "steps": step_num,
                    "result": params.get("reason", "执行失败"),
                    "history": self._history,
                    "dom_calls": self._dom_calls,
                    "vision_calls": self._vision_calls,
                }

            await asyncio.sleep(STEP_DELAY)

        self._print_stats()
        return {
            "success": False,
            "steps": self._max_steps,
            "result": f"超过最大步数 ({self._max_steps})",
            "history": self._history,
            "dom_calls": self._dom_calls,
            "vision_calls": self._vision_calls,
        }

    def _print_stats(self):
        self._log(f"统计 — DOM调用: {self._dom_calls}, 截图调用: {self._vision_calls}")

    # ── 便捷方法 ──────────────────────────────

    async def login_and_navigate(self, page: Page, portal_url: str,
                                  card_number: str, password: str,
                                  target_keyword: str = "知网") -> dict:
        """AI 自主登录代理站并导航到目标入口"""
        goal = (
            f"1. 在浏览器中打开 {portal_url}\n"
            f"2. 使用卡号 {card_number} 和密码 {password} 登录\n"
            f"3. 登录成功后，找到并点击包含「{target_keyword}」的入口链接\n"
            f"4. 确认已进入 {target_keyword} 搜索页面"
        )
        return await self.run(page, goal)

    async def handle_anomaly(self, page: Page, context: str = "") -> dict:
        """AI 检测并处理页面异常"""
        goal = "检测当前页面是否有异常（弹窗、验证码、错误提示、IP封锁等），如果有则关闭弹窗或处理异常。如果页面正常则直接标记完成。"
        if context:
            goal += f"\n背景：{context}"
        return await self.run(page, goal, max_steps=5)
