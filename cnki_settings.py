from pathlib import Path

# ── 连接与路径 ───────────────────────────────────────────
CDP_ENDPOINT = "http://localhost:9222"
DB_PATH = Path("data/membrane_papers.db")
PDF_DIR = Path("data/papers_pdf")

# ── 延迟区间（秒）────────────────────────────────────────
SLEEP_HOVER = (0.6, 2.8)
SLEEP_READ_PAGE = (3, 7)
SLEEP_BEFORE_DL = (2.0, 5.5)
SLEEP_AFTER_DL = (2.0, 4.0)
SLEEP_PAPER_BASE = (4, 8)
SLEEP_BEFORE_FLIP = (3, 7)
SLEEP_AFTER_FLIP = (5, 12)
SLEEP_ON_ERROR = (6, 9)
SLEEP_COOLDOWN = (10, 16)
SLEEP_BLOCKED = (16, 26)
SLEEP_FAKE_READ = (4, 12)
SLEEP_SCROLL_PAUSE = (0.2, 1.5)

# ── 阈值 ─────────────────────────────────────────────────
CONSEC_FAIL_LIMIT = 3
FAKE_READ_CHANCE = 0.25
REFRESH_PAGES_RANGE = (6, 15)

# ── 超时（毫秒）──────────────────────────────────────────
PAGE_TIMEOUT = 40_000
DOWNLOAD_TIMEOUT = 120_000
NEXTPAGE_TIMEOUT = 20_000

# ── 选择器 ───────────────────────────────────────────────
TITLE_SELECTORS = [
    ".result-table-list .name a",
    ".result-table .name a",
    "table.result-table-list td.name a",
    ".GridTableContent td.name a",
    ".result-table-list .title a",
    ".fz14 a",
]

NEXT_PAGE_SELECTORS = [
    "a.next",
    ".pagebar a:has-text('下一页')",
    ".TurnPageToolBar a:has-text('下一页')",
    "a:has-text('下一页')",
]

PDF_SELECTORS = [
    "a:has-text('PDF下载')",
    "a:has-text('PDF')",
    ".btn-pdf",
    ".icon-pdf",
]
