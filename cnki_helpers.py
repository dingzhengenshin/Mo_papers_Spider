import re
from datetime import datetime


def safe_filename(name: str, max_len: int = 100) -> str:
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', "_", name).strip()
    return name[:max_len]


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def is_invalid_detail_title(title: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    bad_exact = {
        "自动登录", "登录", "login", "sso login", "cas login",
        "统一身份认证", "身份认证", "passport"
    }
    return t in bad_exact or len(t) < 3


def build_detail_length_signature(info: dict) -> str:
    title_len = len((info.get("title") or "").strip())
    abstract_len = len((info.get("abstract") or "").strip())
    authors_len = len((info.get("authors") or "").strip())
    url_tail = (info.get("url") or "")[-24:]
    return f"T{title_len}-A{abstract_len}-U{authors_len}-URL{url_tail}"
