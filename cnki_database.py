import sqlite3

from cnki_settings import DB_PATH


def init_db(log) -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            title                TEXT    UNIQUE,
            authors              TEXT,
            abstract_text        TEXT,
            keywords             TEXT,
            source_db            TEXT,
            publish_year         TEXT,
            download_link        TEXT,
            pdf_local_path       TEXT,
            ai_industry_category TEXT,
            ai_product_category  TEXT,
            ai_quality_issue     TEXT,
            scrape_time          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    log(f"[DB] 已连接: {DB_PATH.resolve()}")
    return conn


def save_paper(
    conn: sqlite3.Connection,
    title: str,
    authors: str,
    abstract: str,
    pdf_path: str,
    source_url: str,
    log,
) -> bool:
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO papers
                (title, authors, abstract_text, pdf_local_path, source_db, download_link)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (title, authors, abstract, pdf_path, "知网", source_url),
        )
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.Error as exc:
        log(f"  [DB] ✘ 写入失败: {exc}")
        return False
