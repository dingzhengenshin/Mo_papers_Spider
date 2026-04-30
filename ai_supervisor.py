# -*- coding: utf-8 -*-
"""
AI Supervisor — 爬虫统一监控与断点自动恢复

功能：
  1. 统一监控所有爬虫进程和浏览器 CDP 端口状态
  2. 从数据库分析断点，估算爬取进度
  3. 检测到爬虫死亡时自动重启并从断点恢复
  4. 终端表格输出全局状态报告

用法：
  python ai_supervisor.py                    # 默认每5分钟检查一次
  python ai_supervisor.py --interval 120     # 每2分钟检查一次
  python ai_supervisor.py --no-auto-restart  # 仅监控，不自动重启
"""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# ════════════════════════════════════════════
# 配置区
# ════════════════════════════════════════════
DB_PATH       = Path("./data/membrane_papers.db")
EXCEL_PATH    = Path("./质检省一级单位统计.xlsx")
PID_DIR       = Path("./data")
LOG_DIR       = Path("./data/logs")
PROJECT_ROOT  = Path(__file__).parent.resolve()
PAPERS_PER_PAGE = 20  # 所有平台每页约20篇
CDP_TIMEOUT   = 3.0   # CDP 探测超时(秒)
RESTART_COOLDOWN = 300  # 同一爬虫重启冷却时间(秒)
STALE_THRESHOLD  = 1800 # 最近 scrape_time 超过此秒数视为失联

# ════════════════════════════════════════════
# 爬虫注册表
# ════════════════════════════════════════════
CRAWLERS = {
    "vip_9222": {
        "script": "vip_institution_crawler.py",
        "cdp_port": 9222,
        "source_db": "维普",
        "needs_cdp": True,
    },
    "vip_9223": {
        "script": "vip_institution_crawler_threadtest1.py",
        "cdp_port": 9223,
        "source_db": "维普",
        "needs_cdp": True,
    },
    "vip_9224": {
        "script": "vip_institution_crawler_threadtest2.py",
        "cdp_port": 9224,
        "source_db": "维普",
        "needs_cdp": True,
    },
    "cnki_9223": {
        "script": "cnki_mirror_crawler.py",
        "cdp_port": 9223,
        "source_db": "知网",
        "needs_cdp": True,
    },
    "hnsti_9225": {
        "script": "hnsti_crawler.py",
        "cdp_port": 9225,
        "source_db": "HNSTI",
        "needs_cdp": True,
    },
}


# ════════════════════════════════════════════
# HealthChecker — CDP 探针 + PID 进程检测
# ════════════════════════════════════════════

def check_cdp_alive(port: int) -> dict:
    """通过 CDP /json/version 端点检测 Chrome 是否存活"""
    try:
        url = f"http://127.0.0.1:{port}/json/version"
        resp = urllib.request.urlopen(url, timeout=CDP_TIMEOUT)
        data = json.loads(resp.read())
        return {
            "alive": True,
            "browser": data.get("Browser", "unknown"),
            "websocket": data.get("webSocketDebuggerUrl", ""),
        }
    except Exception:
        return {"alive": False}


def check_cdp_tabs(port: int) -> list:
    """获取 CDP 端口上的所有打开标签页"""
    try:
        url = f"http://127.0.0.1:{port}/json/list"
        resp = urllib.request.urlopen(url, timeout=CDP_TIMEOUT)
        tabs = json.loads(resp.read())
        return [{"title": t.get("title", ""), "url": t.get("url", "")} for t in tabs]
    except Exception:
        return []


def check_pid_file(script_name: str) -> dict:
    """读取 PID 文件并检查进程是否存活"""
    # PID 文件名 = 脚本文件名去掉 .py 后缀
    stem = Path(script_name).stem
    pid_path = PID_DIR / f"{stem}.pid"

    if not pid_path.exists():
        return {"running": False, "pid": None, "pid_file_exists": False}

    try:
        pid = int(pid_path.read_text().strip())
    except ValueError:
        return {"running": False, "pid": None, "pid_file_exists": True, "corrupt": True}

    # Windows: os.kill(pid, 0) 在进程不存在时抛 OSError
    try:
        os.kill(pid, 0)
        return {"running": True, "pid": pid, "pid_file_exists": True}
    except OSError:
        return {"running": False, "pid": pid, "pid_file_exists": True, "dead_pid": pid}


# ════════════════════════════════════════════
# BreakpointAnalyzer — 数据库断点分析
# ════════════════════════════════════════════

def get_db_conn():
    """获取数据库连接"""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def get_overall_stats(conn) -> dict:
    """获取全局统计"""
    if not conn:
        return {}
    try:
        row = conn.execute(
            "SELECT download_status, COUNT(*) as cnt FROM papers GROUP BY download_status"
        ).fetchall()
        return {r["download_status"]: r["cnt"] for r in row}
    except Exception:
        return {}


def get_institution_progress(conn) -> list:
    """获取每个机构+数据源的爬取进度"""
    if not conn:
        return []
    try:
        rows = conn.execute("""
            SELECT
                institution,
                source_db,
                COUNT(*) as total,
                SUM(CASE WHEN download_status = 'ok' THEN 1 ELSE 0 END) as downloaded,
                SUM(CASE WHEN download_status = 'skip' THEN 1 ELSE 0 END) as skipped,
                SUM(CASE WHEN download_status = 'error' THEN 1 ELSE 0 END) as errors,
                SUM(CASE WHEN download_status = 'timeout' THEN 1 ELSE 0 END) as timeouts,
                MAX(scrape_time) as last_scrape
            FROM papers
            WHERE institution IS NOT NULL AND institution != ''
            GROUP BY institution, source_db
            ORDER BY last_scrape DESC
        """).fetchall()

        results = []
        for r in rows:
            total = r["total"]
            est_page = max(1, (total + PAPERS_PER_PAGE - 1) // PAPERS_PER_PAGE)
            last_scrape = r["last_scrape"]

            # 判断状态
            status = "COMPLETED"
            if last_scrape:
                try:
                    last_dt = datetime.fromisoformat(last_scrape)
                    if (datetime.now() - last_dt).total_seconds() > STALE_THRESHOLD:
                        status = "STALE"
                except ValueError:
                    pass

            if r["errors"] > 0 or r["timeouts"] > 0:
                # 检查最近的错误是否在最后一批
                recent_errors = conn.execute("""
                    SELECT download_status FROM papers
                    WHERE institution = ? AND source_db = ?
                    ORDER BY scrape_time DESC LIMIT ?
                """, (r["institution"], r["source_db"], PAPERS_PER_PAGE)).fetchall()

                if any(er["download_status"] in ("error", "timeout") for er in recent_errors):
                    status = "HAS_ERRORS"

            results.append({
                "institution": r["institution"],
                "source_db": r["source_db"],
                "total": total,
                "downloaded": r["downloaded"],
                "skipped": r["skipped"],
                "errors": r["errors"],
                "timeouts": r["timeouts"],
                "est_page": est_page,
                "last_scrape": last_scrape or "N/A",
                "status": status,
            })

        return results
    except Exception as e:
        print(f"[ERR] 查询机构进度失败: {e}")
        return []


def get_active_crawl_info(conn) -> dict:
    """分析哪个爬虫实例正在爬哪个机构（根据最近 scrape_time 和 source_db 推断）"""
    if not conn:
        return {}
    try:
        # 最近30分钟有活动的机构
        cutoff = (datetime.now() - timedelta(minutes=30)).isoformat()
        rows = conn.execute("""
            SELECT institution, source_db, MAX(scrape_time) as last_active,
                   COUNT(*) as recent_count
            FROM papers
            WHERE scrape_time > ?
            GROUP BY institution, source_db
            ORDER BY last_active DESC
        """, (cutoff,)).fetchall()

        active = {}
        for r in rows:
            key = f"{r['source_db']}_{r['institution']}"
            active[key] = {
                "institution": r["institution"],
                "source_db": r["source_db"],
                "last_active": r["last_active"],
                "recent_count": r["recent_count"],
            }
        return active
    except Exception:
        return {}


def estimate_resume_page(conn, institution: str, source_db: str) -> int:
    """估算某个机构在某数据源应该从第几页恢复"""
    if not conn:
        return 1
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM papers WHERE institution = ? AND source_db = ?",
            (institution, source_db)
        ).fetchone()
        total = row["cnt"] if row else 0
        return max(1, (total + PAPERS_PER_PAGE - 1) // PAPERS_PER_PAGE)
    except Exception:
        return 1


# ════════════════════════════════════════════
# ProgressReporter — 终端表格输出
# ════════════════════════════════════════════

def print_report(cdp_status: dict, process_status: dict, inst_progress: list,
                 active_crawls: dict, overall_stats: dict, alerts: list):
    """打印完整的状态报告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*70}")
    print(f"  AI Supervisor Report — {now}")
    print(f"{'='*70}")

    # 1. 浏览器状态
    print(f"\n  [浏览器 CDP 状态]")
    used_ports = set()
    for name, cfg in CRAWLERS.items():
        port = cfg["cdp_port"]
        if port in used_ports:
            continue
        used_ports.add(port)
        info = cdp_status.get(port, {})
        if info.get("alive"):
            print(f"    端口 {port}: 存活  ({info.get('browser', '')})")
            # 显示标签页
            tabs = info.get("tabs", [])
            for t in tabs[:3]:
                url_short = t["url"][:60] + "..." if len(t["url"]) > 60 else t["url"]
                print(f"      -> {t['title'][:40]}  {url_short}")
        else:
            print(f"    端口 {port}: 离线")

    # 2. 进程状态
    print(f"\n  [爬虫进程状态]")
    for name, cfg in CRAWLERS.items():
        pinfo = process_status.get(name, {})
        script = cfg["script"]
        if pinfo.get("running"):
            pid = pinfo["pid"]
            print(f"    {script:<50s}  运行中  PID={pid}")
        else:
            dead_pid = pinfo.get("dead_pid")
            if dead_pid:
                print(f"    {script:<50s}  已停止  (PID={dead_pid} 已退出)")
            elif pinfo.get("pid_file_exists"):
                print(f"    {script:<50s}  已停止  (PID文件损坏)")
            else:
                print(f"    {script:<50s}  未启动")

    # 3. 各机构进度
    if inst_progress:
        print(f"\n  [机构爬取进度]")
        print(f"    {'机构':<30s} {'来源':<8s} {'已爬':>6s} {'下载':>6s} {'估算页':>6s} {'状态':<12s} {'最后活跃'}")
        print(f"    {'-'*30} {'-'*8} {'-'*6} {'-'*6} {'-'*6} {'-'*12} {'-'*20}")
        for ip in inst_progress:
            inst_short = ip["institution"][:28]
            print(
                f"    {inst_short:<30s} {ip['source_db']:<8s} "
                f"{ip['total']:>6d} {ip['downloaded']:>6d} "
                f"{ip['est_page']:>6d} {ip['status']:<12s} "
                f"{str(ip['last_scrape'])[:19]}"
            )

    # 4. 告警
    if alerts:
        print(f"\n  [告警]")
        for alert in alerts:
            prefix = alert.get("level", "INFO")
            msg = alert["msg"]
            if prefix == "ACTION":
                print(f"    >>> {msg}")
            elif prefix == "WARNING":
                print(f"    !!! {msg}")
            else:
                print(f"    --- {msg}")

    # 5. 汇总
    print(f"\n  [汇总]")
    total_papers = sum(overall_stats.values()) if overall_stats else 0
    ok = overall_stats.get("ok", 0)
    skip = overall_stats.get("skip", 0)
    err = overall_stats.get("error", 0)
    tmo = overall_stats.get("timeout", 0)
    running = sum(1 for p in process_status.values() if p.get("running"))
    alive_cdp = sum(1 for c in cdp_status.values() if c.get("alive"))
    print(f"    总记录: {total_papers}  |  已下载: {ok}  |  跳过: {skip}  |  出错: {err}  |  超时: {tmo}")
    print(f"    活跃爬虫: {running}/{len(CRAWLERS)}  |  活跃浏览器: {alive_cdp}/{len(set(c['cdp_port'] for c in CRAWLERS.values()))}")
    print(f"{'='*70}\n")


# ════════════════════════════════════════════
# CrawlRestarter — 自动重启爬虫
# ════════════════════════════════════════════

class CrawlRestarter:
    """管理爬虫重启，带冷却时间防止崩溃循环"""

    def __init__(self):
        self._last_restart = {}  # crawler_name -> last_restart_timestamp

    def can_restart(self, crawler_name: str) -> bool:
        """检查是否过了冷却期"""
        last = self._last_restart.get(crawler_name, 0)
        return (time.time() - last) >= RESTART_COOLDOWN

    def restart(self, crawler_name: str, institution: str, start_page: int) -> subprocess.Popen:
        """重启指定爬虫"""
        cfg = CRAWLERS[crawler_name]
        script_path = PROJECT_ROOT / cfg["script"]

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{crawler_name}_{institution}.log"

        cmd = [
            sys.executable,
            str(script_path),
            "--institution", institution,
            "--page", str(start_page),
        ]

        print(f"  [RESTART] 启动 {cfg['script']} --institution '{institution}' --page {start_page}")
        print(f"  [RESTART] 日志: {log_file}")

        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=open(log_file, "a"),
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )

        self._last_restart[crawler_name] = time.time()
        return proc


# ════════════════════════════════════════════
# SupervisorLoop — 主循环
# ════════════════════════════════════════════

def run_check(restarter: CrawlRestarter, auto_restart: bool) -> dict:
    """执行一次完整检查，返回状态数据"""
    alerts = []

    # 1. 检查所有 CDP 端口
    cdp_status = {}
    checked_ports = set()
    for name, cfg in CRAWLERS.items():
        port = cfg["cdp_port"]
        if port in checked_ports:
            continue
        checked_ports.add(port)
        info = check_cdp_alive(port)
        info["tabs"] = check_cdp_tabs(port)
        cdp_status[port] = info

    # 2. 检查所有爬虫进程
    process_status = {}
    for name, cfg in CRAWLERS.items():
        process_status[name] = check_pid_file(cfg["script"])

    # 3. 数据库分析
    conn = get_db_conn()
    overall_stats = get_overall_stats(conn)
    inst_progress = get_institution_progress(conn)
    active_crawls = get_active_crawl_info(conn)

    # 4. 生成告警
    # 4a. 端口冲突检测
    port_users = {}
    for name, cfg in CRAWLERS.items():
        port_users.setdefault(cfg["cdp_port"], []).append(name)
    for port, users in port_users.items():
        if len(users) > 1:
            alerts.append({
                "level": "WARNING",
                "msg": f"端口 {port} 被 {len(users)} 个爬虫共享: {', '.join(users)}，同时只能跑一个"
            })

    # 4b. 进程死亡但 CDP 存活 → 可恢复
    for name, cfg in CRAWLERS.items():
        if not cfg["needs_cdp"]:
            continue
        pinfo = process_status.get(name, {})
        cdp_info = cdp_status.get(cfg["cdp_port"], {})

        if not pinfo.get("running") and cdp_info.get("alive"):
            # 找到这个爬虫最近在爬什么机构
            source = cfg["source_db"]
            # 查找最近由该 source_db 爬取的机构
            target_inst = None
            resume_page = 1

            if conn:
                # 找最近有活动且有未完成工作的机构
                row = conn.execute("""
                    SELECT institution, MAX(scrape_time) as last_active
                    FROM papers
                    WHERE source_db = ? AND institution IS NOT NULL AND institution != ''
                    GROUP BY institution
                    ORDER BY last_active DESC
                    LIMIT 1
                """, (source,)).fetchone()

                if row:
                    target_inst = row["institution"]
                    resume_page = estimate_resume_page(conn, target_inst, source)

            if target_inst:
                if auto_restart and restarter.can_restart(name):
                    alerts.append({
                        "level": "ACTION",
                        "msg": f"{name} 已停止但 CDP:{cfg['cdp_port']} 存活，自动重启爬取 [{target_inst}] 从第 {resume_page} 页"
                    })
                    restarter.restart(name, target_inst, resume_page)
                elif not auto_restart:
                    alerts.append({
                        "level": "WARNING",
                        "msg": f"{name} 已停止但 CDP:{cfg['cdp_port']} 存活，可恢复爬取 [{target_inst}] 第 {resume_page} 页 (启用 --auto-restart 自动恢复)"
                    })
            else:
                alerts.append({
                    "level": "INFO",
                    "msg": f"{name} 已停止，CDP:{cfg['cdp_port']} 存活，无历史爬取记录可恢复"
                })

    # 4c. 进程死亡且 CDP 也死 → 需要人工介入
    for name, cfg in CRAWLERS.items():
        if not cfg["needs_cdp"]:
            continue
        pinfo = process_status.get(name, {})
        cdp_info = cdp_status.get(cfg["cdp_port"], {})

        if not pinfo.get("running") and not cdp_info.get("alive"):
            alerts.append({
                "level": "WARNING",
                "msg": f"{name} 已停止且 CDP:{cfg['cdp_port']} 离线，需手动启动 Chrome: chrome.exe --remote-debugging-port={cfg['cdp_port']}"
            })

    # 4d. 进程存活但长时间无活动
    for name, cfg in CRAWLERS.items():
        pinfo = process_status.get(name, {})
        if pinfo.get("running"):
            source = cfg["source_db"]
            if conn:
                row = conn.execute("""
                    SELECT MAX(scrape_time) as last_scrape FROM papers
                    WHERE source_db = ?
                """, (source,)).fetchone()
                if row and row["last_scrape"]:
                    try:
                        last_dt = datetime.fromisoformat(row["last_scrape"])
                        gap = (datetime.now() - last_dt).total_seconds()
                        if gap > STALE_THRESHOLD:
                            alerts.append({
                                "level": "WARNING",
                                "msg": f"{name} 进程存活但 {int(gap/60)} 分钟无新数据，可能卡死 (source={source})"
                            })
                    except ValueError:
                        pass

    # 5. 打印报告
    print_report(cdp_status, process_status, inst_progress, active_crawls, overall_stats, alerts)

    if conn:
        conn.close()

    return {
        "cdp_status": cdp_status,
        "process_status": process_status,
        "alerts": alerts,
    }


def main():
    parser = argparse.ArgumentParser(description="AI Supervisor — 爬虫统一监控与断点恢复")
    parser.add_argument("--interval", "-t", type=int, default=300,
                        help="检查间隔(秒)，默认300(5分钟)")
    parser.add_argument("--auto-restart", action="store_true",
                        help="启用自动重启（检测到爬虫死亡时自动恢复）")
    parser.add_argument("--once", action="store_true",
                        help="只检查一次，不进入循环")
    args = parser.parse_args()

    restarter = CrawlRestarter()

    print(f"\n{'='*70}")
    print(f"  AI Supervisor 启动")
    print(f"  检查间隔: {args.interval}s  |  自动重启: {'ON' if args.auto_restart else 'OFF'}")
    print(f"  数据库: {DB_PATH.resolve()}")
    print(f"  管理爬虫数: {len(CRAWLERS)}")
    print(f"{'='*70}")

    if not DB_PATH.exists():
        print(f"\n[WARN] 数据库不存在: {DB_PATH}，部分功能不可用")

    try:
        while True:
            run_check(restarter, args.auto_restart)

            if args.once:
                break

            next_time = datetime.now() + timedelta(seconds=args.interval)
            print(f"  下次检查: {next_time.strftime('%H:%M:%S')}  (Ctrl+C 退出)\n")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\n{'='*70}")
        print("  AI Supervisor 已停止")
        print(f"{'='*70}")


if __name__ == "__main__":
    main()
